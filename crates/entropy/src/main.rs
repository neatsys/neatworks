use std::{
    collections::{HashMap, HashSet},
    env::args,
    future::IntoFuture,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};

use augustus::{
    blob,
    crypto::{Crypto, DigestHash, H256},
    event::{
        erased::{Sender, Session},
        SendEvent,
    },
    kademlia::{self, Buckets, PeerId, PeerRecord},
    net::{
        kademlia::{Control, PeerNet},
        Udp,
    },
    worker::erased::spawn_backend,
};
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, State},
    routing::{get, post},
    Json, Router,
};

use entropy::{Get, GetOk, MessageNet, Peer, Put, PutOk};
use entropy_control_messages::{
    GetConfig, GetResult, PeerUrl, PutConfig, PutResult, StartPeersConfig,
};
use rand::{rngs::StdRng, seq::SliceRandom, thread_rng, RngCore, SeedableRng};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{create_dir, remove_dir_all},
    net::UdpSocket,
    runtime::{self, Runtime},
    signal::ctrl_c,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot, Mutex,
    },
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use wirehair::{Decoder, Encoder};

#[derive(Debug, Clone, derive_more::From)]
enum Upcall {
    PutOk(PutOk<[u8; 32]>),
    GetOk(GetOk<[u8; 32]>),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/benchmark-put", post(benchmark_put))
        .route("/benchmark-put/:put_id", get(poll_benchmark_put))
        .route("/benchmark-get", post(benchmark_get))
        .route("/benchmark-get/:get_id", get(poll_benchmark_get))
        .route("/start-peers", post(start_peers))
        .route("/stop-peers", post(stop_peers))
        .route(
            "/put-chunk/:peer_index",
            post(put_chunk).layer(DefaultBodyLimit::max(1 << 30)),
        )
        .route("/get-chunk/:peer_index/:chunk", post(get_chunk));
    let (upcall_sender, mut upcall_receiver) = unbounded_channel();
    let pending_puts = Arc::new(Mutex::new(HashMap::new()));
    let pending_gets = Arc::new(Mutex::new(HashMap::new()));
    let runtime = Arc::new(runtime::Builder::new_multi_thread().enable_all().build()?);
    let app = app.with_state(AppState {
        peers: Default::default(),
        runtime: runtime.clone(),
        upcall_sender,
        pending_puts: pending_puts.clone(),
        pending_gets: pending_gets.clone(),
        op_client: reqwest::Client::new(),
        benchmark_op_id: Default::default(),
        benchmark_puts: Default::default(),
        benchmark_gets: Default::default(),
    });

    let upcall_session = tokio::spawn(async move {
        while let Some(upcall) = upcall_receiver.recv().await {
            // {
            //     use std::io::Write;
            //     let mut stdout = std::io::stdout().lock();
            //     writeln!(&mut stdout, "{upcall:?}").unwrap();
            // }
            match upcall {
                Upcall::PutOk(PutOk(chunk)) => pending_puts
                    .lock()
                    .await
                    .remove(&chunk)
                    // if the sender is not present a premature stop-peers is probably executed
                    // which is unexpected
                    .unwrap()
                    .send(())
                    .unwrap(),
                Upcall::GetOk(GetOk(chunk, buf)) => pending_gets
                    .lock()
                    .await
                    .remove(&chunk)
                    .unwrap()
                    .send(buf)
                    .unwrap(),
            }
        }
    });

    let addr = args().nth(1);
    let addr = addr.as_deref().unwrap_or("0.0.0.0:3000");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let serve = axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .into_future();
    tokio::select! {
        result = serve => result?,
        result = upcall_session => result?,
    }
    Arc::try_unwrap(runtime)
        .map_err(|_| anyhow::anyhow!("cannot shutdown runtime"))?
        .shutdown_background();
    Ok(())
}

#[derive(Debug, Clone)]
struct AppState {
    peers: Arc<Mutex<PeersState>>,
    runtime: Arc<Runtime>,
    upcall_sender: UnboundedSender<Upcall>,
    pending_puts: Arc<Mutex<HashMap<[u8; 32], oneshot::Sender<()>>>>,
    #[allow(clippy::type_complexity)]
    pending_gets: Arc<Mutex<HashMap<[u8; 32], oneshot::Sender<Vec<u8>>>>>,
    op_client: reqwest::Client,
    benchmark_op_id: Arc<AtomicU32>,
    benchmark_puts: Arc<Mutex<HashMap<u32, JoinHandle<anyhow::Result<PutResult>>>>>,
    benchmark_gets: Arc<Mutex<HashMap<u32, JoinHandle<anyhow::Result<GetResult>>>>>,
}

#[derive(Debug, Default)]
struct PeersState {
    sessions: JoinSet<anyhow::Result<()>>,
    senders: Vec<Sender<Peer<[u8; 32]>>>,
    // cancel
}

async fn ok(State(state): State<AppState>) {
    let mut peers = state.peers.lock().await;
    while let Ok(Some(result)) = timeout(Duration::ZERO, peers.sessions.join_next()).await {
        result.unwrap().unwrap()
    }
}

async fn start_peers(State(state): State<AppState>, Json(config): Json<StartPeersConfig>) {
    let mut peers = state.peers.lock().await;
    assert!(peers.sessions.is_empty());
    let mut rng = StdRng::seed_from_u64(117418);
    let mut records = Vec::new();
    let mut local_peers = Vec::new();
    for (i, ip) in config.ips.iter().copied().enumerate() {
        for j in 0..config.num_peer_per_ip {
            let crypto = Crypto::new_random(&mut rng);
            let record =
                PeerRecord::new(crypto.public_key(), SocketAddr::from((ip, 4000 + j as u16)));
            records.push(record.clone());
            if i == config.ip_index {
                local_peers.push((record, crypto, StdRng::from_rng(rng.clone()).unwrap()))
            }
        }
    }
    for (record, crypto, rng) in local_peers {
        let peer_session = Session::new();
        peers.senders.push(peer_session.sender());
        peers.sessions.spawn_on(
            start_peer(
                record,
                crypto,
                rng,
                records.clone(),
                peer_session,
                state.upcall_sender.clone(),
                config.clone(),
            ),
            state.runtime.handle(),
        );
    }
}

async fn start_peer(
    record: PeerRecord<SocketAddr>,
    crypto: Crypto<PeerId>,
    mut rng: StdRng,
    mut records: Vec<PeerRecord<SocketAddr>>,
    mut peer_session: Session<Peer<[u8; 32]>>,
    upcall_sender: UnboundedSender<Upcall>,
    config: StartPeersConfig,
) -> anyhow::Result<()> {
    let peer_id = record.id;
    let path = format!("/tmp/entropy-{:x}", H256(peer_id));
    let path = std::path::Path::new(&path);
    let _ = remove_dir_all(path).await;
    create_dir(path).await?;
    let socket_net = Udp(UdpSocket::bind(record.addr).await?.into());

    let ip = record.addr.ip();
    let mut buckets = Buckets::new(record);
    // we don't really need this reproducible actually... just realized too late
    records.shuffle(&mut rng);
    for record in records {
        if record.id == peer_id {
            continue;
        }
        buckets.insert(record)?
    }

    let mut kademlia_session = Session::new();
    let mut control_session = Session::new();
    let (mut blob_sender, blob_receiver) = unbounded_channel();
    let (fs_sender, fs_receiver) = unbounded_channel();
    let (crypto_worker, mut crypto_session) = spawn_backend(crypto.clone());
    let (codec_worker, mut codec_session) = spawn_backend(());

    let mut kademlia_peer = kademlia::Peer::new(
        buckets,
        MessageNet::new(socket_net.clone()),
        control_session.sender(),
        crypto_worker,
    );
    let mut control = Control::new(socket_net.clone(), kademlia_session.sender());
    let mut peer = Peer::new(
        peer_id,
        crypto,
        config.fragment_len,
        config.chunk_k,
        config.chunk_n,
        config.chunk_m,
        MessageNet::<_, SocketAddr>::new(PeerNet(control_session.sender())),
        blob_sender.clone(),
        upcall_sender,
        codec_worker,
        fs_sender,
    );

    let socket_session = socket_net.recv_session({
        let mut peer_sender = peer_session.sender();
        let mut kademlia_sender = kademlia_session.sender();
        move |buf| {
            entropy::on_buf(
                buf,
                &mut peer_sender,
                &mut kademlia_sender,
                &mut blob_sender,
            )
        }
    });
    let crypto_session = crypto_session.run(kademlia::SendCrypto(kademlia_session.sender()));
    let kademlia_session = kademlia_session.run(&mut kademlia_peer);
    let blob_session = blob::stream::session(
        ip,
        blob_receiver,
        MessageNet::<_, SocketAddr>::new(PeerNet(control_session.sender())),
        peer_session.sender(),
    );
    let control_session = control_session.run(&mut control);
    let fs_session = entropy::fs::session(path, fs_receiver, peer_session.sender());
    let codec_session = codec_session.run(entropy::SendCodec(peer_session.sender()));
    let peer_session = peer_session.run(&mut peer);

    tokio::select! {
        result = socket_session => result?,
        result = kademlia_session => result?,
        result = control_session => result?,
        result = blob_session => result?,
        result = fs_session => result?,
        result = crypto_session => result?,
        result = codec_session => result?,
        result = peer_session => result?,
    }
    Err(anyhow::anyhow!("unexpected shutdown"))
}

async fn stop_peers(State(state): State<AppState>) {
    let mut peers = state.peers.lock().await;
    peers.sessions.shutdown().await;
    peers.senders.clear();
}

async fn put_chunk(
    State(state): State<AppState>,
    Path(peer_index): Path<usize>,
    mut multipart: Multipart,
) -> String {
    let buf = multipart
        .next_field()
        .await
        .unwrap()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let chunk = buf.sha256();
    let (sender, receiver) = oneshot::channel();
    let replaced = state.pending_puts.lock().await.insert(chunk, sender);
    assert!(replaced.is_none());
    state.peers.lock().await.senders[peer_index]
        .send(Put(chunk, buf))
        .unwrap();
    // detach receiving, so that even if http connection closed receiver keeps alive
    tokio::spawn(receiver).await.unwrap().unwrap();
    format!("{:x}", H256(chunk))
}

async fn get_chunk(
    State(state): State<AppState>,
    Path((peer_index, chunk)): Path<(usize, String)>,
) -> Vec<u8> {
    let chunk = chunk.parse::<H256>().unwrap().into();
    let (sender, receiver) = oneshot::channel();
    let replaced = state.pending_gets.lock().await.insert(chunk, sender);
    assert!(replaced.is_none());
    state.peers.lock().await.senders[peer_index]
        .send(Get(chunk))
        .unwrap();
    tokio::spawn(receiver).await.unwrap().unwrap()
}

async fn benchmark_put(State(state): State<AppState>, Json(config): Json<PutConfig>) -> Json<u32> {
    let session = state.runtime.spawn(async move {
        let mut buf = vec![0; config.chunk_len as usize * config.k.get()];
        thread_rng().fill_bytes(&mut buf);
        let digest = buf.sha256();
        let start = Instant::now();
        let encoder = Arc::new(Encoder::new(&buf, config.chunk_len)?);
        let mut encode_sessions = JoinSet::<anyhow::Result<_>>::new();
        let mut chunk_indexes = HashSet::<u32>::new();
        for peer_url_group in config.peer_urls {
            let mut index;
            while {
                index = rand::random();
                !chunk_indexes.insert(index)
            } {}
            let encoder = encoder.clone();
            let op_client = state.op_client.clone();
            encode_sessions.spawn(async move {
                let buf = encoder.encode(index)?;
                // {
                //     use std::io::Write;
                //     let mut stdout = std::io::stdout().lock();
                //     for chunk in buf.chunks(32) {
                //         for byte in chunk {
                //             write!(&mut stdout, "{byte:02x} ")?
                //         }
                //         writeln!(&mut stdout)?
                //     }
                // }
                let mut put_sessions = JoinSet::<anyhow::Result<_>>::new();
                for (i, peer_url) in peer_url_group.into_iter().enumerate() {
                    let op_client = op_client.clone();
                    let buf = buf.clone();
                    put_sessions.spawn(async move {
                        let form = Form::new().part("", Part::bytes(buf));
                        match peer_url {
                            PeerUrl::Ipfs(peer_url) => {
                                #[allow(non_snake_case)]
                                #[derive(Deserialize)]
                                struct Response {
                                    Hash: String,
                                }
                                let response = op_client
                                    .post(format!("{peer_url}/api/v0/add"))
                                    .multipart(form)
                                    .send()
                                    .await?
                                    .error_for_status()?
                                    .json::<Response>()
                                    .await?;
                                Ok(response.Hash)
                            }
                            PeerUrl::Entropy(url, peer_index) => {
                                assert_eq!(i, 0);
                                let response = op_client
                                    .post(format!("{url}/put-chunk/{peer_index}"))
                                    .multipart(form)
                                    .send()
                                    .await?
                                    .error_for_status()?;
                                Ok(String::from_utf8(response.bytes().await?.to_vec())?)
                            }
                        }
                    });
                }
                let chunk = put_sessions
                    .join_next()
                    .await
                    .ok_or(anyhow::anyhow!("no peer url for the chunk"))???;
                while let Some(also_chunk) = put_sessions.join_next().await {
                    if also_chunk?? != chunk {
                        anyhow::bail!("inconsistent chunk among peers")
                    }
                }
                Ok((index, chunk))
            });
        }
        let mut chunks = Vec::new();
        while let Some(result) = encode_sessions.join_next().await {
            chunks.push(result??)
        }
        Ok(PutResult {
            digest,
            chunks,
            latency: start.elapsed(),
        })
    });
    let put_id = state.benchmark_op_id.fetch_add(1, SeqCst);
    state.benchmark_puts.lock().await.insert(put_id, session);
    Json(put_id)
}

async fn poll_benchmark_put(
    State(state): State<AppState>,
    Path(put_id): Path<u32>,
) -> Json<Option<PutResult>> {
    let mut puts = state.benchmark_puts.lock().await;
    if !puts[&put_id].is_finished() {
        Json(None)
    } else {
        Json(Some(puts.remove(&put_id).unwrap().await.unwrap().unwrap()))
    }
}

async fn benchmark_get(State(state): State<AppState>, Json(config): Json<GetConfig>) -> Json<u32> {
    let session = state.runtime.spawn(async move {
        let start = Instant::now();
        let mut get_sessions = JoinSet::<anyhow::Result<_>>::new();
        for ((index, chunk), peer_url) in config.chunks.into_iter().zip(config.peer_urls) {
            let op_client = state.op_client.clone();
            get_sessions.spawn(async move {
                let buf = match peer_url {
                    PeerUrl::Ipfs(peer_url) => {
                        #[derive(Serialize)]
                        struct Query {
                            arg: String,
                        }
                        op_client
                            .post(format!("{peer_url}/api/v0/cat"))
                            .query(&Query { arg: chunk })
                            .send()
                            .await?
                            .error_for_status()?
                            .bytes()
                            .await?
                    }
                    PeerUrl::Entropy(url, peer_index) => {
                        op_client
                            .post(format!("{url}/get-chunk/{peer_index}/{chunk}"))
                            .send()
                            .await?
                            .error_for_status()?
                            .bytes()
                            .await?
                    }
                };
                Ok((index, buf))
            });
        }
        let mut decoder = Decoder::new(
            config.chunk_len as u64 * config.k.get() as u64,
            config.chunk_len,
        )?;
        while let Some(result) = get_sessions.join_next().await {
            let (index, buf) = result??;
            // {
            //     use std::io::Write;
            //     let mut stdout = std::io::stdout().lock();
            //     for chunk in buf.chunks(32) {
            //         for byte in chunk {
            //             write!(&mut stdout, "{byte:02x} ")?
            //         }
            //         writeln!(&mut stdout)?
            //     }
            // }
            if decoder.decode(index, &buf)? {
                get_sessions.abort_all();
                let buf = decoder.recover()?;
                let latency = start.elapsed();
                get_sessions.shutdown().await;
                return Ok(GetResult {
                    digest: buf.sha256(),
                    latency,
                });
            }
        }
        Err(anyhow::anyhow!("recover fail"))
    });
    let get_id = state.benchmark_op_id.fetch_add(1, SeqCst);
    state.benchmark_gets.lock().await.insert(get_id, session);
    Json(get_id)
}

async fn poll_benchmark_get(
    State(state): State<AppState>,
    Path(get_id): Path<u32>,
) -> Json<Option<GetResult>> {
    let mut gets = state.benchmark_gets.lock().await;
    if !gets[&get_id].is_finished() {
        Json(None)
    } else {
        Json(Some(gets.remove(&get_id).unwrap().await.unwrap().unwrap()))
    }
}
