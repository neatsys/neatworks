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
    crypto::{Crypto, H256},
    event::erased::Session,
    kademlia::{self, Buckets, PeerId, PeerRecord},
    net::{
        kademlia::{Control, PeerNet},
        Udp,
    },
    worker::erased::spawn_backend,
};
use axum::{
    extract::{Multipart, Path, State},
    routing::{get, post},
    Json, Router,
};

use entropy::{GetOk, MessageNet, Peer, PutOk};
use entropy_control_messages::{PutConfig, PutResult, StartPeersConfig};
use rand::{rngs::StdRng, seq::SliceRandom, thread_rng, RngCore, SeedableRng};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use tokio::{
    fs::create_dir,
    net::UdpSocket,
    runtime::{self, Runtime},
    signal::ctrl_c,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        Mutex,
    },
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use wirehair::Encoder;

#[derive(Debug, Clone, derive_more::From)]
enum Upcall {
    PutOk(PutOk),
    GetOk(GetOk),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/benchmark-put", post(benchmark_put))
        .route("/benchmark-put/{put_id}", get(poll_benchmark_put))
        .route("/start-peers", post(start_peers))
        .route("/put-chunk", post(put_chunk));
    let (upcall_sender, mut upcall_receiver) = unbounded_channel();
    let upcall_session = tokio::spawn(async move {
        while let Some(_upcall) = upcall_receiver.recv().await {
            //
        }
    });
    let app = app.with_state(AppState {
        sessions: Default::default(),
        runtime: runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .into(),
        upcall_sender,
        op_client: reqwest::Client::new(),
        benchmark_puts: Default::default(),
        benchmark_put_id: Default::default(),
    });
    let url = args().nth(1).ok_or(anyhow::anyhow!("not specify url"))?;
    let listener = tokio::net::TcpListener::bind(&url).await?;
    let serve = axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .into_future();
    tokio::select! {
        result = serve => result?,
        result = upcall_session => result?,
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct AppState {
    sessions: Arc<Mutex<JoinSet<anyhow::Result<()>>>>,
    runtime: Arc<Runtime>,
    upcall_sender: UnboundedSender<Upcall>,
    op_client: reqwest::Client,
    benchmark_puts: Arc<Mutex<HashMap<u32, JoinHandle<anyhow::Result<PutResult>>>>>,
    benchmark_put_id: Arc<AtomicU32>,
}

async fn ok(State(state): State<AppState>) {
    let mut sessions = state.sessions.lock().await;
    match timeout(Duration::ZERO, sessions.join_next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(result)) => result.unwrap().unwrap(),
    }
}

async fn start_peers(State(state): State<AppState>, Json(config): Json<StartPeersConfig>) {
    let mut sessions = state.sessions.lock().await;
    assert!(sessions.is_empty());
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
        sessions.spawn_on(
            start_peer(
                record,
                crypto,
                rng,
                records.clone(),
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
    upcall_sender: UnboundedSender<Upcall>,
    config: StartPeersConfig,
) -> anyhow::Result<()> {
    let peer_id = record.id;
    let path = format!("/tmp/entropy-{:x}", H256(peer_id));
    let path = std::path::Path::new(&path);
    create_dir(path).await?;
    let socket_net = Udp(UdpSocket::bind(record.addr).await?.into());

    let ip = record.addr.ip();
    let mut buckets = Buckets::new(record);
    records.shuffle(&mut rng);
    for record in records {
        buckets.insert(record)
    }

    let mut kademlia_session = Session::new();
    let mut control_session = Session::new();
    let (mut blob_sender, blob_receiver) = unbounded_channel();
    let (fs_sender, fs_receiver) = unbounded_channel();
    let (crypto_worker, mut crypto_session) = spawn_backend(crypto.clone());
    let (codec_worker, mut codec_session) = spawn_backend(());
    let mut peer_session = Session::new();

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
    let blob_session = blob::session(
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

#[allow(unused)]
async fn put_chunk(State(state): State<AppState>, mut multipart: Multipart) {
    //
}

async fn benchmark_put(State(state): State<AppState>, Json(config): Json<PutConfig>) {
    let put_session = state.runtime.spawn(async move {
        let mut buf = vec![0; config.chunk_len as usize * config.k.get()];
        thread_rng().fill_bytes(&mut buf);
        let start = Instant::now();
        let encoder = Arc::new(Encoder::new(&buf, config.chunk_len)?);
        let mut encode_sessions = JoinSet::<anyhow::Result<_>>::new();
        let mut chunks = HashSet::<u32>::new();
        for peer_url_group in config.peer_urls {
            let mut buf;
            while {
                buf = rand::random();
                !chunks.insert(buf)
            } {}
            let encoder = encoder.clone();
            let op_client = state.op_client.clone();
            encode_sessions.spawn(async move {
                let buf = encoder.encode(buf)?;
                let mut put_sessions = JoinSet::<anyhow::Result<_>>::new();
                for peer_url in peer_url_group {
                    let op_client = op_client.clone();
                    let buf = buf.clone();
                    put_sessions.spawn(async move {
                        #[allow(non_snake_case)]
                        #[derive(Deserialize)]
                        struct Response {
                            Hash: String,
                        }
                        let response = op_client
                            .post(format!("{peer_url}/api/v0/add"))
                            .multipart(Form::new().part("", Part::bytes(buf)))
                            .send()
                            .await?
                            .error_for_status()?
                            .json::<Response>()
                            .await?;
                        Ok(response.Hash)
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
                Ok(chunk)
            });
        }
        let mut chunks = Vec::new();
        while let Some(chunk) = encode_sessions.join_next().await {
            chunks.push(chunk??)
        }
        Ok(PutResult {
            chunks,
            latency: start.elapsed(),
        })
    });
    let put_id = state.benchmark_put_id.fetch_add(1, SeqCst);
    state
        .benchmark_puts
        .lock()
        .await
        .insert(put_id, put_session);
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
