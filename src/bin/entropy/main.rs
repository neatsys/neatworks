mod client;
mod peer;

use std::{
    backtrace::BacktraceStatus,
    collections::HashMap,
    env::args,
    future::IntoFuture,
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};

use augustus::{
    crypto::{peer::Crypto, DigestHash as _, H256},
    entropy::{Get, GetOk, Put, PutOk},
    kademlia::PeerRecord,
    net::Payload,
};
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use entropy_control_messages::{GetConfig, GetResult, PutConfig, PutResult, StartPeersConfig};
use rand::{rngs::StdRng, SeedableRng};
use tokio::{
    net::TcpListener,
    signal::ctrl_c,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot, Mutex,
    },
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tracing::{warn, Level};
use tracing_subscriber::{
    filter::Targets, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

#[derive(Debug, Clone, derive_more::From)]
enum Upcall {
    PutOk(PutOk<H256>),
    GetOk(GetOk<H256>),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_max_level(Level::TRACE)
        .finish()
        .with(if let Ok(var) = std::env::var("RUST_LOG") {
            Targets::from_str(&var)?
        } else {
            Targets::new().with_default(Level::INFO)
        })
        .init();

    let rlimit = nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE)?;
    nix::sys::resource::setrlimit(
        nix::sys::resource::Resource::RLIMIT_NOFILE,
        rlimit.1,
        rlimit.1,
    )?;

    let app = Router::new()
        // interestingly this artifact/server has dual purposes
        // it is a common (in this codebase) benchmark server that somehow accepts command line
        // arguments and report results through HTTP, with following endpoints
        .route("/ok", get(ok))
        .route("/benchmark-put", post(benchmark_put))
        .route("/benchmark-put/:put_id", get(poll_benchmark_put))
        .route("/benchmark-get", post(benchmark_get))
        .route("/benchmark-get/:get_id", get(poll_benchmark_get))
        .route("/start-peers", post(start_peers))
        .route("/stop-peers", post(stop_peers))
        // and the same time, it also includes the following endpoints that belongs to the internal
        // of entropy, which happens to also communicate using HTTP (for aligning with IPFS)
        .route(
            "/put-chunk/:peer_index",
            post(put_chunk).layer(DefaultBodyLimit::max(1 << 30)),
        )
        .route("/get-chunk/:peer_index/:chunk", post(get_chunk));

    let (upcall_sender, mut upcall_receiver) = unbounded_channel();
    let pending_puts = Arc::new(Mutex::new(HashMap::new()));
    let pending_gets = Arc::new(Mutex::new(HashMap::new()));
    // let runtime = Arc::new(runtime::Builder::new_multi_thread().enable_all().build()?);
    let app = app.with_state(AppState {
        peers: Default::default(),
        // runtime: runtime.clone(),
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
    let listener = TcpListener::bind(&addr).await?;
    let serve = axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .into_future();
    tokio::select! {
        result = serve => result?,
        result = upcall_session => result?,
    }
    Ok(())
}

#[derive(Clone)]
struct AppState {
    peers: Arc<Mutex<PeersState>>,
    upcall_sender: UnboundedSender<Upcall>,
    pending_puts: Arc<Mutex<HashMap<H256, oneshot::Sender<()>>>>,
    #[allow(clippy::type_complexity)]
    pending_gets: Arc<Mutex<HashMap<H256, oneshot::Sender<Payload>>>>,
    op_client: reqwest::Client,
    benchmark_op_id: Arc<AtomicU32>,
    benchmark_puts: Arc<Mutex<HashMap<u32, JoinHandle<anyhow::Result<PutResult>>>>>,
    benchmark_gets: Arc<Mutex<HashMap<u32, JoinHandle<anyhow::Result<GetResult>>>>>,
}

#[derive(Default)]
struct PeersState {
    sessions: JoinSet<anyhow::Result<()>>,
    #[allow(clippy::type_complexity)]
    senders: Vec<UnboundedSender<peer::Invoke>>,
}

async fn ok(State(state): State<AppState>) -> StatusCode {
    let mut peers = state.peers.lock().await;
    if let Ok(Some(result)) = timeout(Duration::ZERO, peers.sessions.join_next()).await {
        match result {
            Err(err) => warn!("{err}"),
            Ok(Err(err)) => {
                warn!("{err}");
                if err.backtrace().status() == BacktraceStatus::Captured {
                    warn!("\n{}", err.backtrace())
                } else {
                    warn!("{}", err.backtrace())
                }
            }
            Ok(Ok(())) => warn!("unexpected peer exit"),
        }
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::OK
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
        let (invoke_sender, invoke_receiver) = unbounded_channel();
        peers.senders.push(invoke_sender);
        // peers.sessions.spawn_on(
        peers.sessions.spawn(
            peer::session(
                record,
                crypto,
                rng,
                records.clone(),
                invoke_receiver,
                state.upcall_sender.clone(),
                config.clone(),
            ),
            // state.runtime.handle(),
        );
    }
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
) -> Response {
    let task = async {
        let buf = multipart
            .next_field()
            .await?
            .ok_or(anyhow::format_err!("missing filed"))?
            .bytes()
            .await?;
        let chunk = buf.sha256();
        let (sender, receiver) = oneshot::channel();
        let replaced = state.pending_puts.lock().await.insert(chunk, sender);
        assert!(replaced.is_none());
        state.peers.lock().await.senders[peer_index].send(Put(chunk, buf).into())?;
        // detach receiving because the `sender` expects `receiver` to always outlive itself even if
        // this handling future is aborted i.e. dropped
        tokio::spawn(receiver).await??;
        anyhow::Ok(format!("{chunk:x}"))
    };
    match task.await {
        Ok(result) => result.into_response(),
        Err(err) => {
            warn!("{err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn get_chunk(
    State(state): State<AppState>,
    Path((peer_index, chunk)): Path<(usize, String)>,
) -> Response {
    let task = async {
        let chunk = chunk.parse()?;
        let (sender, receiver) = oneshot::channel();
        let replaced = state.pending_gets.lock().await.insert(chunk, sender);
        anyhow::ensure!(replaced.is_none());
        state.peers.lock().await.senders[peer_index].send(Get(chunk).into())?;
        anyhow::Ok(tokio::spawn(receiver).await??)
    };
    match task.await {
        Ok(Payload(result)) => result.into_response(),
        Err(err) => {
            warn!("{err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn benchmark_put(State(state): State<AppState>, Json(config): Json<PutConfig>) -> Json<u32> {
    let session = tokio::spawn(client::put_session(config, state.op_client.clone()));
    let put_id = state.benchmark_op_id.fetch_add(1, SeqCst);
    state.benchmark_puts.lock().await.insert(put_id, session);
    Json(put_id)
}

async fn poll_benchmark_put(State(state): State<AppState>, Path(put_id): Path<u32>) -> Response {
    let mut puts = state.benchmark_puts.lock().await;
    let result = if !puts[&put_id].is_finished() {
        None
    } else {
        match async { puts.remove(&put_id).unwrap().await? }.await {
            Ok(result) => Some(result),
            Err(err) => {
                warn!("{err}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    };
    Json(result).into_response()
}

async fn benchmark_get(State(state): State<AppState>, Json(config): Json<GetConfig>) -> Json<u32> {
    let session = tokio::spawn(client::get_session(config, state.op_client.clone()));
    let get_id = state.benchmark_op_id.fetch_add(1, SeqCst);
    state.benchmark_gets.lock().await.insert(get_id, session);
    Json(get_id)
}

async fn poll_benchmark_get(State(state): State<AppState>, Path(get_id): Path<u32>) -> Response {
    let mut gets = state.benchmark_gets.lock().await;
    let result = if !gets[&get_id].is_finished() {
        None
    } else {
        match async { gets.remove(&get_id).unwrap().await? }.await {
            Ok(result) => Some(result),
            Err(err) => {
                warn!("{err}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    };
    Json(result).into_response()
}

// cSpell:words kademlia reqwest oneshot upcall ipfs quic rustix seedable
// cSpell:ignore rlimit setrlimit getrlimit nofile
