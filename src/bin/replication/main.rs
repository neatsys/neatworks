mod client;

use std::{
    future::{pending, Future},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use augustus::{
    app::{ycsb, App, Sqlite},
    crypto::{Crypto, CryptoFlavor},
    event::{
        erased::{
            session::{Buffered, Sender},
            Blanket, Event, Session, Unify,
        },
        session::SessionTimer,
        OnEventUniversal, OnTimerUniversal,
    },
    net::{session::Udp, IndexNet},
    pbft, unreplicated,
    worker::{spawning_backend, Submit},
};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use rand::{rngs::StdRng, SeedableRng};
use replication_control_messages::{
    BenchmarkResult, ClientConfig, Protocol, ReplicaConfig, YcsbBackend,
};
use tokio::{
    runtime,
    signal::ctrl_c,
    spawn,
    task::{spawn_blocking, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/start-client", post(start_client))
        .route("/take-benchmark-result", post(take_benchmark_result))
        .route("/start-replica", post(start_replica))
        .route("/stop-replica", post(stop_replica))
        .with_state(AppState {
            session: Default::default(),
            benchmark_result: Default::default(),
        });
    let ip = std::env::args().nth(1);
    let ip = ip.as_deref().unwrap_or("0.0.0.0");
    let listener = tokio::net::TcpListener::bind(format!("{ip}:3000")).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct AppState {
    session: Arc<Mutex<Option<AppSession>>>,
    benchmark_result: Arc<Mutex<Option<BenchmarkResult>>>,
}

type AppSession = (JoinHandle<anyhow::Result<()>>, CancellationToken);

async fn ok(State(state): State<AppState>) {
    let mut handle = None;
    {
        let mut session = state.session.lock().unwrap();
        if session
            .as_ref()
            .map(|(handle, _)| handle.is_finished())
            .unwrap_or(false)
        {
            handle = Some(session.take().unwrap().0)
        }
    }
    if let Some(handle) = handle {
        handle.await.unwrap().unwrap()
    }
}

async fn start_client(
    State(state): State<AppState>,
    Json(config): Json<ClientConfig>,
) -> StatusCode {
    let mut session = state.session.lock().unwrap();
    let replaced = session.replace((
        tokio::spawn(client::session(config, state.benchmark_result.clone())),
        CancellationToken::new(),
    ));
    if replaced.is_none() {
        StatusCode::OK
    } else {
        warn!("duplicated session");
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

async fn take_benchmark_result(State(state): State<AppState>) -> Json<Option<BenchmarkResult>> {
    let result = state.benchmark_result.lock().unwrap().take();
    if result.is_some() {
        let session = { state.session.lock().unwrap().take().unwrap() };
        session.0.await.unwrap().unwrap()
    }
    Json(result)
}

async fn start_replica(State(state): State<AppState>, Json(config): Json<ReplicaConfig>) {
    let mut session = state.session.lock().unwrap();

    // 1. earlier crash in case of any failure during app initialization
    // 2. block control plane client until initialization is done, prevent it to start clients too
    //    soon
    use augustus::app::BTreeMap;
    use replication_control_messages::App::*;
    let app = match config.app {
        Null => Box::new(augustus::app::Null) as Box<dyn App + Send + Sync>,
        Ycsb(ycsb_config) => {
            let settings = ycsb::WorkloadSettings::new(ycsb_config.record_count);
            let mut app = match ycsb_config.backend {
                YcsbBackend::BTree => Box::new(BTreeMap::new()) as Box<dyn App + Send + Sync>,
                YcsbBackend::Sqlite => Box::new(Sqlite::new(settings.field_count).unwrap()),
            };
            let mut workload =
                ycsb::Workload::new(StdRng::seed_from_u64(117418), settings).unwrap();
            println!("YCSB startup");
            for op in workload.startup_ops() {
                app.execute(&serde_json::to_vec(&op).unwrap()).unwrap();
            }
            app
        }
    };

    let cancel = CancellationToken::new();
    let session_cancel = cancel.clone();
    let handle = spawn_blocking(move || {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let socket = runtime.block_on(tokio::net::UdpSocket::bind(
            config.replica_addrs[config.replica_id as usize],
        ))?;
        println!(
            "Replica {} bind to {:?}",
            config.replica_id,
            socket.local_addr()
        );
        let net = Udp(socket.into());

        let crypto = Crypto::new_hardcoded(
            config.num_replica,
            config.replica_id,
            CryptoFlavor::Schnorrkel,
            // CryptoFlavor::Secp256k1,
        )?;
        match config.protocol {
            Protocol::Unreplicated => {
                assert_eq!(config.replica_id, 0);
                let state = Blanket(Unify(unreplicated::Replica::new(
                    app,
                    unreplicated::ToClientMessageNet::new(net.clone()),
                )));
                runtime.block_on(replica_session(
                    state,
                    unreplicated::erased::to_replica_on_buf::<SocketAddr>,
                    net,
                    |_| pending(),
                    session_cancel,
                ))
            }
            Protocol::Pbft => {
                let (crypto_worker, mut crypto_executor) = spawning_backend();
                let state = Blanket(Buffered::from(pbft::Replica::new(
                    config.replica_id,
                    app,
                    pbft::ToReplicaMessageNet::new(IndexNet::new(
                        net.clone(),
                        config.replica_addrs,
                        config.replica_id as usize,
                    )),
                    pbft::ToClientMessageNet::new(net.clone()),
                    Box::new(pbft::CryptoWorker::from(crypto_worker))
                        as Box<
                            dyn Submit<Crypto, dyn pbft::SendCryptoEvent<SocketAddr>> + Send + Sync,
                        >,
                    config.num_replica,
                    config.num_faulty,
                )));
                runtime.block_on(replica_session(
                    state,
                    pbft::to_replica_on_buf,
                    net,
                    move |sender| async move { crypto_executor.run(crypto, sender).await },
                    session_cancel,
                ))
            }
        }
    });
    let replaced = session.replace((handle, cancel));
    assert!(replaced.is_none())
}

async fn replica_session<
    S: OnEventUniversal<SessionTimer, Event = Event<S, SessionTimer>>
        + OnTimerUniversal<SessionTimer>
        + Send
        + 'static,
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
>(
    mut state: S,
    on_buf: impl Fn(&[u8], &mut Sender<S>) -> anyhow::Result<()> + Send + Sync + 'static,
    net: Udp,
    crypto_session: impl FnOnce(Sender<S>) -> F,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut session = Session::new();
    let mut recv_session = spawn({
        let mut sender = Sender::from(session.sender());
        async move { net.recv_session(|buf| on_buf(buf, &mut sender)).await }
    });
    let mut crypto_session = spawn({
        let sender = Sender::from(session.sender());
        crypto_session(sender)
    });
    let mut state_session = spawn(async move { session.run(&mut state).await });
    'select: {
        tokio::select! {
            result = &mut recv_session => result??,
            result = &mut crypto_session => result??,
            result = &mut state_session => result??,
            () = cancel.cancelled() => break 'select,
        }
        return Err(anyhow::format_err!("unreachable"));
    }
    recv_session.abort();
    crypto_session.abort();
    state_session.abort();
    let _ = recv_session.await;
    let _ = crypto_session.await;
    let _ = state_session.await;
    Ok(())
}

async fn stop_replica(State(state): State<AppState>) {
    let (handle, cancel) = {
        let mut session = state.session.lock().unwrap();
        session.take().unwrap()
    };
    cancel.cancel();
    handle.await.unwrap().unwrap()
}

// cSpell:words unreplicated pbft upcall ycsb seedable schnorrkel secp256k1
