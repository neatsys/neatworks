use std::{
    future::{pending, Future},
    iter::repeat_with,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use augustus::{
    app::{ycsb, App, Sqlite},
    crypto::Crypto,
    event::{
        erased::{
            session::{Buffered, Sender},
            Blanket, OnEventFixTimer, OnTimerFixTimer, Session, Unify,
        },
        session::SessionTimer,
        SendEvent,
    },
    net::{session::Udp, IndexNet},
    pbft, unreplicated,
    worker::erased::spawn_backend,
    workload::{CloseLoop, Invoke, InvokeOk, Iter, OpLatency, Workload},
};
use axum::{
    extract::State,
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
    sync::Barrier,
    task::{spawn_blocking, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
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

async fn start_client(State(state): State<AppState>, Json(config): Json<ClientConfig>) {
    let mut session = state.session.lock().unwrap();
    let cancel = CancellationToken::new();
    let benchmark_result = state.benchmark_result.clone();
    benchmark_result.lock().unwrap().take();
    let handle = spawn_blocking(move || {
        let runtime = &runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        match config.protocol {
            Protocol::Unreplicated => {
                runtime.block_on(client_session::<Unify<unreplicated::Client<_, _, _>>>(
                    config,
                    unreplicated::erased::to_client_on_buf,
                    benchmark_result,
                ))
            }
            Protocol::Pbft => runtime.block_on(client_session::<Buffered<pbft::Client<_>>>(
                config,
                pbft::to_client_on_buf,
                benchmark_result,
            )),
        }
    });
    let replaced = session.replace((handle, cancel));
    assert!(replaced.is_none())
}

trait NewClient<S> {
    fn new_client(
        &self,
        id: u32,
        addr: SocketAddr,
        net: Udp,
        upcall: impl SendEvent<InvokeOk> + Send + Sync + 'static,
    ) -> S;
}

impl
    NewClient<
        Unify<
            unreplicated::Client<
                Box<dyn unreplicated::ToReplicaNet<SocketAddr> + Send + Sync>,
                Box<dyn unreplicated::ClientUpcall + Send + Sync>,
                SocketAddr,
            >,
        >,
    > for ClientConfig
{
    fn new_client(
        &self,
        id: u32,
        addr: SocketAddr,
        net: Udp,
        upcall: impl SendEvent<InvokeOk> + Send + Sync + 'static,
    ) -> Unify<
        unreplicated::Client<
            Box<dyn unreplicated::ToReplicaNet<SocketAddr> + Send + Sync>,
            Box<dyn unreplicated::ClientUpcall + Send + Sync>,
            SocketAddr,
        >,
    > {
        Unify(unreplicated::Client::new(
            id,
            addr,
            Box::new(unreplicated::ToReplicaMessageNet::new(IndexNet::new(
                net,
                self.replica_addrs.clone(),
                None,
            ))),
            Box::new(upcall),
        ))
    }
}

impl NewClient<Buffered<pbft::Client<SocketAddr>>> for ClientConfig {
    fn new_client(
        &self,
        id: u32,
        addr: SocketAddr,
        net: Udp,
        upcall: impl SendEvent<InvokeOk> + Send + Sync + 'static,
    ) -> Buffered<pbft::Client<SocketAddr>> {
        Buffered::from(pbft::Client::new(
            id,
            addr,
            pbft::ToReplicaMessageNet::new(IndexNet::new(net, self.replica_addrs.clone(), None)),
            upcall,
            self.num_replica,
            self.num_faulty,
        ))
    }
}

async fn client_session<
    S: OnEventFixTimer<Invoke, SessionTimer> + OnTimerFixTimer<SessionTimer> + Send + Sync + 'static,
>(
    config: ClientConfig,
    on_buf: impl Fn(&[u8], &mut Sender<S>) -> anyhow::Result<()> + Clone + Send + Sync + 'static,
    benchmark_result: Arc<Mutex<Option<BenchmarkResult>>>,
) -> anyhow::Result<()>
where
    ClientConfig: NewClient<S>,
{
    let mut sessions = JoinSet::new();
    let stop = CancellationToken::new();
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(config.num_close_loop + 1));

    use replication_control_messages::App::*;
    match &config.app {
        Null => {
            spawn_client_sessions(
                &mut sessions,
                config,
                on_buf,
                || OpLatency::new(Iter(repeat_with(Default::default))),
                stop.clone(),
                latencies.clone(),
                barrier.clone(),
            )
            .await?
        }
        Ycsb(ycsb_config) => {
            use replication_control_messages::YcsbProfile::*;
            let workload = ycsb::Workload::new(
                StdRng::seed_from_u64(117418),
                match &ycsb_config.profile {
                    A => ycsb::WorkloadSettings::new_a,
                    B => ycsb::WorkloadSettings::new_b,
                    C => ycsb::WorkloadSettings::new_c,
                    D => ycsb::WorkloadSettings::new_d,
                    E => ycsb::WorkloadSettings::new_e,
                    F => ycsb::WorkloadSettings::new_f,
                }(ycsb_config.record_count),
            )?;
            let mut i = 0;
            spawn_client_sessions(
                &mut sessions,
                config,
                on_buf,
                || {
                    i += 1;
                    workload.clone_reseed(StdRng::seed_from_u64(117418 + i))
                },
                stop.clone(),
                latencies.clone(),
                barrier.clone(),
            )
            .await?
        }
    }

    // TODO escape with an error indicating the root problem instead of a disconnected channel error
    // caused by the problem
    // is it (easily) possible?
    'select: {
        tokio::select! {
            result = sessions.join_next() => result.unwrap()??,
            () = tokio::time::sleep(Duration::from_secs(1)) => break 'select,
            // () = cancel.cancelled() => break 'select,
        }
        return Err(anyhow::anyhow!("unexpected shutdown"));
    }
    stop.cancel();
    barrier.wait().await;
    sessions.shutdown().await;
    let mut latencies = latencies.lock().unwrap();
    let throughput = latencies.len() as f32;
    benchmark_result.lock().unwrap().replace(BenchmarkResult {
        throughput: latencies.len() as f32,
        latency: latencies.drain(..).sum::<Duration>() / (throughput.floor() as u32 + 1),
    });
    Ok(())
}

async fn spawn_client_sessions<
    S: OnEventFixTimer<Invoke, SessionTimer> + OnTimerFixTimer<SessionTimer> + Send + Sync + 'static,
    W: Workload + Into<Vec<Duration>> + Send + Sync + 'static,
>(
    sessions: &mut JoinSet<anyhow::Result<()>>,
    config: ClientConfig,
    on_buf: impl Fn(&[u8], &mut Sender<S>) -> anyhow::Result<()> + Clone + Send + Sync + 'static,
    mut workload: impl FnMut() -> W,
    stop: CancellationToken,
    latencies: Arc<Mutex<Vec<Duration>>>,
    barrier: Arc<Barrier>,
) -> anyhow::Result<()>
where
    ClientConfig: NewClient<S>,
    W::Attach: Send + Sync,
{
    for client_id in repeat_with(rand::random).take(config.num_close_loop) {
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
        let addr = socket.local_addr()?;
        println!("Client {client_id:08x} bind to {addr}");
        let net = Udp(socket.into());

        let mut session = Session::new();
        let mut close_loop_session = Session::new();

        let mut state = Blanket(config.new_client(
            client_id,
            addr,
            net.clone(),
            Sender::from(close_loop_session.sender()),
        ));
        let mut close_loop = Blanket(Unify(CloseLoop::new(
            Sender::from(session.sender()),
            workload(),
        )));

        let mut sender = Sender::from(session.sender());
        let on_buf = on_buf.clone();
        sessions.spawn(async move { net.recv_session(|buf| on_buf(buf, &mut sender)).await });
        sessions.spawn(async move { session.run(&mut state).await });
        let stop = stop.clone();
        let latencies = latencies.clone();
        let barrier = barrier.clone();
        sessions.spawn(async move {
            close_loop.launch()?;
            tokio::select! {
                result = close_loop_session.run(&mut close_loop) => result?,
                () = stop.cancelled() => {}
            }
            latencies
                .lock()
                .unwrap()
                .extend(close_loop.0 .0.workload.into());
            barrier.wait().await;
            Ok(())
        });
    }
    Ok(())
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
                app.execute(&op).unwrap();
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

        let crypto = Crypto::new_hardcoded_replication(config.num_replica, config.replica_id)?;
        let (crypto_worker, mut crypto_executor) = spawn_backend(crypto);

        match config.protocol {
            Protocol::Unreplicated => {
                assert_eq!(config.replica_id, 0);
                let state = Unify(unreplicated::Replica::new(
                    app,
                    Box::new(unreplicated::ToClientMessageNet::new(net.clone())),
                ));
                runtime.block_on(replica_session(
                    state,
                    unreplicated::erased::to_replica_on_buf,
                    net,
                    |_| pending(),
                    session_cancel,
                ))
            }
            Protocol::Pbft => {
                let state = Buffered::from(pbft::Replica::<_, SocketAddr>::new(
                    config.replica_id,
                    app,
                    pbft::ToReplicaMessageNet::new(IndexNet::new(
                        net.clone(),
                        config.replica_addrs,
                        config.replica_id as usize,
                    )),
                    pbft::ToClientMessageNet::new(net.clone()),
                    crypto_worker,
                    config.num_replica,
                    config.num_faulty,
                ));
                runtime.block_on(replica_session(
                    state,
                    pbft::to_replica_on_buf,
                    net,
                    move |sender| async move { crypto_executor.run(sender, |sender| sender).await },
                    session_cancel,
                ))
            }
        }
    });
    let replaced = session.replace((handle, cancel));
    assert!(replaced.is_none())
}

async fn replica_session<
    S: OnTimerFixTimer<SessionTimer> + Send + 'static,
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
>(
    state: S,
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
    let mut state = Blanket(state);
    let mut state_session = spawn(async move { session.run(&mut state).await });
    'select: {
        tokio::select! {
            result = &mut recv_session => result??,
            result = &mut crypto_session => result??,
            result = &mut state_session => result??,
            () = cancel.cancelled() => break 'select,
        }
        return Err(anyhow::anyhow!("unexpected shutdown"));
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
