use std::{
    iter::repeat_with,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use augustus::{
    app::Null,
    event::{OnEvent, Session, SessionSender},
    net::Udp,
    replication::{Concurrent, ConcurrentEvent, ReplicaNet},
    unreplicated,
};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use replication_control_messages::BenchmarkResult;
use tokio::{
    runtime,
    signal::ctrl_c,
    spawn,
    task::{spawn_blocking, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/start-client", post(start_client))
        .route("/benchmark-result", get(benchmark_result))
        .route("/start-replica", post(start_replica))
        .route("/stop-replica", post(stop_replica))
        .with_state(AppState {
            session: Default::default(),
            benchmark_result: Default::default(),
        });
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
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

async fn start_client(State(state): State<AppState>) {
    let mut session = state.session.lock().unwrap();
    let cancel = CancellationToken::new();
    let benchmark_result = state.benchmark_result.clone();
    let handle = spawn_blocking(|| {
        let runtime = runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let replica_addrs = vec![SocketAddr::from(([127, 0, 0, 1], 3001))];
        let new_state = |client_id, addr, net, upcall| {
            unreplicated::Client::new(
                client_id,
                addr,
                unreplicated::ToReplicaMessageNet(ReplicaNet::new(net, replica_addrs.clone())),
                upcall,
            )
        };
        runtime.block_on(client_session(
            new_state,
            unreplicated::to_client_on_buf,
            benchmark_result,
        ))
    });
    let replaced = session.replace((handle, cancel));
    assert!(replaced.is_none())
}

async fn client_session<S: OnEvent<M> + Send + 'static, M: Send + 'static>(
    mut new_state: impl FnMut(u32, SocketAddr, Udp, SessionSender<ConcurrentEvent>) -> S,
    on_buf: impl Fn(&SessionSender<M>, &[u8]) -> anyhow::Result<()> + Clone + Send + Sync + 'static,
    benchmark_result: Arc<Mutex<Option<BenchmarkResult>>>,
) -> anyhow::Result<()>
where
    Vec<u8>: Into<M>,
{
    let mut concurrent = Concurrent::new();
    let mut concurrent_session = Session::new();
    let mut sessions = JoinSet::new();
    for client_id in repeat_with(rand::random).take(1) {
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
        let addr = socket.local_addr()?;
        let net = Udp(socket.into());
        let mut state = new_state(client_id, addr, net.clone(), concurrent_session.sender());
        let mut session = Session::new();
        concurrent.insert_client_sender(client_id, session.sender())?;
        let sender = session.sender();
        let on_buf = on_buf.clone();
        sessions.spawn(async move { net.recv_session(|buf| on_buf(&sender, buf)).await });
        sessions.spawn(async move { session.run(&mut state).await });
    }
    concurrent.launch()?;
    'select: {
        tokio::select! {
            result = concurrent_session.run(&mut concurrent) => result?,
            result = sessions.join_next() => result.unwrap()??,
            () = tokio::time::sleep(Duration::from_secs(1)) => break 'select,
        }
        return Err(anyhow::anyhow!("unexpected shutdown"));
    }
    let throughput = concurrent.latencies.len() as f32;
    let latency =
        concurrent.latencies.into_iter().sum::<Duration>() / (throughput.floor() as u32 + 1);
    benchmark_result.lock().unwrap().replace(BenchmarkResult {
        throughput,
        latency,
    });
    Ok(())
}

async fn benchmark_result(State(state): State<AppState>) -> Json<Option<BenchmarkResult>> {
    state.benchmark_result.lock().unwrap().clone().into()
}

async fn start_replica(State(state): State<AppState>) {
    let mut session = state.session.lock().unwrap();
    let cancel = CancellationToken::new();
    let session_cancel = cancel.clone();
    let handle = spawn_blocking(move || {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let socket = runtime.block_on(tokio::net::UdpSocket::bind("0.0.0.0:3001"))?;
        let net = Udp(socket.into());
        let state = unreplicated::Replica::new(Null, unreplicated::ToClientMessageNet(net.clone()));
        runtime.block_on(replica_session(
            state,
            unreplicated::to_replica_on_buf,
            net,
            session_cancel,
        ))
    });
    let replaced = session.replace((handle, cancel));
    assert!(replaced.is_none())
}

async fn replica_session<M: Send + 'static>(
    mut state: impl OnEvent<M> + Send + 'static,
    on_buf: impl Fn(&SessionSender<M>, &[u8]) -> anyhow::Result<()> + Send + Sync + 'static,
    net: Udp,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut session = Session::new();
    let recv_session = spawn({
        let sender = session.sender();
        async move { net.recv_session(|buf| on_buf(&sender, buf)).await }
    });
    let state_session = spawn(async move { session.run(&mut state).await });
    tokio::select! {
        result = recv_session => result??,
        result = state_session => result??,
        () = cancel.cancelled() => return Ok(()),
    }
    Err(anyhow::anyhow!("unexpected shutdown"))
}

async fn stop_replica(State(state): State<AppState>) {
    let (handle, cancel) = {
        let mut session = state.session.lock().unwrap();
        session.take().unwrap()
    };
    cancel.cancel();
    handle.await.unwrap().unwrap()
}
