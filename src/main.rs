use std::sync::{Arc, Mutex};

use augustus::{
    app::Null,
    event::{OnEvent, Session, SessionSender},
    net::Udp,
    unreplicated,
};
use axum::{
    extract::State,
    routing::{get, post},
    Router,
};
use tokio::{
    runtime,
    signal::ctrl_c,
    spawn,
    task::{spawn_blocking, JoinHandle},
};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/start-replica", post(start_replica))
        .route("/stop-replica", post(stop_replica))
        .with_state(Arc::new(AppState {
            session: Mutex::new(None),
        }));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .await?;
    Ok(())
}

#[derive(Debug)]
struct AppState {
    session: Mutex<Option<(JoinHandle<anyhow::Result<()>>, CancellationToken)>>,
}

async fn ok(State(state): State<Arc<AppState>>) {
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

async fn start_replica(State(state): State<Arc<AppState>>) {
    let mut session = state.session.lock().unwrap();
    let cancel = CancellationToken::new();
    let session_cancel = cancel.clone();
    let handle = spawn_blocking(move || {
        let runtime = &runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let socket = runtime
            .block_on(tokio::net::UdpSocket::bind("0.0.0.0:3001"))
            .unwrap();
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
        () = cancel.cancelled() => return Ok(())
    }
    Err(anyhow::anyhow!("unexpected shutdown"))
}

async fn stop_replica(State(state): State<Arc<AppState>>) {
    let (handle, cancel) = {
        let mut session = state.session.lock().unwrap();
        session.take().unwrap()
    };
    cancel.cancel();
    handle.await.unwrap().unwrap()
}
