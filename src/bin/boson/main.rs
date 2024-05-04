mod cops;
mod mutex;
mod quorum;

use std::{
    backtrace::BacktraceStatus,
    sync::Arc,
    time::{Duration, SystemTime},
};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use tokio::{
    signal::ctrl_c,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinHandle,
    time::{sleep, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // tracing_subscriber::fmt::init();
    use tracing::Level;
    use tracing_subscriber::{
        filter::Targets, fmt::format::FmtSpan, layer::SubscriberExt as _,
        util::SubscriberInitExt as _,
    };
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_max_level(Level::TRACE)
        .finish()
        .with(if let Ok(var) = std::env::var("RUST_LOG") {
            var.parse()?
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
    let runtime = tokio::runtime::Runtime::new()?;
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/mutex/start", post(mutex_start))
        .route("/mutex/stop", post(stop))
        .route("/mutex/request", post(mutex_request))
        .route("/cops/start-client", post(cops_start_client))
        .route("/cops/poll-results", post(cops_poll_results))
        .route("/cops/start-server", post(cops_start_server))
        .route("/cops/stop-server", post(stop))
        .route("/start-quorum", post(start_quorum))
        .route("/stop-quorum", post(stop))
        .with_state(AppState {
            session: Default::default(),
            channel: Default::default(),
            handle: runtime.handle().clone(),
        });
    let ip = std::env::args().nth(1);
    let ip = ip.as_deref().unwrap_or("0.0.0.0");
    let listener = tokio::net::TcpListener::bind(format!("{ip}:3000")).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .await?;
    runtime.shutdown_background();
    Ok(())
}

#[derive(Debug, Clone)]
struct AppState {
    session: Arc<Mutex<Option<AppSession>>>,
    channel: Arc<Mutex<Option<AppChannel>>>,
    handle: tokio::runtime::Handle,
}

#[derive(Debug)]
struct AppSession {
    handle: JoinHandle<anyhow::Result<()>>,
    cancel: CancellationToken,
}

#[derive(Debug)]
struct AppChannel {
    event_sender: UnboundedSender<mutex::Event>,
    upcall: UnboundedReceiver<Upcall>,
}

#[derive(Debug, derive_more::From)]
enum Upcall {
    RequestOk(augustus::lamport_mutex::events::RequestOk),
    Latencies(Vec<Duration>),
}

fn log_exit(err: anyhow::Error) -> StatusCode {
    let err = err;
    warn!("{err}");
    if err.backtrace().status() == BacktraceStatus::Captured {
        warn!("\n{}", err.backtrace())
    } else {
        warn!("{}", err.backtrace())
    }
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn ok(State(state): State<AppState>) -> StatusCode {
    let mut state_session = state.session.lock().await;
    let Some(session) = &mut *state_session else {
        return StatusCode::NOT_FOUND;
    };
    if !session.handle.is_finished() {
        return StatusCode::OK;
    }
    match state_session.take().unwrap().handle.await {
        Err(err) => warn!("{err}"),
        Ok(Err(err)) => return log_exit(err),
        Ok(Ok(())) => warn!("unexpected peer exit"),
    }
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn mutex_start(
    State(state): State<AppState>,
    Json(config): Json<boson_control_messages::Mutex>,
) -> StatusCode {
    if let Err(err) = async {
        let mut session = state.session.lock().await;
        anyhow::ensure!(session.is_none());
        let (event_sender, event_receiver) = unbounded_channel();
        let (upcall_sender, upcall_receiver) = unbounded_channel();
        let cancel = CancellationToken::new();
        use boson_control_messages::Variant::*;
        let handle = match &config.variant {
            Untrusted => state.handle.spawn(mutex::untrusted_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
            Replicated(_) => state.handle.spawn(mutex::replicated_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
            Quorum(_) => state.handle.spawn(mutex::quorum_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
            NitroEnclaves => state.handle.spawn(mutex::nitro_enclaves_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
        };
        *session = Some(AppSession { handle, cancel });
        let replaced = state.channel.lock().await.replace(AppChannel {
            event_sender,
            upcall: upcall_receiver,
        });
        anyhow::ensure!(replaced.is_none());
        anyhow::Ok(())
    }
    .await
    {
        log_exit(err)
    } else {
        StatusCode::OK
    }
}

async fn stop(State(state): State<AppState>) -> StatusCode {
    if let Err(err) = async {
        let Some(session) = state.session.lock().await.take() else {
            anyhow::bail!("missing session")
        };
        session.cancel.cancel();
        session.handle.await??;
        let Some(channel) = state.channel.lock().await.take() else {
            anyhow::bail!("missing channel")
        };
        drop(channel);
        Ok(())
    }
    .await
    {
        log_exit(err)
    } else {
        StatusCode::OK
    }
}

async fn mutex_request(State(state): State<AppState>, at: Json<SystemTime>) -> Response {
    let task = async {
        sleep(at.duration_since(SystemTime::now())?).await;
        let mut channel = state.channel.lock().await;
        let Some(channel) = channel.as_mut() else {
            anyhow::bail!("missing session")
        };
        let start = Instant::now();
        channel.event_sender.send(mutex::Event::Request)?;
        let Some(result) = channel.upcall.recv().await else {
            state.session.lock().await.take().unwrap().handle.await??;
            anyhow::bail!("unreachable")
        };
        anyhow::ensure!(matches!(result, Upcall::RequestOk(_)), "{result:?}");
        channel.event_sender.send(mutex::Event::Release)?;
        Ok(Json(start.elapsed()))
    };
    match task.await {
        Ok(elapsed) => elapsed.into_response(),
        Err(err) => log_exit(err).into_response(),
    }
}

async fn cops_start_client(
    State(state): State<AppState>,
    Json(config): Json<boson_control_messages::CopsClient>,
) -> StatusCode {
    if let Err(err) = async {
        let mut session = state.session.lock().await;
        anyhow::ensure!(session.is_none());
        let (event_sender, _) = unbounded_channel();
        let (upcall_sender, upcall_receiver) = unbounded_channel();
        let cancel = CancellationToken::new();
        use boson_control_messages::Variant::*;
        let handle = match &config.variant {
            Untrusted => state
                .handle
                .spawn(cops::untrusted_client_session(config, upcall_sender)),
            Replicated(_) => state
                .handle
                .spawn(cops::pbft_client_session(config, upcall_sender)),
            Quorum(_) => state
                .handle
                .spawn(cops::quorum_client_session(config, upcall_sender)),
            NitroEnclaves => state
                .handle
                .spawn(cops::nitro_enclaves_client_session(config, upcall_sender)),
        };
        *session = Some(AppSession { handle, cancel });
        let replaced = state.channel.lock().await.replace(AppChannel {
            event_sender,
            upcall: upcall_receiver,
        });
        anyhow::ensure!(replaced.is_none());
        anyhow::Ok(())
    }
    .await
    {
        log_exit(err)
    } else {
        StatusCode::OK
    }
}

async fn cops_poll_results(State(state): State<AppState>) -> Response {
    let task = async {
        let mut state_channel = state.channel.lock().await;
        let Some(channel) = state_channel.as_mut() else {
            anyhow::bail!("unimplemented")
        };
        // waiting for `Option::take_if` to stabilize
        if !channel.upcall.is_closed() {
            return Ok(None);
        }
        let mut channel = state_channel.take().unwrap();
        let mut latencies = Vec::new();
        while let Ok(result) = channel.upcall.try_recv() {
            let Upcall::Latencies(some_latencies) = result else {
                anyhow::bail!("unimplemented")
            };
            latencies.extend(some_latencies)
        }
        state.session.lock().await.take().unwrap().handle.await??;
        let throughput = latencies.len() as f32 / 10.;
        latencies.sort_unstable();
        let latency = latencies
            .get(latencies.len() / 2)
            .copied()
            .unwrap_or_default();
        let latency_99 = latencies
            .get(latencies.len() * 99 / 100)
            .copied()
            .unwrap_or_default();
        Ok(Some((throughput, latency, latency_99)))
    };
    match task.await {
        Ok(result) => Json(result).into_response(),
        Err(err) => log_exit(err).into_response(),
    }
}

async fn cops_start_server(
    State(state): State<AppState>,
    Json(config): Json<boson_control_messages::CopsServer>,
) -> StatusCode {
    if let Err(err) = async {
        let mut session = state.session.lock().await;
        anyhow::ensure!(session.is_none());
        let (event_sender, _) = unbounded_channel();
        let (_, upcall_receiver) = unbounded_channel();
        let cancel = CancellationToken::new();
        use boson_control_messages::Variant::*;
        let handle = match &config.variant {
            Untrusted => state
                .handle
                .spawn(cops::untrusted_server_session(config, cancel.clone())),
            Replicated(_) => state
                .handle
                .spawn(cops::pbft_server_session(config, cancel.clone())),
            Quorum(_) => state
                .handle
                .spawn(cops::quorum_server_session(config, cancel.clone())),
            NitroEnclaves => state
                .handle
                .spawn(cops::nitro_enclaves_server_session(config, cancel.clone())),
        };
        *session = Some(AppSession { handle, cancel });
        let replaced = state.channel.lock().await.replace(AppChannel {
            event_sender,
            upcall: upcall_receiver,
        });
        anyhow::ensure!(replaced.is_none());
        anyhow::Ok(())
    }
    .await
    {
        log_exit(err)
    } else {
        StatusCode::OK
    }
}

async fn start_quorum(
    State(state): State<AppState>,
    Json(config): Json<boson_control_messages::QuorumServer>,
) -> StatusCode {
    if let Err(err) = async {
        let mut session = state.session.lock().await;
        anyhow::ensure!(session.is_none());
        let (event_sender, _) = unbounded_channel();
        let (_, upcall_receiver) = unbounded_channel();
        let cancel = CancellationToken::new();
        let handle = state.handle.spawn(quorum::session(config, cancel.clone()));
        *session = Some(AppSession { handle, cancel });
        let replaced = state.channel.lock().await.replace(AppChannel {
            event_sender,
            upcall: upcall_receiver,
        });
        anyhow::ensure!(replaced.is_none());
        anyhow::Ok(())
    }
    .await
    {
        log_exit(err)
    } else {
        StatusCode::OK
    }
}

// cSpell:words upcall lamport pbft
// cSpell:ignore rlimit setrlimit getrlimit nofile
