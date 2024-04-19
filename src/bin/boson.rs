mod boson_cops;
mod boson_mutex;
mod boson_quorum;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
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
}

#[derive(Debug)]
struct AppSession {
    handle: JoinHandle<anyhow::Result<()>>,
    cancel: CancellationToken,
    event_sender: UnboundedSender<boson_mutex::Event>,
    upcall: UnboundedReceiver<Upcall>,
}

#[derive(derive_more::From)]
enum Upcall {
    RequestOk(augustus::lamport_mutex::events::RequestOk),
    ThroughputLatency(f32, Duration),
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
    let mut session = state.session.lock().await;
    {
        let Some(session) = &mut *session else {
            return StatusCode::NOT_FOUND;
        };
        if !session.handle.is_finished() {
            return StatusCode::OK;
        }
    }
    match session.take().unwrap().handle.await {
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
            Untrusted => tokio::spawn(boson_mutex::untrusted_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
            Replicated(_) => tokio::spawn(boson_mutex::replicated_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
            Quorum(_) => tokio::spawn(boson_mutex::quorum_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
        };
        *session = Some(AppSession {
            handle,
            cancel,
            event_sender,
            upcall: upcall_receiver,
        });
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
        let mut session = state.session.lock().await;
        let Some(session) = session.take() else {
            anyhow::bail!("missing session")
        };
        session.cancel.cancel();
        session.handle.await?
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
        let mut session = state.session.lock().await;
        let Some(session) = session.as_mut() else {
            anyhow::bail!("missing session")
        };
        let start = Instant::now();
        session.event_sender.send(boson_mutex::Event::Request)?;
        // TODO timeout
        let result = session.upcall.recv().await;
        anyhow::ensure!(matches!(result, Some(Upcall::RequestOk(_))));
        session.event_sender.send(boson_mutex::Event::Release)?;
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
            Untrusted => tokio::spawn(boson_cops::untrusted_client_session(config, upcall_sender)),
            Replicated(_) => tokio::spawn(boson_cops::pbft_client_session(config, upcall_sender)),
            Quorum(_) => tokio::spawn(boson_cops::quorum_client_session(config, upcall_sender)),
        };
        *session = Some(AppSession {
            handle,
            cancel,
            event_sender,
            upcall: upcall_receiver,
        });
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
        let mut state_session = state.session.lock().await;
        let Some(session) = state_session.as_mut() else {
            anyhow::bail!("unimplemented")
        };
        if !session.upcall.is_closed() {
            Ok(None)
        } else {
            let mut results = Vec::new();
            while let Ok(result) = session.upcall.try_recv() {
                let Upcall::ThroughputLatency(throughput, latency) = result else {
                    anyhow::bail!("unimplemented")
                };
                results.push((throughput, latency))
            }
            state_session.take().unwrap().handle.await??;
            Ok(Some(results))
        }
    };
    match task.await {
        Ok(results) => Json(results).into_response(),
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
            Untrusted => tokio::spawn(boson_cops::untrusted_server_session(config, cancel.clone())),
            Replicated(_) => tokio::spawn(boson_cops::pbft_server_session(config, cancel.clone())),
            Quorum(_) => tokio::spawn(boson_cops::quorum_server_session(config, cancel.clone())),
        };
        *session = Some(AppSession {
            handle,
            cancel,
            event_sender,
            upcall: upcall_receiver,
        });
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
        let handle = tokio::spawn(boson_quorum::session(config, cancel.clone()));
        *session = Some(AppSession {
            handle,
            cancel,
            event_sender,
            upcall: upcall_receiver,
        });
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
