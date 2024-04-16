use std::{backtrace::BacktraceStatus, sync::Arc};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use boson_control_messages::MutexUntrusted;
use tokio::{
    signal::ctrl_c,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

mod boson_mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/mutex/start", post(mutex_start))
        .route("/mutex/stop", post(mutex_stop))
        .route("/mutex/request", post(mutex_request))
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
    upcall: UnboundedReceiver<augustus::lamport_mutex::event::RequestOk>,
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
    Json(config): Json<MutexUntrusted>,
) -> StatusCode {
    if let Err(err) = async {
        let mut session = state.session.lock().await;
        anyhow::ensure!(session.is_none());
        let (event_sender, event_receiver) = unbounded_channel();
        let (upcall_sender, upcall_receiver) = unbounded_channel();
        let cancel = CancellationToken::new();
        *session = Some(AppSession {
            handle: tokio::spawn(boson_mutex::untrusted_session(
                config,
                event_receiver,
                upcall_sender,
                cancel.clone(),
            )),
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

async fn mutex_stop(State(state): State<AppState>) -> StatusCode {
    if let Err(err) = async {
        let mut session = state.session.lock().await;
        let Some(session) = session.take() else {
            anyhow::bail!("unimplemented")
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

async fn mutex_request(State(state): State<AppState>) -> Response {
    let task = async {
        let mut session = state.session.lock().await;
        let Some(session) = session.as_mut() else {
            anyhow::bail!("unimplemented")
        };
        let start = Instant::now();
        session.event_sender.send(boson_mutex::Event::Request)?;
        // TODO timeout
        session.upcall.recv().await;
        session.event_sender.send(boson_mutex::Event::Release)?;
        Ok(Json(start.elapsed()))
    };
    match task.await {
        Ok(elapsed) => elapsed.into_response(),
        Err(err) => log_exit(err).into_response(),
    }
}

// cSpell:words upcall lamport
