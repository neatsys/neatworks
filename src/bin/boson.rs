use std::{backtrace::BacktraceStatus, sync::Arc};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use tokio::{
    signal::ctrl_c,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

mod boson_mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

async fn ok(State(state): State<AppState>) -> (StatusCode, &'static str) {
    let mut session = state.session.lock().await;
    {
        let Some(session) = &mut *session else {
            return (StatusCode::NOT_FOUND, "no session");
        };
        if !session.handle.is_finished() {
            return (StatusCode::OK, "ok");
        }
    }
    match session.take().unwrap().handle.await {
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
    (StatusCode::INTERNAL_SERVER_ERROR, "err")
}

async fn mutex_start(State(state): State<AppState>) {
    let mut session = state.session.lock().await;
    assert!(session.is_none());
    let (event_sender, event_receiver) = unbounded_channel();
    let (upcall_sender, upcall_receiver) = unbounded_channel();
    let cancel = CancellationToken::new();
    *session = Some(AppSession {
        handle: tokio::spawn(boson_mutex::untrusted_session(
            event_receiver,
            upcall_sender,
            cancel.clone(),
        )),
        cancel,
        event_sender,
        upcall: upcall_receiver,
    })
}

async fn mutex_stop(State(state): State<AppState>) {
    let mut session = state.session.lock().await;
    let Some(session) = session.take() else {
        unimplemented!()
    };
    session.cancel.cancel();
    session.handle.await.unwrap().unwrap()
}

async fn mutex_request(State(state): State<AppState>) {
    let mut session = state.session.lock().await;
    let Some(session) = session.as_mut() else {
        unimplemented!()
    };
    session
        .event_sender
        .send(boson_mutex::Event::Request)
        .unwrap();
    // TODO timeout
    session.upcall.recv().await;
    session
        .event_sender
        .send(boson_mutex::Event::Release)
        .unwrap();
}

// cSpell:words upcall lamport
