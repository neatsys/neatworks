mod client;
mod replica;

use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use replication_control_messages::{BenchmarkResult, ClientConfig, ReplicaConfig};
use tokio::{signal::ctrl_c, task::JoinHandle};
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
    let task = || {
        let replaced = session.replace((
            tokio::spawn(client::session(config, state.benchmark_result.clone())),
            CancellationToken::new(),
        ));
        anyhow::ensure!(replaced.is_none());
        Ok(())
    };
    match task() {
        Ok(()) => StatusCode::OK,
        Err(err) => {
            warn!("{err}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
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

async fn start_replica(
    State(state): State<AppState>,
    Json(config): Json<ReplicaConfig>,
) -> StatusCode {
    let mut session = state.session.lock().unwrap();
    let task = || {
        let cancel = CancellationToken::new();
        let replaced = session.replace((
            tokio::spawn(replica::session(config, cancel.clone())?),
            cancel,
        ));
        anyhow::ensure!(replaced.is_none());
        Ok(())
    };
    match task() {
        Ok(()) => StatusCode::OK,
        Err(err) => {
            warn!("{err}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
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
