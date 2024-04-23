mod client;
mod replica;

use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
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

fn take_finished(state: AppState) -> anyhow::Result<Option<JoinHandle<anyhow::Result<()>>>> {
    let mut session = state
        .session
        .lock()
        .map_err(|err| anyhow::format_err!("{err}"))?;
    let Some((handle, _)) = session.as_mut() else {
        return Ok(None); // consider fail for unexpected taking
    };
    Ok(if !handle.is_finished() {
        None
    } else {
        Some(session.take().unwrap().0)
    })
}

async fn ok(State(state): State<AppState>) -> Response {
    run(async {
        if let Some(handle) = take_finished(state)? {
            handle.await??;
            anyhow::bail!("unreachable")
        }
        Ok(())
    })
    .await
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

async fn take_benchmark_result(State(state): State<AppState>) -> Response {
    run(async {
        let result = state
            .benchmark_result
            .lock()
            .map_err(|err| anyhow::format_err!("{err}"))?
            .take();
        if result.is_some() {
            let (session, _) = {
                state
                    .session
                    .lock()
                    .map_err(|err| anyhow::format_err!("{err}"))?
                    .take()
                    .unwrap()
            };
            session.await??
        }
        anyhow::Ok(Json(result))
    })
    .await
}

async fn run<T: IntoResponse>(task: impl Future<Output = anyhow::Result<T>>) -> Response {
    match task.await {
        Ok(result) => result.into_response(),
        Err(err) => {
            warn!("{err}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn start_replica(
    State(state): State<AppState>,
    Json(config): Json<ReplicaConfig>,
) -> StatusCode {
    let task = || {
        let mut session = state
            .session
            .lock()
            .map_err(|err| anyhow::format_err!("{err}"))?;
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

async fn stop_replica(State(state): State<AppState>) -> Response {
    run(async {
        let (handle, cancel) = {
            let mut session = state
                .session
                .lock()
                .map_err(|err| anyhow::format_err!("{err}"))?;
            session
                .take()
                .ok_or(anyhow::format_err!("missing session"))?
        };
        cancel.cancel();
        handle.await??;
        anyhow::Ok(())
    })
    .await
}

// cSpell:words unreplicated pbft upcall ycsb seedable schnorrkel secp256k1
