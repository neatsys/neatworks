use std::{
    env::args,
    sync::{Arc, Mutex},
};

use axum::{extract::State, routing::get, Router};
use tokio::{signal::ctrl_c, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let app = Router::new().route("/ok", get(ok)).with_state(AppState {
        session: Default::default(),
    });
    let url = args().nth(1).ok_or(anyhow::anyhow!("not specify url"))?;
    let listener = tokio::net::TcpListener::bind(&url).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct AppState {
    session: Arc<Mutex<Option<AppSession>>>,
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
