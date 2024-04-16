use std::time::Duration;

use tokio::{task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()?;
    let mut watchdog_sessions = JoinSet::new();
    for index in 0..2 {
        watchdog_sessions.spawn(start_mutex_session(
            client.clone(),
            format!("http://127.0.0.{}:3000", index + 1),
        ));
    }
    tokio::select! {
        () = sleep(Duration::from_millis(3000)) => {}
        Some(result) = watchdog_sessions.join_next() => result??,
    }
    let mut stop_sessions = JoinSet::new();
    for index in 0..2 {
        stop_sessions.spawn(stop_mutex_session(
            client.clone(),
            format!("http://127.0.0.{}:3000", index + 1),
        ));
    }
    while let Some(result) = stop_sessions.join_next().await {
        result??
    }
    Ok(())
}

async fn start_mutex_session(client: reqwest::Client, url: String) -> anyhow::Result<()> {
    client
        .post(format!("{url}/mutex/start"))
        .send()
        .await?
        .error_for_status()?;
    loop {
        sleep(Duration::from_millis(1000)).await;
        client
            .get(format!("{url}/ok"))
            .send()
            .await?
            .error_for_status()?;
    }
}

async fn stop_mutex_session(client: reqwest::Client, url: String) -> anyhow::Result<()> {
    client
        .post(format!("{url}/mutex/stop"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
