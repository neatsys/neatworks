use std::time::Duration;

use replication_control_messages::BenchmarkResult;
use tokio::task::JoinSet;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    benchmark_session(control_client).await
}

async fn watchdog_session(control_client: reqwest::Client, url: String) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        control_client
            .get(format!("{url}/ok"))
            .send()
            .await?
            .error_for_status()?;
    }
}

async fn result_session(
    control_client: reqwest::Client,
    url: String,
) -> anyhow::Result<BenchmarkResult> {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(result) = control_client
            .get(format!("{url}/benchmark-result"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
        {
            return Ok(result);
        }
    }
}

async fn benchmark_session(control_client: reqwest::Client) -> anyhow::Result<()> {
    let mut watchdog_sessions = JoinSet::new();
    let replica_url = "http://127.0.0.1:3000";
    control_client
        .post(format!("{replica_url}/start-replica"))
        .send()
        .await?
        .error_for_status()?;
    watchdog_sessions.spawn(watchdog_session(control_client.clone(), replica_url.into()));
    let client_url = "http://127.0.0.101:3000";
    control_client
        .post(format!("{client_url}/start-client"))
        .send()
        .await?
        .error_for_status()?;
    watchdog_sessions.spawn(watchdog_session(control_client.clone(), client_url.into()));
    let result = 'select: {
        tokio::select! {
            result = result_session(control_client.clone(), client_url.into()) => break 'select result?,
            result = watchdog_sessions.join_next() => result.unwrap()??,
        }
        return Err(anyhow::anyhow!("unexpected shutdown"));
    };
    control_client
        .post(format!("{replica_url}/stop-replica"))
        .send()
        .await?
        .error_for_status()?;
    println!("{result:?}");
    Ok(())
}
