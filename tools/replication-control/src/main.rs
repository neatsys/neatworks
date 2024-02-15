use std::{net::SocketAddr, time::Duration};

use replication_control_messages::{BenchmarkResult, ClientConfig, Protocol, ReplicaConfig};
use tokio::{task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    benchmark_session(control_client, Protocol::Unreplicated).await
    // benchmark_session(control_client, Protocol::Pbft).await
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

async fn benchmark_session(
    control_client: reqwest::Client,
    protocol: Protocol,
) -> anyhow::Result<()> {
    let replica_urls = [
        "http://127.0.0.1:3000",
        "http://127.0.0.2:3000",
        "http://127.0.0.3:3000",
        "http://127.0.0.4:3000",
    ];
    let replica_addrs = [
        SocketAddr::from(([127, 0, 0, 1], 3001)),
        SocketAddr::from(([127, 0, 0, 2], 3001)),
        SocketAddr::from(([127, 0, 0, 3], 3001)),
        SocketAddr::from(([127, 0, 0, 4], 3001)),
    ];
    let num_replica = match protocol {
        Protocol::Unreplicated => 1,
        _ => 4,
    };
    let num_faulty = match protocol {
        Protocol::Unreplicated => 0,
        _ => 1,
    };

    let mut watchdog_sessions = JoinSet::new();
    println!("Start replica(s)");
    for (replica_id, replica_url) in replica_urls.into_iter().enumerate() {
        let config = ReplicaConfig {
            replica_id: replica_id as _,
            replica_addrs: replica_addrs.into(),
            protocol,
            num_replica,
            num_faulty,
        };
        control_client
            .post(format!("{replica_url}/start-replica"))
            .json(&config)
            .send()
            .await?
            .error_for_status()?;
        watchdog_sessions.spawn(watchdog_session(control_client.clone(), replica_url.into()));
        if matches!(protocol, Protocol::Unreplicated) {
            break;
        }
    }
    tokio::select! {
        _ = sleep(Duration::from_millis(1200)) => {}
        result = watchdog_sessions.join_next() => result.unwrap()??,
    }
    println!("Start client(s)");
    let client_url = "http://127.0.0.101:3000";
    let config = ClientConfig {
        protocol,
        replica_addrs: replica_addrs.into(),
        num_replica,
        num_faulty,
    };
    control_client
        .post(format!("{client_url}/start-client"))
        .json(&Protocol::Unreplicated)
        .json(&config)
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
    for replica_url in replica_urls {
        control_client
            .post(format!("{replica_url}/stop-replica"))
            .send()
            .await?
            .error_for_status()?;
        if matches!(protocol, Protocol::Unreplicated) {
            break;
        }
    }
    println!("{result:?}");
    Ok(())
}
