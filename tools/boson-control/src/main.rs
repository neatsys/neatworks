use std::{net::SocketAddr, time::Duration};

use tokio::{task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()?;

    let urls = (0..2)
        .map(|i| format!("http://127.0.0.{}:3000", i + 1))
        .collect::<Vec<_>>();
    let addrs = (0..2)
        .map(|i| SocketAddr::from(([127, 0, 0, i + 1], 4000)))
        .collect::<Vec<_>>();
    let client_addrs = (0..2)
        .map(|i| SocketAddr::from(([127, 0, 0, i + 1], 5000)))
        .collect::<Vec<_>>();

    let mut watchdog_sessions = JoinSet::new();
    for (index, url) in urls.iter().enumerate() {
        let config =
            // boson_control_messages::Mutex::Untrusted(boson_control_messages::MutexUntrusted {
            //     addrs: addrs.clone(),
            //     id: index as _,
            // });
            boson_control_messages::Mutex::Replicated(boson_control_messages::MutexReplicated {
                addrs: addrs.clone(),
                client_addrs: client_addrs.clone(),
                id: index as _,
                num_faulty: 0,
            });

        watchdog_sessions.spawn(mutex_start_session(client.clone(), url.clone(), config));
    }
    for _ in 0..10 {
        sleep(Duration::from_millis(1000)).await;
        tokio::select! {
            result = mutex_request_session(client.clone(), urls[0].clone()) => result?,
            Some(result) = watchdog_sessions.join_next() => result??,
        }
    }
    let mut stop_sessions = JoinSet::new();
    for url in urls {
        stop_sessions.spawn(mutex_stop_session(client.clone(), url));
    }
    while let Some(result) = stop_sessions.join_next().await {
        result??
    }
    Ok(())
}

async fn mutex_start_session(
    client: reqwest::Client,
    url: String,
    config: boson_control_messages::Mutex,
) -> anyhow::Result<()> {
    client
        .post(format!("{url}/mutex/start"))
        .json(&config)
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

async fn mutex_stop_session(client: reqwest::Client, url: String) -> anyhow::Result<()> {
    client
        .post(format!("{url}/mutex/stop"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn mutex_request_session(client: reqwest::Client, url: String) -> anyhow::Result<()> {
    let latency = client
        .post(format!("{url}/mutex/request"))
        .send()
        .await?
        .error_for_status()?
        .json::<Duration>()
        .await?;
    println!("{latency:?}");
    Ok(())
}
