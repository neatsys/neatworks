use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    time::{Duration, SystemTime},
};

use tokio::{task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()?;
    let item = std::env::args().nth(1);
    match item.as_deref() {
        Some("mutex") => mutex_session(client).await,
        Some("cops") => cops_session(client).await,
        _ => Ok(()),
    }
}

async fn while_ok<T>(
    watchdog_sessions: &mut JoinSet<anyhow::Result<()>>,
    task: impl Future<Output = T>,
) -> anyhow::Result<T> {
    tokio::select! {
        result = task => return Ok(result),
        Some(result) = watchdog_sessions.join_next() => result??,
    }
    anyhow::bail!("unreachable")
}

async fn mutex_session(client: reqwest::Client) -> anyhow::Result<()> {
    let urls = (0..2)
        .map(|i| format!("http://127.0.0.{}:3000", i + 1))
        .collect::<Vec<_>>();
    let clock_urls = (0..2)
        .map(|i| format!("http://127.0.1.{}:3000", i + 1))
        .collect::<Vec<_>>();

    let addrs = (0..2)
        .map(|i| SocketAddr::from(([127, 0, 0, i + 1], 4000)))
        .collect::<Vec<_>>();
    let clock_addrs = (0..2)
        .map(|i| SocketAddr::from(([127, 0, 0, i + 1], 5000)))
        .collect::<Vec<_>>();

    let mut watchdog_sessions = JoinSet::new();
    let quorum = boson_control_messages::Quorum {
        addrs: clock_addrs,
        num_faulty: 1,
    };
    for (i, url) in clock_urls.iter().enumerate() {
        let config = boson_control_messages::QuorumServer {
            quorum: quorum.clone(),
            index: i,
        };
        watchdog_sessions.spawn(start_quorum_session(client.clone(), url.clone(), config));
    }
    while_ok(&mut watchdog_sessions, sleep(Duration::from_millis(5000))).await?;
    use boson_control_messages::Variant::*;
    // let variant = Quorum(quorum);
    // let variant = Untrusted;
    let variant = Replicated(boson_control_messages::Replicated { num_faulty: 0 });
    for (index, url) in urls.iter().enumerate() {
        let config = boson_control_messages::Mutex {
            addrs: addrs.clone(),
            id: index as _,
            variant: variant.clone(),
        };
        watchdog_sessions.spawn(mutex_start_session(client.clone(), url.clone(), config));
    }
    for _ in 0..10 {
        let at = SystemTime::now() + Duration::from_millis(2000);
        println!("Next request scheduled at {at:?}");
        while_ok(
            &mut watchdog_sessions,
            mutex_request_session(client.clone(), urls[0].clone(), at),
        )
        .await??
    }
    watchdog_sessions.shutdown().await;
    let mut stop_sessions = JoinSet::new();
    for url in urls {
        stop_sessions.spawn(mutex_stop_session(client.clone(), url));
    }
    for url in clock_urls {
        stop_sessions.spawn(stop_quorum_session(client.clone(), url));
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

async fn mutex_request_session(
    client: reqwest::Client,
    url: String,
    at: SystemTime,
) -> anyhow::Result<()> {
    let latency = client
        .post(format!("{url}/mutex/request"))
        .json(&at)
        .timeout(Duration::from_millis(5000))
        .send()
        .await?
        .error_for_status()?
        .json::<Duration>()
        .await?;
    println!("{latency:?}");
    Ok(())
}

async fn cops_session(client: reqwest::Client) -> anyhow::Result<()> {
    let urls = (0..2)
        .map(|i| format!("http://127.0.0.{}:3000", i + 1))
        .collect::<Vec<_>>();
    let client_urls = (0..2)
        .map(|i| format!("http://127.0.0.{}:3000", i + 101))
        .collect::<Vec<_>>();
    let clock_urls = (0..2)
        .map(|i| format!("http://127.0.1.{}:3000", i + 1))
        .collect::<Vec<_>>();
    let addrs = (0..2)
        .map(|i| SocketAddr::from(([127, 0, 0, i + 1], 4000)))
        .collect::<Vec<_>>();
    let clock_addrs = (0..2)
        .map(|i| SocketAddr::from(([127, 0, 0, i + 1], 5000)))
        .collect::<Vec<_>>();
    let client_ips = (0..2)
        .map(|_| IpAddr::from([127, 0, 0, 101]))
        .collect::<Vec<_>>();
    let mut watchdog_sessions = JoinSet::new();
    println!("Start clock services");
    let quorum = boson_control_messages::Quorum {
        addrs: clock_addrs.clone(),
        num_faulty: 1,
    };
    for (i, url) in clock_urls.iter().enumerate() {
        let config = boson_control_messages::QuorumServer {
            quorum: quorum.clone(),
            index: i,
        };
        watchdog_sessions.spawn(start_quorum_session(client.clone(), url.clone(), config));
    }
    use boson_control_messages::Variant::*;
    // let variant = Replicated(boson_control_messages::CopsReplicated { num_faulty: 0 });
    // let variant = Untrusted;
    let variant = Quorum(quorum);
    println!("Start servers");
    for (i, url) in urls.iter().enumerate() {
        let config = boson_control_messages::CopsServer {
            addrs: addrs.clone(),
            id: i as _,
            record_count: 1000,
            variant: variant.clone(),
        };
        watchdog_sessions.spawn(cops_start_server_session(
            client.clone(),
            url.clone(),
            config,
        ));
    }
    while_ok(&mut watchdog_sessions, sleep(Duration::from_millis(5000))).await?;
    println!("Start clients");
    let mut client_sessions = JoinSet::new();
    for (i, url) in client_urls.into_iter().enumerate() {
        let config = boson_control_messages::CopsClient {
            addrs: addrs.clone(),
            ip: client_ips[i],
            index: i,
            num_concurrent: 10,
            num_concurrent_put: 1,
            record_count: 1000,
            put_range: 500 * i..500 * (i + 1),
            variant: variant.clone(),
        };
        client_sessions.spawn(cops_client_session(client.clone(), url, config));
    }
    while_ok(&mut watchdog_sessions, async {
        while let Some(result) = client_sessions.join_next().await {
            result??
        }
        anyhow::Ok(())
    })
    .await??;
    println!("Shutdown");
    watchdog_sessions.shutdown().await;
    let mut stop_sessions = JoinSet::new();
    for url in urls {
        stop_sessions.spawn(cops_stop_server_session(client.clone(), url));
    }
    for url in clock_urls {
        stop_sessions.spawn(stop_quorum_session(client.clone(), url));
    }
    while let Some(result) = stop_sessions.join_next().await {
        result??
    }
    Ok(())
}

async fn cops_start_server_session(
    client: reqwest::Client,
    url: String,
    config: boson_control_messages::CopsServer,
) -> anyhow::Result<()> {
    client
        .post(format!("{url}/cops/start-server"))
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

async fn cops_stop_server_session(client: reqwest::Client, url: String) -> anyhow::Result<()> {
    client
        .post(format!("{url}/cops/stop-server"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn cops_client_session(
    client: reqwest::Client,
    url: String,
    config: boson_control_messages::CopsClient,
) -> anyhow::Result<()> {
    client
        .post(format!("{url}/cops/start-client"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?;
    loop {
        sleep(Duration::from_millis(1000)).await;
        let results = client
            .post(format!("{url}/cops/poll-results"))
            .send()
            .await?
            .error_for_status()?
            .json::<Option<Vec<(f32, Duration)>>>()
            .await?;
        if let Some(results) = results {
            println!("{results:?}");
            break Ok(());
        }
    }
}

async fn start_quorum_session(
    client: reqwest::Client,
    url: String,
    config: boson_control_messages::QuorumServer,
) -> anyhow::Result<()> {
    client
        .post(format!("{url}/start-quorum"))
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

async fn stop_quorum_session(client: reqwest::Client, url: String) -> anyhow::Result<()> {
    client
        .post(format!("{url}/stop-quorum"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

// cSpell:words reqwest
