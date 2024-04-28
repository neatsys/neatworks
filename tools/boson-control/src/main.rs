use std::{
    collections::HashMap,
    fmt::Write,
    future::Future,
    net::{IpAddr, SocketAddr},
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use boson_control::{terraform_output, TerraformOutputInstance};
use tokio::{
    fs::{create_dir_all, write},
    io::AsyncWriteExt,
    net::TcpStream,
    process::Command,
    task::JoinSet,
    time::sleep,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()?;
    let item = std::env::args().nth(1);
    match item.as_deref() {
        Some("test-mutex") => {
            let instances = (0..4)
                .map(|i| TerraformOutputInstance {
                    public_ip: IpAddr::from([127, 0, 0, i + 1]),
                    private_ip: IpAddr::from([127, 0, 0, i + 1]),
                    public_dns: format!("127.0.0.{}", i + 1),
                })
                .collect();
            let clock_instances = (0..2)
                .map(|i| TerraformOutputInstance {
                    public_ip: IpAddr::from([127, 0, 0, i + 101]),
                    private_ip: IpAddr::from([127, 0, 0, i + 101]),
                    public_dns: format!("127.0.0.{}", i + 101),
                })
                .collect();
            mutex_session(
                client.clone(),
                instances,
                clock_instances,
                RequestMode::All,
                Variant::Replicated,
                4,
            )
            .await?;
            Ok(())
        }
        Some("mutex") => {
            let instances = terraform_output("mutex_instances").await?;
            let clock_instances = terraform_output("quorum_instances").await?;
            for n in 1..=20 {
                mutex_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    RequestMode::One,
                    Variant::Quorum,
                    n,
                )
                .await?
            }
            for n in 1..=20 {
                mutex_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    RequestMode::One,
                    Variant::Untrusted,
                    n,
                )
                .await?
            }
            for n in 1..=20 {
                mutex_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    RequestMode::One,
                    Variant::Replicated,
                    n,
                )
                .await?
            }
            for n in 1..=16 {
                mutex_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    RequestMode::All,
                    Variant::Quorum,
                    n,
                )
                .await?
            }
            for n in 1..=16 {
                mutex_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    RequestMode::All,
                    Variant::Untrusted,
                    n,
                )
                .await?
            }
            Ok(())
        }
        Some("cops") => {
            let instances = terraform_output("cops_instances").await?;
            let clock_instances = terraform_output("quorum_instances").await?;
            cops_session(
                client.clone(),
                instances.clone(),
                clock_instances.clone(),
                Variant::Untrusted,
                1,
                1,
                5,
                2,
            )
            .await?;
            cops_session(
                client.clone(),
                instances.clone(),
                clock_instances.clone(),
                Variant::Untrusted,
                1,
                1,
                10,
                4,
            )
            .await?;
            for n in 1..=10 {
                cops_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    Variant::Untrusted,
                    1,
                    2,
                    n * 10,
                    n * 4,
                )
                .await?
            }

            cops_session(
                client.clone(),
                instances.clone(),
                clock_instances.clone(),
                Variant::Quorum,
                1,
                1,
                5,
                2,
            )
            .await?;
            cops_session(
                client.clone(),
                instances.clone(),
                clock_instances.clone(),
                Variant::Quorum,
                1,
                1,
                10,
                4,
            )
            .await?;
            for n in 1..=10 {
                cops_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    Variant::Quorum,
                    1,
                    2,
                    n * 10,
                    n * 4,
                )
                .await?
            }

            for n in [1, 2, 4, 8, 20, 40, 60, 80, 120, 160] {
                cops_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    Variant::Untrusted,
                    1,
                    2,
                    200,
                    n,
                )
                .await?
            }
            cops_session(
                client.clone(),
                instances.clone(),
                clock_instances.clone(),
                Variant::Untrusted,
                1,
                2,
                160,
                160,
            )
            .await?;

            for (n_put, n) in [
                (1, 200),
                (2, 200),
                (4, 200),
                (8, 200),
                (20, 200),
                (20, 100),
                (30, 100),
                (40, 100),
                (30, 50),
                (40, 50),
                (50, 50),
            ] {
                cops_session(
                    client.clone(),
                    instances.clone(),
                    clock_instances.clone(),
                    Variant::Quorum,
                    1,
                    2,
                    n,
                    n_put,
                )
                .await?
            }

            Ok(())
        }
        Some("cops-replicated") => {
            let instances = terraform_output("cops_instances").await?;
            for n in [1].into_iter().chain((2..=20).step_by(2)) {
                cops_replicated_session(client.clone(), instances.clone(), 5, 1, n * 10, n * 4)
                    .await?
            }
            for n in [1, 2, 4, 8, 20, 40, 60, 80, 120, 160, 200] {
                cops_replicated_session(client.clone(), instances.clone(), 5, 1, 200, n).await?
            }
            Ok(())
        }
        Some("quorum") => bench_quorum_session(client).await,
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

#[derive(Debug)]
pub enum RequestMode {
    One,
    All,
}

#[derive(Debug)]
pub enum Variant {
    Untrusted,
    Replicated,
    Quorum,
}

async fn mutex_session(
    client: reqwest::Client,
    instances: Vec<TerraformOutputInstance>,
    clock_instances: Vec<TerraformOutputInstance>,
    mode: RequestMode,
    variant: Variant,
    num_region_processor: usize,
) -> anyhow::Result<()> {
    // this one will always be taken below
    let one_instance = instances[0].clone();
    let mut region_instances = HashMap::<_, Vec<_>>::new();
    for instance in instances {
        region_instances
            .entry(instance.region())
            .or_default()
            .push(instance.clone())
    }
    let num_region = region_instances.len();

    // let mut clock_instances = Vec::new();
    let mut instances = Vec::new();
    for region_instances in region_instances.into_values() {
        // let mut region_instances = region_instances.into_iter();
        // clock_instances.extend((&mut region_instances).take(2));
        let region_instances = region_instances.into_iter();
        instances.extend(region_instances.take(num_region_processor))
    }
    anyhow::ensure!(clock_instances.len() >= num_region * 2);
    anyhow::ensure!(!instances.is_empty());

    let urls = instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();
    let one_url = format!("http://{}:3000", one_instance.public_dns);
    assert!(urls.contains(&one_url));
    let clock_urls = clock_instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();

    let addrs = instances
        .iter()
        .map(|instance| SocketAddr::from((instance.public_ip, 4000)))
        .collect::<Vec<_>>();
    let clock_addrs = clock_instances
        .iter()
        .map(|instance| SocketAddr::from((instance.public_ip, 5000)))
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
    let variant_config = match variant {
        Variant::Untrusted => Untrusted,
        Variant::Replicated => Replicated(boson_control_messages::Replicated {
            num_faulty: num_region_processor - 1,
        }),
        Variant::Quorum => Quorum(quorum),
    };
    for (index, url) in urls.iter().enumerate() {
        let config = boson_control_messages::Mutex {
            addrs: addrs.clone(),
            id: index as _,
            num_faulty: num_region_processor - 1,
            variant: variant_config.clone(),
        };
        watchdog_sessions.spawn(mutex_start_session(client.clone(), url.clone(), config));
    }
    let mut lines = String::new();
    for i in 0..10 {
        let at = SystemTime::now() + Duration::from_millis(2000);
        println!("Next request scheduled at {at:?}");
        let out = Arc::new(Mutex::new(Vec::new()));
        match mode {
            RequestMode::One => {
                while_ok(
                    &mut watchdog_sessions,
                    mutex_request_session(client.clone(), one_url.clone(), at, out.clone()),
                )
                .await??
            }
            RequestMode::All => {
                let mut sessions = JoinSet::new();
                for url in &urls {
                    sessions.spawn(mutex_request_session(
                        client.clone(),
                        url.clone(),
                        at,
                        out.clone(),
                    ));
                }
                while_ok(&mut watchdog_sessions, async move {
                    while let Some(result) = sessions.join_next().await {
                        result??
                    }
                    anyhow::Ok(())
                })
                .await??
            }
        }
        if i != 0 {
            for duration in Arc::into_inner(out).unwrap().into_inner()? {
                writeln!(
                    &mut lines,
                    "{mode:?},{variant:?},{},{}",
                    urls.len(),
                    duration.as_secs_f32()
                )?
            }
        }
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
    // print!("{lines}");
    // let path = Path::new("tools/boson-control/notebooks");
    // create_dir_all(path).await?;
    // write(
    //     path.join(format!(
    //         "mutex-{}.txt",
    //         SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
    //     )),
    //     lines,
    // )
    // .await?;
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
    out: Arc<Mutex<Vec<Duration>>>,
) -> anyhow::Result<()> {
    let latency = client
        .post(format!("{url}/mutex/request"))
        .json(&at)
        .timeout(Duration::from_millis(30000))
        .send()
        .await?
        .error_for_status()?
        .json::<Duration>()
        .await?;
    println!("{latency:?}");
    out.lock()
        .map_err(|err| anyhow::format_err!("{err}"))?
        .push(latency);
    Ok(())
}

async fn cops_session(
    client: reqwest::Client,
    instances: Vec<TerraformOutputInstance>,
    clock_instances: Vec<TerraformOutputInstance>,
    variant: Variant,
    num_region_replica: usize,
    num_replica_client: usize,
    num_concurrent: usize,
    num_concurrent_put: usize,
) -> anyhow::Result<()> {
    let mut region_instances = HashMap::<_, Vec<_>>::new();
    for instance in instances {
        region_instances
            .entry(instance.region())
            .or_default()
            .push(instance.clone())
    }
    let num_region = region_instances.len();
    let num_region_client = num_region_replica * num_replica_client;

    // let mut clock_instances = Vec::new();
    let mut instances = Vec::new();
    let mut client_instances = Vec::new();
    for region_instances in region_instances.into_values() {
        let mut region_instances = region_instances.into_iter();
        // clock_instances.extend((&mut region_instances).take(2));
        instances.extend((&mut region_instances).take(num_region_replica));
        client_instances.extend(region_instances.take(num_region_client));
    }
    anyhow::ensure!(clock_instances.len() >= num_region * 2);
    anyhow::ensure!(instances.len() == num_region * num_region_replica);
    anyhow::ensure!(client_instances.len() == num_region * num_region_client);

    let urls = instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();
    let client_urls = client_instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();
    let clock_urls = clock_instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();
    let addrs = instances
        .iter()
        .map(|instance| SocketAddr::from((instance.public_ip, 4000)))
        .collect::<Vec<_>>();
    let clock_addrs = clock_instances
        .iter()
        .map(|instance| SocketAddr::from((instance.public_ip, 5000)))
        .collect::<Vec<_>>();
    let client_ips = client_instances
        .iter()
        .map(|instance| instance.public_ip)
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
    let variant_config = match variant {
        Variant::Untrusted => Untrusted,
        Variant::Replicated => anyhow::bail!("unimplemented"),
        Variant::Quorum => Quorum(quorum),
    };
    println!("Start servers");
    let record_count = 1000;
    for (i, url) in urls.iter().enumerate() {
        println!("{url}");
        let config = boson_control_messages::CopsServer {
            addrs: addrs.clone(),
            id: i as _,
            record_count,
            variant: variant_config.clone(),
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
    let record_count_per_replica = record_count / urls.len();
    let out = Arc::new(Mutex::new(Vec::new()));
    for (i, url) in client_urls.into_iter().enumerate() {
        let index = i / num_replica_client;
        let config = boson_control_messages::CopsClient {
            addrs: addrs.clone(),
            ip: client_ips[i],
            index,
            num_concurrent,
            num_concurrent_put,
            record_count,
            put_range: record_count_per_replica * index..record_count_per_replica * (index + 1),
            variant: variant_config.clone(),
        };
        client_sessions.spawn(cops_client_session(
            client.clone(),
            url,
            config,
            out.clone(),
        ));
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
    let (throughputs, latencies) = Arc::into_inner(out)
        .unwrap()
        .into_inner()?
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let throughput = throughputs.into_iter().sum::<f32>();
    let lantecy = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    // println!(
    //     "{variant:?},{},{num_concurrent},{num_concurrent_put},{throughput},{}",
    //     num_region_replica,
    //     lantecy.as_secs_f32()
    // );
    let path = Path::new("tools/boson-control/notebooks");
    create_dir_all(path).await?;
    write(
        path.join(format!(
            "cops-{}.txt",
            SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
        )),
        format!(
            "{variant:?},{},{num_concurrent},{num_concurrent_put},{throughput},{}",
            num_region_replica,
            lantecy.as_secs_f32()
        ),
    )
    .await?;
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
    out: Arc<Mutex<Vec<(f32, Duration)>>>,
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
            out.lock()
                .map_err(|err| anyhow::format_err!("{err}"))?
                .extend(results);
            break Ok(());
        }
    }
}

async fn cops_replicated_session(
    client: reqwest::Client,
    instances: Vec<TerraformOutputInstance>,
    num_region_replica: usize,
    num_replica_client: usize,
    num_concurrent: usize,
    num_concurrent_put: usize,
) -> anyhow::Result<()> {
    let mut region_instances = HashMap::<_, Vec<_>>::new();
    for instance in instances {
        region_instances
            .entry(instance.region())
            .or_default()
            .push(instance.clone())
    }
    let num_region = region_instances.len();
    let num_region_client = num_region_replica * num_replica_client;

    // let mut clock_instances = Vec::new();
    let mut instances = Vec::new();
    let mut client_instances = Vec::new();
    for region_instances in region_instances.into_values() {
        let mut region_instances = region_instances.into_iter();
        // clock_instances.extend((&mut region_instances).take(2));
        instances.extend((&mut region_instances).take(num_region_replica));
        client_instances.extend(region_instances.take(num_region_client));
    }
    anyhow::ensure!(instances.len() == num_region * num_region_replica);
    anyhow::ensure!(client_instances.len() == num_region * num_region_client);

    let urls = instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();
    let client_urls = client_instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();
    let addrs = instances
        .iter()
        .map(|instance| SocketAddr::from((instance.public_ip, 4000)))
        .collect::<Vec<_>>();
    let client_ips = client_instances
        .iter()
        .map(|instance| instance.public_ip)
        .collect::<Vec<_>>();
    let mut watchdog_sessions = JoinSet::new();
    use boson_control_messages::Variant::*;
    let variant_config = Replicated(boson_control_messages::Replicated {
        num_faulty: (urls.len() - 1) / 3,
    });
    println!("Start servers");
    let record_count = 1000;
    for (i, url) in urls.iter().enumerate() {
        println!("{url}");
        let config = boson_control_messages::CopsServer {
            addrs: addrs.clone(),
            id: i as _,
            record_count,
            variant: variant_config.clone(),
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
    let record_count_per_replica = record_count / urls.len();
    let out = Arc::new(Mutex::new(Vec::new()));
    for (i, url) in client_urls.into_iter().enumerate() {
        let index = i / num_replica_client;
        let config = boson_control_messages::CopsClient {
            addrs: addrs.clone(),
            ip: client_ips[i],
            index,
            num_concurrent,
            num_concurrent_put,
            record_count,
            put_range: record_count_per_replica * index..record_count_per_replica * (index + 1),
            variant: variant_config.clone(),
        };
        client_sessions.spawn(cops_client_session(
            client.clone(),
            url,
            config,
            out.clone(),
        ));
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
    while let Some(result) = stop_sessions.join_next().await {
        result??
    }
    let (throughputs, latencies) = Arc::into_inner(out)
        .unwrap()
        .into_inner()?
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let throughput = throughputs.into_iter().sum::<f32>();
    let lantecy = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    // println!(
    //     "Replicated (local),{},{num_concurrent},{num_concurrent_put},{throughput},{}",
    //     num_region_replica,
    //     lantecy.as_secs_f32()
    // );
    let path = Path::new("tools/boson-control/notebooks");
    create_dir_all(path).await?;
    write(
        path.join(format!(
            "cops-{}.txt",
            SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
        )),
        format!(
            "Replicated (local),{},{num_concurrent},{num_concurrent_put},{throughput},{}",
            num_region_replica,
            lantecy.as_secs_f32()
        ),
    )
    .await?;
    Ok(())
}

async fn bench_quorum_session(client: reqwest::Client) -> anyhow::Result<()> {
    let instance = terraform_output("microbench_instances").await?.remove(0);
    let clock_instances = terraform_output("microbench_quorum_instances").await?;
    let clock_urls = clock_instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_dns))
        .collect::<Vec<_>>();
    let clock_addrs = clock_instances
        .iter()
        .map(|instance| SocketAddr::from((instance.public_ip, 5000)))
        .collect::<Vec<_>>();
    let mut lines = Vec::new();
    for num_faulty in 0..10 {
        let quorum = boson_control_messages::Quorum {
            addrs: clock_addrs.clone(),
            num_faulty,
        };

        println!("Start clock services");
        let mut watchdog_sessions = JoinSet::new();
        for (i, url) in clock_urls.iter().enumerate() {
            let config = boson_control_messages::QuorumServer {
                quorum: quorum.clone(),
                index: i,
            };
            watchdog_sessions.spawn(start_quorum_session(client.clone(), url.clone(), config));
        }

        while_ok(&mut watchdog_sessions, async {
            let command = Command::new("ssh")
                .arg(format!("ec2-user@{}", instance.public_dns))
                .arg(format!("./boson-bench-clock quorum {}", instance.public_ip))
                .stdout(Stdio::piped())
                .spawn()?;
            sleep(Duration::from_millis(1000)).await;
            let buf = serde_json::to_vec(&quorum)?;
            TcpStream::connect((&*instance.public_dns, 3000))
                .await?
                .write_all(&buf)
                .await?;
            let output = command.wait_with_output().await?;
            anyhow::ensure!(output.status.success());
            lines.extend(output.stdout);
            anyhow::Ok(())
        })
        .await??;

        watchdog_sessions.shutdown().await;
        let mut stop_sessions = JoinSet::new();
        for url in &clock_urls {
            stop_sessions.spawn(stop_quorum_session(client.clone(), url.clone()));
        }
        while let Some(result) = stop_sessions.join_next().await {
            result??
        }
    }
    write(
        format!(
            "tools/boson-control/notebooks/clock-quorum-{}.txt",
            SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
        ),
        lines,
    )
    .await?;
    Ok(())
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
