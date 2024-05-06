use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    future::Future,
    net::SocketAddr,
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use boson_control::{terraform_output, Instance};
use tokio::{
    fs::{create_dir_all, write},
    io::AsyncWriteExt,
    net::TcpStream,
    process::Command,
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum RequestMode {
    One,
    All,
}

#[derive(Debug, Clone, Copy)]
pub enum Variant {
    Untrusted,
    Replicated,
    Quorum,
    NitroEnclaves,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .build()?;
    let item = std::env::args().nth(1);
    match item.as_deref() {
        Some("test-mutex") => {
            let instances = terraform_output("mutex_instances").await?;
            let clock_instances = terraform_output("quorum_instances").await?;
            mutex_session(
                client.clone(),
                instances,
                clock_instances,
                RequestMode::All,
                Variant::Replicated,
                10,
            )
            .await?;
            Ok(())
        }
        Some("test-cops") => {
            let client_instances = terraform_output("cops_client_instances").await?;
            let instances = terraform_output("cops_instances").await?;
            let clock_instances = terraform_output("quorum_instances").await?;
            cops_session(
                client.clone(),
                client_instances.clone(),
                instances.clone(),
                clock_instances.clone(),
                Variant::NitroEnclaves,
                80,
                0.01,
            )
            .await?;
            Ok(())
        }
        Some("mutex") => {
            let instances = terraform_output("mutex_instances").await?;
            let clock_instances = terraform_output("quorum_instances").await?;
            for variant in [
                Variant::Untrusted,
                Variant::Replicated,
                Variant::Quorum,
                Variant::NitroEnclaves,
            ] {
                for n in 1..=20 {
                    mutex_session(
                        client.clone(),
                        instances.clone(),
                        clock_instances.clone(),
                        RequestMode::One,
                        variant,
                        n,
                    )
                    .await?
                }
            }
            for variant in [
                Variant::Quorum,
                Variant::NitroEnclaves,
                Variant::Replicated,
                Variant::Untrusted,
            ] {
                for n in match variant {
                    Variant::Replicated => 1..=10,
                    Variant::NitroEnclaves => 1..=12,
                    _ => 1..=16,
                }
                .rev()
                {
                    mutex_session(
                        client.clone(),
                        instances.clone(),
                        clock_instances.clone(),
                        RequestMode::All,
                        variant,
                        n,
                    )
                    .await?
                }
            }
            Ok(())
        }
        Some("cops") => {
            let client_instances = terraform_output("cops_client_instances").await?;
            let instances = terraform_output("cops_instances").await?;
            let clock_instances = terraform_output("quorum_instances").await?;
            cops_session(
                client.clone(),
                client_instances.clone(),
                instances.clone(),
                clock_instances.clone(),
                Variant::Untrusted,
                1,
                0.1,
            )
            .await?;
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

async fn watchdog_session(
    client: reqwest::Client,
    url: String,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut num_missing_ok = 0;
    loop {
        if let Ok(()) = timeout(Duration::from_millis(1000), cancel.cancelled()).await {
            return Ok(());
        }
        if let Err(err) = async {
            client
                .get(format!("{url}/ok"))
                .send()
                .await?
                .error_for_status()?;
            anyhow::Ok(())
        }
        .await
        {
            if let Some(err_ref) = err.downcast_ref::<reqwest::Error>() {
                if err_ref.is_status() {
                    break Err(err);
                }
            }
            if num_missing_ok < 3 {
                num_missing_ok += 1;
                println!("! missing /ok {num_missing_ok}/3 from {url}");
                continue;
            }
            break Err(err);
        }
        num_missing_ok = 0
    }
}

async fn mutex_session(
    client: reqwest::Client,
    instances: Vec<Instance>,
    clock_instances: Vec<Instance>,
    mode: RequestMode,
    variant: Variant,
    num_region_processor: usize,
) -> anyhow::Result<()> {
    // this one will always be taken below
    let one_instance = instances[0].clone();
    let mut region_instances = BTreeMap::<_, Vec<_>>::new();
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
    let cancel = CancellationToken::new();
    for (i, url) in clock_urls.iter().enumerate() {
        let config = boson_control_messages::QuorumServer {
            quorum: quorum.clone(),
            index: i,
        };
        watchdog_sessions.spawn(quorum_watchdog_session(
            client.clone(),
            url.clone(),
            config,
            cancel.clone(),
        ));
    }
    while_ok(&mut watchdog_sessions, sleep(Duration::from_millis(5000))).await?;
    use boson_control_messages::Variant::*;
    let variant_config = match variant {
        Variant::Untrusted => Untrusted,
        Variant::Replicated => Replicated(boson_control_messages::Replicated {
            num_faulty: num_region_processor - 1,
        }),
        Variant::Quorum => Quorum(quorum),
        Variant::NitroEnclaves => NitroEnclaves,
    };
    for (index, url) in urls.iter().enumerate() {
        let config = boson_control_messages::Mutex {
            addrs: addrs.clone(),
            id: index as _,
            num_faulty: num_region_processor - 1,
            variant: variant_config.clone(),
        };
        watchdog_sessions.spawn(mutex_watchdog_session(
            client.clone(),
            url.clone(),
            config,
            cancel.clone(),
        ));
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
        // i == 0 for 1. warm up network stack 2. map based clocks has smaller data size which
        // causes abnormal result
        // i == 1 just not work well when Variant == Quorum and 16 region processors, not sure why
        if i >= 2 {
            for duration in Arc::into_inner(out).unwrap().into_inner()? {
                writeln!(
                    &mut lines,
                    "{mode:?},{variant:?},{},{}",
                    urls.len(),
                    duration.as_secs_f32()
                )?
            }
        }
        // sleep(Duration::from_millis(10000)).await
    }
    cancel.cancel();
    while let Some(result) = watchdog_sessions.join_next().await {
        result??
    }
    // print!("{lines}");
    let path = Path::new("tools/boson-control/notebooks/mutex");
    create_dir_all(path).await?;
    write(
        path.join(format!(
            "{}.txt",
            SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
        )),
        lines,
    )
    .await?;
    Ok(())
}

async fn mutex_watchdog_session(
    client: reqwest::Client,
    url: String,
    config: boson_control_messages::Mutex,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    client
        .post(format!("{url}/mutex/start"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?;
    watchdog_session(client.clone(), url.clone(), cancel).await?;
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
        .timeout(Duration::from_millis(65000))
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
    client_instances: Vec<Instance>,
    instances: Vec<Instance>,
    clock_instances: Vec<Instance>,
    variant: Variant,
    num_concurrent: usize,
    put_ratio: f64,
) -> anyhow::Result<()> {
    let num_region = instances
        .iter()
        .map(|instance| instance.region())
        .collect::<BTreeSet<_>>()
        .len();
    let num_region_client = 1; // TODO

    // anyhow::ensure!(clock_instances.len() >= num_region * 2);
    anyhow::ensure!(instances.len() == num_region);
    anyhow::ensure!(client_instances.len() == num_region * num_region_client);

    let urls = instances
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

    let mut watchdog_sessions = JoinSet::new();
    println!("Start clock services");
    let quorum = boson_control_messages::Quorum {
        addrs: clock_addrs.clone(),
        num_faulty: 1,
    };
    let cancel = CancellationToken::new();
    for (i, url) in clock_urls.iter().enumerate() {
        let config = boson_control_messages::QuorumServer {
            quorum: quorum.clone(),
            index: i,
        };
        watchdog_sessions.spawn(quorum_watchdog_session(
            client.clone(),
            url.clone(),
            config,
            cancel.clone(),
        ));
    }
    use boson_control_messages::Variant::*;
    let variant_config = match variant {
        Variant::Untrusted => Untrusted,
        Variant::Replicated => Replicated(boson_control_messages::Replicated {
            num_faulty: (instances.len() - 1) / 3,
        }),
        Variant::Quorum => Quorum(quorum),
        Variant::NitroEnclaves => NitroEnclaves,
    };
    println!("Start servers");
    let record_count = 500;
    for (i, url) in urls.iter().enumerate() {
        println!("{url}");
        let config = boson_control_messages::CopsServer {
            addrs: addrs.clone(),
            id: i as _,
            record_count,
            variant: variant_config.clone(),
        };
        watchdog_sessions.spawn(cops_server_watchdog_session(
            client.clone(),
            url.clone(),
            config,
            cancel.clone(),
        ));
    }
    while_ok(&mut watchdog_sessions, sleep(Duration::from_millis(5000))).await?;
    println!("Start clients");
    let mut client_sessions = JoinSet::new();
    let record_count_per_replica = record_count / instances.len();
    let out = Arc::new(Mutex::new(Vec::new()));
    for (i, instance) in client_instances.iter().enumerate() {
        let addrs = match variant {
            Variant::Replicated => addrs.clone(),
            _ => {
                let server_instance = instances
                    .iter()
                    .find(|server_instance| server_instance.region() == instance.region())
                    .ok_or(anyhow::format_err!(
                        "no server in region {:?}",
                        instance.region()
                    ))?;
                vec![SocketAddr::from((server_instance.public_ip, 4000))]
            }
        };
        let index = i / num_region_client;
        let config = boson_control_messages::CopsClient {
            addrs,
            ip: instance.public_ip,
            num_concurrent,
            put_ratio,
            record_count,
            put_range: record_count_per_replica * index..record_count_per_replica * (index + 1),
            variant: variant_config.clone(),
        };
        client_sessions.spawn(cops_client_session(
            client.clone(),
            format!("http://{}:3000", instance.public_dns),
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
    cancel.cancel();
    while let Some(result) = watchdog_sessions.join_next().await {
        result??
    }
    let results = Arc::into_inner(out).unwrap().into_inner()?;
    let throughput = results
        .iter()
        .map(|(throughput, _)| throughput)
        .sum::<f32>();
    let latency = results
        .iter()
        .map(|(throughput, latency)| latency.mul_f32(*throughput))
        .sum::<Duration>()
        .div_f32(throughput);
    println!("{throughput} {latency:?}");
    let path = Path::new("tools/boson-control/notebooks/cops");
    create_dir_all(path).await?;
    write(
        path.join(format!(
            "{}.txt",
            SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
        )),
        format!(
            "{variant:?},{num_concurrent},{put_ratio},{throughput},{}",
            latency.as_secs_f32()
        ),
    )
    .await?;
    Ok(())
}

async fn cops_server_watchdog_session(
    client: reqwest::Client,
    url: String,
    config: boson_control_messages::CopsServer,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    client
        .post(format!("{url}/cops/start-server"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?;
    watchdog_session(client.clone(), url.clone(), cancel).await?;
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
        let result = client
            .post(format!("{url}/cops/poll-results"))
            .send()
            .await?
            .error_for_status()?
            .json::<Option<(f32, Duration, Duration)>>()
            .await?;
        if let Some(result) = result {
            println!("{result:?} {url}");
            out.lock()
                .map_err(|err| anyhow::format_err!("{err}"))?
                .push((result.0, result.1));
            break Ok(());
        }
    }
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
        let cancel = CancellationToken::new();
        for (i, url) in clock_urls.iter().enumerate() {
            let config = boson_control_messages::QuorumServer {
                quorum: quorum.clone(),
                index: i,
            };
            watchdog_sessions.spawn(quorum_watchdog_session(
                client.clone(),
                url.clone(),
                config,
                cancel.clone(),
            ));
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

        cancel.cancel();
        while let Some(result) = watchdog_sessions.join_next().await {
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

async fn quorum_watchdog_session(
    client: reqwest::Client,
    url: String,
    config: boson_control_messages::QuorumServer,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    client
        .post(format!("{url}/start-quorum"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?;
    watchdog_session(client.clone(), url.clone(), cancel).await?;
    client
        .post(format!("{url}/stop-quorum"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

// cSpell:words reqwest
