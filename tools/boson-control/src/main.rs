use std::{
    collections::BTreeMap,
    fmt::Write,
    future::Future,
    net::SocketAddr,
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use boson_control::{terraform_output, Instance, TerraformOutputRegion};
use hdrhistogram::Histogram;
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
    let output = terraform_output().await?;
    let regions = output.regions;
    match item.as_deref() {
        Some("test-mutex") => {
            mutex_session(
                client.clone(),
                &regions,
                RequestMode::All,
                Variant::NitroEnclaves,
                12,
            )
            .await?;
            Ok(())
        }
        Some("test-cops") => {
            cops_session(client.clone(), &regions, Variant::Replicated, 8000, 0.03, 5).await?;
            Ok(())
        }
        Some("mutex") => {
            for variant in [
                // Variant::Untrusted,
                // Variant::Replicated,
                Variant::Quorum,
                // Variant::NitroEnclaves,
            ] {
                for n in match variant {
                    Variant::Replicated => 1..=10,
                    Variant::NitroEnclaves => 1..=13,
                    // _ => 1..=16,
                    _ => 8..=16,
                } {
                    mutex_session(client.clone(), &regions, RequestMode::All, variant, n).await?
                }
            }
            for variant in [
                Variant::Untrusted,
                Variant::Replicated,
                Variant::Quorum,
                Variant::NitroEnclaves,
            ] {
                for n in 1..=16 {
                    mutex_session(client.clone(), &regions, RequestMode::One, variant, n).await?
                }
            }
            Ok(())
        }
        Some("cops") => {
            // TODO
            Ok(())
        }
        Some("quorum") => {
            let mut microbench = output.microbench;
            bench_quorum_session(client, microbench.remove(0), output.microbench_quorum).await
        }
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
    regions: &BTreeMap<String, TerraformOutputRegion>,
    mode: RequestMode,
    variant: Variant,
    num_region_processor: usize,
) -> anyhow::Result<()> {
    let mut addrs = Vec::new();
    let mut urls = Vec::new();
    let mut clock_addrs = Vec::new();
    let mut clock_urls = Vec::new();
    for region in regions.values() {
        anyhow::ensure!(region.quorum.len() >= 2);
        anyhow::ensure!(!region.mutex.is_empty());
        addrs.extend(
            region
                .mutex
                .iter()
                .take(num_region_processor)
                .map(|instance| SocketAddr::from((instance.public_ip, 4000))),
        );
        urls.extend(
            region
                .mutex
                .iter()
                .take(num_region_processor)
                .map(Instance::url),
        );
        clock_addrs.extend(
            region
                .quorum
                .iter()
                .map(|instance| SocketAddr::from((instance.public_ip, 5000))),
        );
        clock_urls.extend(region.quorum.iter().map(Instance::url))
    }

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
                // a stably selected instance to send Request
                // with current scripts this is always select a processor in af-south, unfortunately
                // leads to not ideal results (to both our and compared systems)
                let url = urls[0].clone();
                println!("{url}");
                while_ok(
                    &mut watchdog_sessions,
                    mutex_request_session(client.clone(), url, at, out.clone()),
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
        // i == 0 for
        // * warm up network stack
        //   * connect backoff slow things down
        //   * connect backoff destroys synchronization barrier which may speed things up = =
        // * map based clocks has smaller data size which may get abnormally faster
        if i > 0 {
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
    regions: &BTreeMap<String, TerraformOutputRegion>,
    variant: Variant,
    num_concurrent: usize,
    put_ratio: f64,
    num_region: usize,
) -> anyhow::Result<()> {
    let num_region_client = 1; //

    let mut addrs = Vec::new();
    let mut clock_addrs = Vec::new();
    let mut urls = Vec::new();
    let mut clock_urls = Vec::new();
    for region in regions.values().take(num_region) {
        anyhow::ensure!(region.cops.len() == 1);
        anyhow::ensure!(region.cops_client.len() == num_region_client);
        anyhow::ensure!(region.quorum.len() >= 2);
        addrs.push(SocketAddr::from((region.cops[0].public_ip, 4000)));
        urls.push(region.cops[0].url());
        clock_addrs.extend(
            region
                .quorum
                .iter()
                .map(|instance| SocketAddr::from((instance.public_ip, 5000))),
        );
        clock_urls.extend(region.quorum.iter().map(Instance::url))
    }

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
            num_faulty: (addrs.len() - 1) / 3,
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
    let record_count_per_replica = record_count / regions.len();
    let out = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));
    for (i, region) in regions.values().take(num_region).enumerate() {
        let addrs = match variant {
            Variant::Replicated => addrs.clone(),
            _ => {
                let server_instance = &region.cops[0];
                vec![SocketAddr::from((server_instance.public_ip, 4000))]
            }
        };
        let instance = &region.cops_client[0];
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
            instance.url(),
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
    let histogram = Arc::into_inner(out).unwrap().into_inner()?;
    let throughput = histogram.len() as f32 / 10.;
    let latency = Duration::from_nanos(histogram.value_at_quantile(0.999));
    println!("{throughput} {latency:?}");
    let path = Path::new("tools/boson-control/notebooks/cops");
    create_dir_all(path).await?;
    write(
        path.join(format!(
            "{}.txt",
            SystemTime::UNIX_EPOCH.elapsed()?.as_secs()
        )),
        format!(
            "{variant:?},{},{num_concurrent},{put_ratio},{throughput},{}",
            regions
                .keys()
                .take(num_region)
                .cloned()
                .collect::<Vec<_>>()
                .join("+"),
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
    out: Arc<Mutex<Histogram<u64>>>,
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
            .json::<Option<boson_control_messages::CopsClientOk>>()
            .await?;
        if let Some(boson_control_messages::CopsClientOk(histogram)) = result {
            let mut lines = vec![url];
            // for v in histogram.iter_quantiles(1) {
            //     lines.push(format!(
            //         "{:7.3}% {:?} ({} samples)",
            //         v.percentile(),
            //         Duration::from_nanos(v.value_iterated_to()),
            //         v.count_at_value()
            //     ))
            // }
            for q in [0.5, 0.99, 0.999] {
                lines.push(format!(
                    "{q:<5}th {:?}",
                    Duration::from_nanos(histogram.value_at_quantile(q))
                ))
            }
            println!("{}", lines.join("\n"));
            out.lock()
                .map_err(|err| anyhow::format_err!("{err}"))?
                .add(histogram)?;
            break Ok(());
        }
    }
}

async fn bench_quorum_session(
    client: reqwest::Client,
    instance: Instance,
    clock_instances: Vec<Instance>,
) -> anyhow::Result<()> {
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
