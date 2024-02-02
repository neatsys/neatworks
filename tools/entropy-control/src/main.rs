use std::{
    env::args,
    io::{SeekFrom::Start, Write as _},
    net::IpAddr,
    num::NonZeroUsize,
    ops::Range,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};

use entropy_control::{retain_instances, terraform_instances, TerraformOutputInstance};
use entropy_control_messages::{
    GetConfig, GetResult, PeerUrl, PutConfig, PutResult, StartPeersConfig,
};
use rand::{seq::SliceRandom, thread_rng, Rng};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::mpsc::unbounded_channel,
    task::JoinSet,
    time::{sleep, Instant},
};

const NUM_PEER_PER_IP: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let category = args().nth(1).unwrap_or_default();

    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let instances = terraform_instances().await?;
    // let instances = vec![instances[0].clone()];
    // let instances = vec![entropy_control::TerraformOutputInstance {
    //     public_ip: [127, 0, 0, 1].into(),
    //     private_ip: [127, 0, 0, 1].into(),
    //     public_dns: "localhost".into(),
    // }];

    if category.ends_with("latency") {
        let mut out = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open("entropy-5.txt")
            .await?;
        let mut lines = String::new();
        out.seek(Start(0)).await?;
        out.read_to_string(&mut lines).await?;
        let lines = lines.lines().map(ToString::to_string).collect::<Vec<_>>();
        let mut out = out.into_std().await;
        let mut out = |line| writeln!(&mut out, "{line}").unwrap();

        benchmark(
            control_client.clone(),
            &instances,
            &category,
            1 << 22,
            NonZeroUsize::new(32).unwrap(),
            NonZeroUsize::new(80).unwrap(),
            NonZeroUsize::new(88).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(10).unwrap(),
            1,
            &lines,
            &mut out,
        )
        .await?;

        benchmark(
            control_client.clone(),
            &instances,
            &category,
            1 << 23,
            NonZeroUsize::new(32).unwrap(),
            NonZeroUsize::new(80).unwrap(),
            NonZeroUsize::new(88).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(5).unwrap(),
            1,
            &lines,
            &mut out,
        )
        .await?;

        benchmark(
            control_client.clone(),
            &instances,
            &category,
            1 << 21,
            NonZeroUsize::new(32).unwrap(),
            NonZeroUsize::new(80).unwrap(),
            NonZeroUsize::new(88).unwrap(),
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(20).unwrap(),
            1,
            &lines,
            &mut out,
        )
        .await?;

        if !category.starts_with("ipfs") {
            benchmark(
                control_client.clone(),
                &instances,
                &category,
                1 << 23,
                NonZeroUsize::new(16).unwrap(),
                NonZeroUsize::new(40).unwrap(),
                NonZeroUsize::new(48).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(10).unwrap(),
                1,
                &lines,
                &mut out,
            )
            .await?;

            benchmark(
                control_client.clone(),
                &instances,
                &category,
                1 << 21,
                NonZeroUsize::new(64).unwrap(),
                NonZeroUsize::new(160).unwrap(),
                NonZeroUsize::new(168).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(10).unwrap(),
                1,
                &lines,
                &mut out,
            )
            .await?
        } else {
            // benchmark(
            //     control_client.clone(),
            //     &instances,
            //     &category,
            //     1 << 25,
            //     NonZeroUsize::new(32).unwrap(),
            //     NonZeroUsize::new(80).unwrap(),
            //     NonZeroUsize::new(88).unwrap(),
            //     NonZeroUsize::new(1).unwrap(),
            //     NonZeroUsize::new(1).unwrap(),
            //     1,
            //     &lines,
            //     &mut out,
            // )
            // .await?;
        }

        for num_per_region in [4, 8, 12, 16] {
            if category.starts_with("ipfs")
                && args().nth(2) != Some((num_per_region * 5 * 100).to_string())
            {
                continue;
            }
            let instances = retain_instances(&instances, num_per_region);
            assert_eq!(instances.len(), num_per_region * 5);
            benchmark(
                control_client.clone(),
                &instances,
                &category,
                1 << 22,
                NonZeroUsize::new(32).unwrap(),
                NonZeroUsize::new(80).unwrap(),
                NonZeroUsize::new(88).unwrap(),
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(10).unwrap(),
                1,
                &lines,
                &mut out,
            )
            .await?;
        }
    } else {
        let mut out = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open("entropy-1.txt")
            .await?;
        let mut lines = String::new();
        out.seek(Start(0)).await?;
        out.read_to_string(&mut lines).await?;
        let lines = lines.lines().map(ToString::to_string).collect::<Vec<_>>();
        let mut out = out.into_std().await;
        let mut out = |line| writeln!(&mut out, "{line}").unwrap();

        benchmark(
            control_client.clone(),
            &instances,
            &category,
            1 << 22,
            NonZeroUsize::new(32).unwrap(),
            NonZeroUsize::new(80).unwrap(),
            NonZeroUsize::new(88).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(10).unwrap(),
            600,
            &lines,
            &mut out,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn benchmark(
    control_client: reqwest::Client,
    instances: &[TerraformOutputInstance],
    category: &str,
    fragment_len: u32,
    chunk_k: NonZeroUsize,
    chunk_n: NonZeroUsize,
    chunk_m: NonZeroUsize,
    k: NonZeroUsize,
    n: NonZeroUsize,
    num_operation_per_hour: usize,
    lines: &[String],
    mut out: impl FnMut(String),
) -> anyhow::Result<()> {
    let chunk_len = fragment_len * chunk_k.get() as u32;
    assert_eq!(chunk_len as usize * k.get(), 1 << 30);
    let replication_factor = if category.starts_with("ipfs") { 3 } else { 1 };

    let prefix = format!(
        "NEAT,{},{chunk_k},{chunk_n},{chunk_m},{k},{n},{},{num_operation_per_hour}",
        if category.starts_with("ipfs") {
            "ipfs"
        } else {
            "entropy"
        },
        instances.len() * NUM_PEER_PER_IP,
    );
    println!("{prefix}");
    let count = lines
        .iter()
        .filter(|line| line.starts_with(&prefix))
        .count();
    // 10 operations
    if num_operation_per_hour == 1 && count >= 20 {
        return Ok(());
    }
    if num_operation_per_hour != 1 && count != 0 {
        return Ok(());
    }

    let mut start_peers_sessions = JoinSet::new();
    if !category.starts_with("ipfs") {
        let public_ips = instances
            .iter()
            .map(|instance| instance.public_ip)
            .collect::<Vec<_>>();
        for (index, instance) in instances.iter().enumerate() {
            start_peers_sessions.spawn(start_peers_session(
                control_client.clone(),
                format!("http://{}:3000", instance.public_ip),
                public_ips.clone(),
                index,
                instance.private_ip,
                fragment_len,
                chunk_k,
                chunk_n,
                chunk_m,
            ));
        }
        tokio::select! {
            () = sleep(Duration::from_secs(10)) => {}
            Some(result) = start_peers_sessions.join_next() => result??
        }
    }

    let peer_urls = if category.starts_with("ipfs") {
        instances
            .iter()
            .flat_map(|instance| {
                (0..100)
                    .map(|i| PeerUrl::Ipfs(format!("http://{}:{}", instance.public_ip, 5000 + i)))
            })
            .collect::<Vec<_>>()
    } else {
        instances
            .iter()
            .flat_map(|instance| {
                (0..100).map(|i| PeerUrl::Entropy(format!("http://{}:3000", instance.public_ip), i))
            })
            .collect::<Vec<_>>()
    };
    let instance_urls = instances
        .iter()
        .map(|instance| format!("http://{}:3000", instance.public_ip))
        .collect::<Vec<_>>();

    if num_operation_per_hour == 1 {
        let num_total = (20 - count) / 2 + 1;
        // let num_total = 1;
        close_loop_session(
            control_client.clone(),
            peer_urls.clone(),
            instance_urls.clone(),
            chunk_len,
            k,
            n,
            replication_factor,
            Arc::new(AtomicUsize::new(0)),
            0..num_total,
            |line| out(format!("{prefix},{line}")),
        )
        .await?
    } else {
        let interval = Duration::from_secs(3600) / num_operation_per_hour as u32;
        let interval_sleep = sleep(Duration::ZERO);
        tokio::pin!(interval_sleep);

        let mut open_loop_sessions = JoinSet::<anyhow::Result<_>>::new();
        let mut warmup_finish = false;
        let mut count = 0;
        let (out_sender, mut out_receiver) = unbounded_channel();
        let mut out_lines = Vec::new();
        loop {
            enum Select {
                Sleep,
                JoinNext(()),
                Recv(String),
            }
            match tokio::select! {
                () = &mut interval_sleep => Select::Sleep,
                result = open_loop_sessions.join_next() => Select::JoinNext(result.unwrap()??),
                recv = out_receiver.recv() => Select::Recv(recv.unwrap()),
            } {
                Select::Sleep => {
                    interval_sleep.as_mut().reset(Instant::now() + interval);

                    let put_url = instance_urls
                        .choose(&mut thread_rng())
                        .ok_or(anyhow::anyhow!("no instance available"))?;
                    let get_url = instance_urls
                        .choose(&mut thread_rng())
                        .ok_or(anyhow::anyhow!("no instance available"))?;
                    println!("Put {put_url} Get {get_url}");

                    if warmup_finish {
                        count += 1;
                        let out_sender = out_sender.clone();
                        open_loop_sessions.spawn(operation_session(
                            control_client.clone(),
                            put_url.clone(),
                            get_url.clone(),
                            peer_urls.clone(),
                            chunk_len,
                            k,
                            n,
                            replication_factor,
                            move |line| {
                                let _ = out_sender.send(line);
                            },
                        ));
                    } else {
                        open_loop_sessions.spawn(operation_session(
                            control_client.clone(),
                            put_url.clone(),
                            get_url.clone(),
                            peer_urls.clone(),
                            chunk_len,
                            k,
                            n,
                            replication_factor,
                            |_| {},
                        ));
                    }
                }
                Select::JoinNext(()) => warmup_finish = true,
                Select::Recv(line) => {
                    out_lines.push(format!("{prefix},{line}"));
                    if count >= 10 {
                        break;
                    }
                }
            }
        }
        out_receiver.close();
        let session = async {
            while let Some(result) = open_loop_sessions.join_next().await {
                result??
            }
            Result::<_, anyhow::Error>::Ok(())
        };
        tokio::select! {
            result = session => result?,
            Some(result) = start_peers_sessions.join_next() => result??,
        }
        assert!(open_loop_sessions.is_empty());
        while let Some(line) = out_receiver.recv().await {
            out_lines.push(format!("{prefix},{line}"))
        }
        out(out_lines.join("\n"))
    }

    if !category.starts_with("ipfs") {
        start_peers_sessions.shutdown().await;

        let mut stop_peers_sessions = JoinSet::new();
        for instance in instances {
            stop_peers_sessions.spawn(stop_peers_session(
                control_client.clone(),
                format!("http://{}:3000", instance.public_ip.clone()),
            ));
        }
        while let Some(result) = stop_peers_sessions.join_next().await {
            result??
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn start_peers_session(
    control_client: reqwest::Client,
    url: String,
    public_ips: Vec<IpAddr>,
    index: usize,
    private_ip: IpAddr,
    fragment_len: u32,
    chunk_k: NonZeroUsize,
    chunk_n: NonZeroUsize,
    chunk_m: NonZeroUsize,
) -> anyhow::Result<()> {
    let config = StartPeersConfig {
        ips: public_ips,
        ip_index: index,
        bind_ip: private_ip,
        num_peer_per_ip: NUM_PEER_PER_IP,
        fragment_len,
        chunk_k,
        chunk_n,
        chunk_m,
    };
    control_client
        .post(format!("{url}/start-peers"))
        .timeout(Duration::from_secs(10))
        .json(&config)
        .send()
        .await?
        .error_for_status()?;
    let mut count = 0;
    loop {
        sleep(Duration::from_secs(1)).await;
        let result = async {
            control_client
                .get(format!("{url}/ok"))
                .send()
                .await?
                .error_for_status()
        }
        .await;
        if let Err(err) = result {
            if !err.is_timeout() {
                return Err(err)?;
            }
            if count < 10 {
                count += 1;
                eprintln!("{err}")
            } else {
                Err(err)?
            }
        } else {
            count = 0
        }
    }
}

async fn stop_peers_session(control_client: reqwest::Client, url: String) -> anyhow::Result<()> {
    control_client
        .post(format!("{url}/stop-peers"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn operation_session(
    control_client: reqwest::Client,
    put_url: String,
    get_url: String,
    peer_urls: Vec<PeerUrl>,
    chunk_len: u32,
    k: NonZeroUsize,
    n: NonZeroUsize,
    replication_factor: usize,
    mut out: impl FnMut(String),
) -> anyhow::Result<()> {
    let put_peer_urls = peer_urls
        .choose_multiple(&mut thread_rng(), n.get() * replication_factor)
        .cloned()
        .collect::<Vec<_>>()
        .chunks_exact(replication_factor)
        .map(<[_]>::to_vec)
        .collect::<Vec<_>>();
    let config = PutConfig {
        chunk_len,
        k,
        peer_urls: put_peer_urls,
    };
    let put_id = control_client
        .post(format!("{put_url}/benchmark-put"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?
        .json::<u32>()
        .await?;
    println!("PutId {put_id}");
    let result = loop {
        sleep(Duration::from_secs(1)).await;
        let result = async {
            control_client
                .get(format!("{put_url}/benchmark-put/{put_id}"))
                .send()
                .await?
                .error_for_status()
        }
        .await;
        match result {
            Err(err) if err.is_timeout() => {
                eprintln!("{err}");
                continue;
            }
            Err(err) => Err(err)?,
            Ok(result) => {
                if let Some(result) = result.json::<Option<PutResult>>().await? {
                    break result;
                }
            }
        }
    };
    println!("Put {:?}", result.latency);
    out(format!(
        "put,{},{put_url},{put_id}",
        result.latency.as_secs_f32()
    ));
    // return Ok(());
    let digest = result.digest;

    sleep(Duration::from_secs(1)).await;
    let get_peer_urls = peer_urls
        .choose_multiple(&mut thread_rng(), n.into())
        .cloned()
        .collect();
    let config = GetConfig {
        chunk_len,
        k,
        chunks: result.chunks,
        peer_urls: get_peer_urls,
    };
    let get_id = control_client
        .post(format!("{get_url}/benchmark-get"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?
        .json::<u32>()
        .await?;
    println!("GetId {get_id}");
    let result = loop {
        sleep(Duration::from_secs(1)).await;
        let result = async {
            control_client
                .get(format!("{get_url}/benchmark-get/{get_id}"))
                .send()
                .await?
                .error_for_status()
        }
        .await;
        match result {
            Err(err) if err.is_timeout() => {
                eprintln!("{err}");
                continue;
            }
            Err(err) => Err(err)?,
            Ok(result) => {
                if let Some(result) = result.json::<Option<GetResult>>().await? {
                    break result;
                }
            }
        }
    };
    println!("Get {:?}", result.latency);
    out(format!(
        "get,{},{get_url},{get_id}",
        result.latency.as_secs_f32()
    ));
    if result.digest == digest {
        Ok(())
    } else {
        Err(anyhow::anyhow!("digest mismatch"))
    }
}

#[allow(clippy::too_many_arguments)]
async fn close_loop_session(
    control_client: reqwest::Client,
    peer_urls: Vec<PeerUrl>,    // choosen to serve put/get-chunk
    instance_urls: Vec<String>, // choosen to serve benchmark-put/get
    chunk_len: u32,
    k: NonZeroUsize,
    n: NonZeroUsize,
    replication_factor: usize,
    shared_count: Arc<AtomicUsize>,
    valid_range: Range<usize>,
    mut out: impl FnMut(String),
) -> anyhow::Result<()> {
    let mut count;
    while {
        count = shared_count.fetch_add(1, SeqCst);
        count < valid_range.end
    } {
        let backoff = Duration::from_millis(thread_rng().gen_range(1000..5000));
        sleep(backoff).await;
        let put_url = instance_urls
            .choose(&mut thread_rng())
            .ok_or(anyhow::anyhow!("no instance available"))?;
        let get_url = instance_urls
            .choose(&mut thread_rng())
            .ok_or(anyhow::anyhow!("no instance available"))?;
        println!("Put {put_url} Get {get_url}",);
        if valid_range.contains(&count) {
            operation_session(
                control_client.clone(),
                put_url.clone(),
                get_url.clone(),
                peer_urls.clone(),
                chunk_len,
                k,
                n,
                replication_factor,
                &mut out,
            )
            .await
        } else {
            println!("(Discard result of warmup/cooldown)");
            operation_session(
                control_client.clone(),
                put_url.clone(),
                get_url.clone(),
                peer_urls.clone(),
                chunk_len,
                k,
                n,
                replication_factor,
                |_| {},
            )
            .await
        }?
    }
    Ok(())
}
