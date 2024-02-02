use std::{
    env::args,
    io::{SeekFrom::Start, Write},
    net::IpAddr,
    num::NonZeroUsize,
    ops::Range,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    },
    time::Duration,
};

use entropy_control::{terraform_instances, TerraformOutputInstance};
use entropy_control_messages::{
    GetConfig, GetResult, PeerUrl, PutConfig, PutResult, StartPeersConfig,
};
use rand::{seq::SliceRandom, thread_rng, Rng};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt},
    task::JoinSet,
    time::sleep,
};

const NUM_PEER_PER_IP: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let category = args().nth(1).unwrap_or_default();

    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
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

        // benchmark(
        //     control_client.clone(),
        //     &instances,
        //     &category,
        //     1 << 22,
        //     NonZeroUsize::new(32).unwrap(),
        //     NonZeroUsize::new(80).unwrap(),
        //     NonZeroUsize::new(88).unwrap(),
        //     NonZeroUsize::new(8).unwrap(),
        //     NonZeroUsize::new(10).unwrap(),
        //     1,
        //     &lines,
        //     &mut out,
        // )
        // .await?;

        // benchmark(
        //     control_client.clone(),
        //     &instances,
        //     &category,
        //     1 << 23,
        //     NonZeroUsize::new(32).unwrap(),
        //     NonZeroUsize::new(80).unwrap(),
        //     NonZeroUsize::new(88).unwrap(),
        //     NonZeroUsize::new(4).unwrap(),
        //     NonZeroUsize::new(5).unwrap(),
        //     1,
        //     &lines,
        //     &mut out,
        // )
        // .await?;

        // benchmark(
        //     control_client.clone(),
        //     &instances,
        //     &category,
        //     1 << 21,
        //     NonZeroUsize::new(32).unwrap(),
        //     NonZeroUsize::new(80).unwrap(),
        //     NonZeroUsize::new(88).unwrap(),
        //     NonZeroUsize::new(16).unwrap(),
        //     NonZeroUsize::new(20).unwrap(),
        //     1,
        //     &lines,
        //     &mut out,
        // )
        // .await?;

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
            benchmark(
                control_client.clone(),
                &instances,
                &category,
                1 << 25,
                NonZeroUsize::new(32).unwrap(),
                NonZeroUsize::new(80).unwrap(),
                NonZeroUsize::new(88).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                1,
                &lines,
                &mut out,
            )
            .await?;
        }
    } else {
        todo!()
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
    num_concurrency: usize,
    lines: &[String],
    mut out: impl FnMut(String),
) -> anyhow::Result<()> {
    assert_eq!(fragment_len as usize * chunk_k.get() * k.get(), 1 << 30);
    let prefix = format!(
        "NEAT,{},{chunk_k},{chunk_n},{chunk_m},{k},{n},{num_concurrency}",
        if category.starts_with("ipfs") {
            "ipfs"
        } else {
            "entropy"
        }
    );
    let count = lines
        .iter()
        .filter(|line| line.starts_with(&prefix))
        .count();
    // 10 operations
    if num_concurrency == 1 && count >= 20 {
        return Ok(());
    }
    if num_concurrency != 1 && count != 0 {
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
            () = sleep(Duration::from_secs(3)) => {}
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

    if num_concurrency == 1 {
        let num_total = 10 - count;
        close_loop_session(
            control_client.clone(),
            peer_urls.clone(),
            instance_urls.clone(),
            fragment_len * chunk_k.get() as u32,
            k,
            n,
            1,
            Arc::new(AtomicUsize::new(0)),
            0..num_total,
            |line| out(format!("{prefix},{line}")),
        )
        .await?
    } else {
        // 1x for warmup, 1x for cooldown, save the middle 2x
        let num_total = (num_concurrency * 4).max(10);

        let mut close_loop_sessions = JoinSet::new();
        let count = Arc::new(AtomicUsize::new(0));
        let out_lines = Arc::new(Mutex::new(Vec::new()));
        for _ in 0..num_concurrency {
            let out_lines = out_lines.clone();
            close_loop_sessions.spawn(close_loop_session(
                control_client.clone(),
                peer_urls.clone(),
                instance_urls.clone(),
                fragment_len * chunk_k.get() as u32,
                k,
                n,
                1,
                count.clone(),
                num_concurrency..num_total,
                // num_concurrency..0,
                move |line| out_lines.lock().unwrap().push(line),
            ));
        }
        let session = async {
            while let Some(result) = close_loop_sessions.join_next().await {
                result??
            }
            Result::<_, anyhow::Error>::Ok(())
        };
        tokio::select! {
            result = session => result?,
            Some(result) = start_peers_sessions.join_next() => result??,
        }
        assert!(close_loop_sessions.is_empty());
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
