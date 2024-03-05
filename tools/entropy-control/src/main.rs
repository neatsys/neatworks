use std::{env::args, net::IpAddr, num::NonZeroUsize, time::Duration};

use entropy_control::{terraform_instances, TerraformOutputInstance};
use entropy_control_messages::{
    GetConfig, GetResult, PeerUrl, PutConfig, PutResult, StartPeersConfig,
};
use rand::{seq::SliceRandom, thread_rng};
use tokio::{task::JoinSet, time::sleep};

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
    latency_benchmark(
        control_client,
        &instances,
        &category,
        1 << 20,
        32.try_into().unwrap(),
        80.try_into().unwrap(),
        88.try_into().unwrap(),
        8.try_into().unwrap(),
        10.try_into().unwrap(),
        10,
        &[],
        |line| println!("{line}"),
    )
    .await?;

    // if !category.ends_with("stress") {
    //     let mut out = OpenOptions::new()
    //         .create(true)
    //         .read(true)
    //         .append(true)
    //         .open("entropy-5.txt")
    //         .await?;
    //     let mut lines = String::new();
    //     out.seek(Start(0)).await?;
    //     out.read_to_string(&mut lines).await?;
    //     let lines = lines.lines().map(ToString::to_string).collect::<Vec<_>>();
    //     let mut out = out.into_std().await;
    //     let mut out = |line| writeln!(&mut out, "{line}").unwrap();

    //     benchmark(
    //         control_client.clone(),
    //         &instances,
    //         &category,
    //         1 << 22,
    //         NonZeroUsize::new(32).unwrap(),
    //         NonZeroUsize::new(80).unwrap(),
    //         NonZeroUsize::new(88).unwrap(),
    //         NonZeroUsize::new(8).unwrap(),
    //         NonZeroUsize::new(10).unwrap(),
    //         1,
    //         &lines,
    //         &mut out,
    //     )
    //     .await?;
    //     // return Ok(());

    //     benchmark(
    //         control_client.clone(),
    //         &instances,
    //         &category,
    //         1 << 23,
    //         NonZeroUsize::new(32).unwrap(),
    //         NonZeroUsize::new(80).unwrap(),
    //         NonZeroUsize::new(88).unwrap(),
    //         NonZeroUsize::new(4).unwrap(),
    //         NonZeroUsize::new(5).unwrap(),
    //         1,
    //         &lines,
    //         &mut out,
    //     )
    //     .await?;

    //     benchmark(
    //         control_client.clone(),
    //         &instances,
    //         &category,
    //         1 << 21,
    //         NonZeroUsize::new(32).unwrap(),
    //         NonZeroUsize::new(80).unwrap(),
    //         NonZeroUsize::new(88).unwrap(),
    //         NonZeroUsize::new(16).unwrap(),
    //         NonZeroUsize::new(20).unwrap(),
    //         1,
    //         &lines,
    //         &mut out,
    //     )
    //     .await?;

    //     if !category.starts_with("ipfs") {
    //         benchmark(
    //             control_client.clone(),
    //             &instances,
    //             &category,
    //             1 << 23,
    //             NonZeroUsize::new(16).unwrap(),
    //             NonZeroUsize::new(40).unwrap(),
    //             NonZeroUsize::new(48).unwrap(),
    //             NonZeroUsize::new(8).unwrap(),
    //             NonZeroUsize::new(10).unwrap(),
    //             1,
    //             &lines,
    //             &mut out,
    //         )
    //         .await?;

    //         benchmark(
    //             control_client.clone(),
    //             &instances,
    //             &category,
    //             1 << 21,
    //             NonZeroUsize::new(64).unwrap(),
    //             NonZeroUsize::new(160).unwrap(),
    //             NonZeroUsize::new(168).unwrap(),
    //             NonZeroUsize::new(8).unwrap(),
    //             NonZeroUsize::new(10).unwrap(),
    //             1,
    //             &lines,
    //             &mut out,
    //         )
    //         .await?
    //     } else {
    //         // benchmark(
    //         //     control_client.clone(),
    //         //     &instances,
    //         //     &category,
    //         //     1 << 25,
    //         //     NonZeroUsize::new(32).unwrap(),
    //         //     NonZeroUsize::new(80).unwrap(),
    //         //     NonZeroUsize::new(88).unwrap(),
    //         //     NonZeroUsize::new(1).unwrap(),
    //         //     NonZeroUsize::new(1).unwrap(),
    //         //     1,
    //         //     &lines,
    //         //     &mut out,
    //         // )
    //         // .await?;
    //     }

    //     for num_per_region in [4, 8, 12, 16] {
    //         // well, i can just run ipfs according to what specified from command line
    //         // but this is safer right?
    //         if category.starts_with("ipfs")
    //             && args().nth(2) != Some((num_per_region * 5 * 100).to_string())
    //         {
    //             continue;
    //         }
    //         let instances = retain_instances(&instances, num_per_region);
    //         assert_eq!(instances.len(), num_per_region * 5);
    //         benchmark(
    //             control_client.clone(),
    //             &instances,
    //             &category,
    //             1 << 22,
    //             NonZeroUsize::new(32).unwrap(),
    //             NonZeroUsize::new(80).unwrap(),
    //             NonZeroUsize::new(88).unwrap(),
    //             NonZeroUsize::new(8).unwrap(),
    //             NonZeroUsize::new(10).unwrap(),
    //             1,
    //             &lines,
    //             &mut out,
    //         )
    //         .await?;
    //     }
    // } else {
    //     let mut out = OpenOptions::new()
    //         .create(true)
    //         .read(true)
    //         .append(true)
    //         .open("entropy-1.txt")
    //         .await?;
    //     let mut lines = String::new();
    //     out.seek(Start(0)).await?;
    //     out.read_to_string(&mut lines).await?;
    //     let lines = lines.lines().map(ToString::to_string).collect::<Vec<_>>();
    //     let mut out = out.into_std().await;
    //     let mut out = |line| writeln!(&mut out, "{line}").unwrap();

    //     // for n in (1..10).map(|i| i * 600).chain((1..10).map(|i| i * 6000)) {
    //     for n in [10, 20, 40, 100, 200] {
    //         benchmark(
    //             control_client.clone(),
    //             &instances,
    //             &category,
    //             1 << 22,
    //             NonZeroUsize::new(32).unwrap(),
    //             NonZeroUsize::new(80).unwrap(),
    //             NonZeroUsize::new(88).unwrap(),
    //             NonZeroUsize::new(8).unwrap(),
    //             NonZeroUsize::new(10).unwrap(),
    //             n,
    //             &lines,
    //             &mut out,
    //         )
    //         .await?;
    //         // sleep(Duration::from_secs(10)).await
    //     }
    // }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn latency_benchmark(
    control_client: reqwest::Client,
    instances: &[TerraformOutputInstance],
    category: &str,
    fragment_len: u32,
    chunk_k: NonZeroUsize,
    chunk_n: NonZeroUsize,
    chunk_m: NonZeroUsize,
    k: NonZeroUsize,
    n: NonZeroUsize,
    num_total: usize,
    lines: &[String],
    mut out: impl FnMut(String),
) -> anyhow::Result<()> {
    let prefix = format!(
        "NEAT,{},{chunk_k},{chunk_n},{chunk_m},{k},{n},{}",
        if category.starts_with("ipfs") {
            "ipfs"
        } else {
            "entropy"
        },
        instances.len() * NUM_PEER_PER_IP,
    );
    println!("{prefix}");
    let _count = lines
        .iter()
        .filter(|line| line.starts_with(&(prefix.clone() + ",")))
        .count();
    // TODO skip when `count` is already sufficient

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
            () = sleep(Duration::from_millis(1200)) => {}
            Some(result) = start_peers_sessions.join_next() => result??
        }
    }

    for _ in 0..num_total {
        operation_session(
            control_client.clone(),
            instances,
            category,
            fragment_len,
            chunk_k,
            k,
            n,
            &mut out,
        )
        .await?
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
    instances: &[TerraformOutputInstance],
    category: &str,
    fragment_len: u32,
    chunk_k: NonZeroUsize,
    k: NonZeroUsize,
    n: NonZeroUsize,
    mut out: impl FnMut(String),
) -> anyhow::Result<()> {
    let chunk_len = fragment_len * chunk_k.get() as u32;
    // assert_eq!(chunk_len as usize * k.get(), 1 << 30);
    let replication_factor = if category.starts_with("ipfs") { 3 } else { 1 };

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

    let put_url = instance_urls
        .choose(&mut thread_rng())
        .ok_or(anyhow::anyhow!("no instance available"))?;
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
    let get_url = instance_urls
        .choose(&mut thread_rng())
        .ok_or(anyhow::anyhow!("no instance available"))?;
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
