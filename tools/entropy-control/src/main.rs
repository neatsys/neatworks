use std::{env::args, net::IpAddr, num::NonZeroUsize, time::Duration};

use entropy_control::terraform_instances;
use entropy_control_messages::{
    GetConfig, GetResult, PeerUrl, PutConfig, PutResult, StartPeersConfig,
};
use rand::{seq::SliceRandom, thread_rng};
use tokio::{task::JoinSet, time::sleep};

const NUM_PEER_PER_IP: usize = 100;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let category = args().nth(1);
    if category.as_deref() == Some("ipfs") {
        return benchmark_ipfs().await;
    }

    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;

    // let instances = terraform_instances().await?;
    // let instances = vec![instances[0].clone()];
    let instances = vec![entropy_control::TerraformOutputInstance {
        public_ip: [127, 0, 0, 1].into(),
        private_ip: [127, 0, 0, 1].into(),
        public_dns: "localhost".into(),
    }];
    let peer_urls = instances
        .iter()
        .flat_map(|instance| {
            (0..NUM_PEER_PER_IP)
                .map(|i| PeerUrl::Entropy(format!("http://{}:3000", instance.public_ip), i))
        })
        .collect::<Vec<_>>();

    let fragment_len = 1 << 25;
    let chunk_k = NonZeroUsize::new(4).unwrap();
    let chunk_n = NonZeroUsize::new(5).unwrap();
    let chunk_m = NonZeroUsize::new(8).unwrap();
    let k = NonZeroUsize::new(8).unwrap();
    let n = NonZeroUsize::new(10).unwrap();

    let public_ips = instances
        .iter()
        .map(|instance| instance.public_ip)
        .collect::<Vec<_>>();
    let mut start_peers_sessions = JoinSet::new();
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
    for _ in 0..10 {
        sleep(Duration::from_secs(3)).await;
        let put_instance = instances
            .choose(&mut thread_rng())
            .ok_or(anyhow::anyhow!("no instance available"))?;
        let get_instance = instances
            .choose(&mut thread_rng())
            .ok_or(anyhow::anyhow!("no instance available"))?;
        println!(
            "Put {} Get {}",
            put_instance.public_ip, get_instance.public_ip
        );
        let benchmark_session = benchmark_session(
            control_client.clone(),
            format!("http://{}:3000", put_instance.public_ip),
            format!("http://{}:3000", get_instance.public_ip),
            peer_urls.clone(),
            fragment_len * chunk_k.get() as u32,
            k,
            n,
            1,
        );
        tokio::select! {
            Some(result) = start_peers_sessions.join_next() => result??,
            result = benchmark_session => result?,
        }
        // break;
    }
    start_peers_sessions.shutdown().await;

    let mut stop_peers_sessions = JoinSet::new();
    for instance in instances {
        stop_peers_sessions.spawn(stop_peers_session(
            control_client.clone(),
            format!("http://{}:3000", instance.public_ip),
        ));
    }
    while let Some(result) = stop_peers_sessions.join_next().await {
        result??
    }
    Ok(())
}

async fn benchmark_ipfs() -> anyhow::Result<()> {
    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;

    let instances = terraform_instances().await?;
    // let instances = vec![instances[0].clone()];
    let peer_urls = instances
        .iter()
        .flat_map(|instance| {
            (0..100).map(|i| PeerUrl::Ipfs(format!("http://{}:{}", instance.public_ip, 5000 + i)))
        })
        .collect::<Vec<_>>();

    let fragment_len = 1 << 25;
    let chunk_k = NonZeroUsize::new(4).unwrap();
    let k = NonZeroUsize::new(8).unwrap();
    let n = NonZeroUsize::new(10).unwrap();

    for _ in 0..10 {
        sleep(Duration::from_secs(3)).await;
        let put_instance = instances
            .choose(&mut thread_rng())
            .ok_or(anyhow::anyhow!("no instance available"))?;
        let get_instance = instances
            .choose(&mut thread_rng())
            .ok_or(anyhow::anyhow!("no instance available"))?;
        println!(
            "Put {} Get {}",
            put_instance.public_ip, get_instance.public_ip
        );
        let benchmark_session = benchmark_session(
            control_client.clone(),
            format!("http://{}:3000", put_instance.public_ip),
            format!("http://{}:3000", get_instance.public_ip),
            peer_urls.clone(),
            fragment_len * chunk_k.get() as u32,
            k,
            n,
            3,
        );
        benchmark_session.await?
        // break;
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
        .json(&config)
        .send()
        .await?
        .error_for_status()?;
    loop {
        sleep(Duration::from_secs(1)).await;
        control_client
            .get(format!("{url}/ok"))
            .send()
            .await?
            .error_for_status()?;
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
async fn benchmark_session(
    control_client: reqwest::Client,
    put_url: String,
    get_url: String,
    peer_urls: Vec<PeerUrl>,
    chunk_len: u32,
    k: NonZeroUsize,
    n: NonZeroUsize,
    replication_factor: usize,
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
    let result = loop {
        sleep(Duration::from_secs(1)).await;
        if let Some(result) = control_client
            .get(format!("{put_url}/benchmark-put/{put_id}"))
            .send()
            .await?
            .error_for_status()?
            .json::<Option<PutResult>>()
            .await?
        {
            break result;
        }
    };
    println!("Put {:?}", result.latency);
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
    let result = loop {
        sleep(Duration::from_secs(1)).await;
        if let Some(result) = control_client
            .get(format!("{get_url}/benchmark-get/{get_id}"))
            .send()
            .await?
            .error_for_status()?
            .json::<Option<GetResult>>()
            .await?
        {
            break result;
        }
    };
    println!("Get {:?}", result.latency);
    if result.digest == digest {
        Ok(())
    } else {
        Err(anyhow::anyhow!("digest mismatch"))
    }
}
