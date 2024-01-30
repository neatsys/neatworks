use std::{net::IpAddr, num::NonZeroUsize, time::Duration};

use entropy_control_messages::{
    GetConfig, GetResult, PeerUrl, PutConfig, PutResult, StartPeersConfig,
};
use rand::{seq::SliceRandom, thread_rng};
use tokio::time::sleep;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;

    // let peer_urls = (0..100)
    //     .map(|i| PeerUrl::Ipfs(format!("http://localhost:{}", 5000 + i)))
    //     .collect::<Vec<_>>();
    let peer_urls = (0..100)
        .map(|i| PeerUrl::Entropy("http://localhost:3000".into(), i))
        .collect::<Vec<_>>();

    let fragment_len = 1 << 20;
    let chunk_k = 4.try_into().unwrap();
    let chunk_n = 5.try_into().unwrap();
    let chunk_m = 8.try_into().unwrap();
    let k = 8.try_into().unwrap();
    let n = 10.try_into().unwrap();

    let mut start_peers_session = tokio::spawn(start_peers_session(
        control_client.clone(),
        "http://localhost:3000".into(),
        fragment_len,
        chunk_k,
        chunk_n,
        chunk_m,
    ));
    for _ in 0..10 {
        sleep(Duration::from_secs(1)).await;
        let benchmark_session = benchmark_session(
            control_client.clone(),
            "http://localhost:3000".into(),
            "http://localhost:3000".into(),
            peer_urls.clone(),
            fragment_len * chunk_k.get() as u32,
            k,
            n,
            1,
            // 3,
        );
        tokio::select! {
            result = &mut start_peers_session => result??,
            result = benchmark_session => result?,
        }
    }
    Ok(())
}

async fn start_peers_session(
    control_client: reqwest::Client,
    url: String,
    fragment_len: u32,
    chunk_k: NonZeroUsize,
    chunk_n: NonZeroUsize,
    chunk_m: NonZeroUsize,
) -> anyhow::Result<()> {
    let config = StartPeersConfig {
        ips: vec![IpAddr::from([127, 0, 0, 1])],
        ip_index: 0,
        num_peer_per_ip: 100,
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
