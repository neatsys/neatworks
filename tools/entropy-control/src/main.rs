use std::time::Duration;

use entropy_control_messages::{GetConfig, GetResult, PeerUrl, PutConfig, PutResult};
use rand::{seq::SliceRandom, thread_rng};
use tokio::time::sleep;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let control_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?;
    benchmark_session(control_client).await
}

async fn benchmark_session(control_client: reqwest::Client) -> anyhow::Result<()> {
    let client_url = "http://localhost:3000".to_string();
    let peer_urls = (0..10)
        .map(|i| PeerUrl::Ipfs(format!("http://localhost:{}", 5000 + i)))
        .collect::<Vec<_>>();
    let chunk_len = 100;
    let k = 5.try_into().unwrap();
    let n = 4;

    let put_peer_urls = peer_urls
        .choose_multiple(&mut thread_rng(), n)
        .cloned()
        .collect::<Vec<_>>()
        .chunks_exact(1)
        .map(<[_]>::to_vec)
        .collect::<Vec<_>>();
    let config = PutConfig {
        chunk_len,
        k,
        peer_urls: put_peer_urls,
    };
    let put_id = control_client
        .post(format!("{client_url}/benchmark-put"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?
        .json::<u32>()
        .await?;
    let result = loop {
        sleep(Duration::from_secs(1)).await;
        if let Some(result) = control_client
            .get(format!("{client_url}/benchmark-put/{put_id}"))
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
    let digest = result.digest;

    let get_peer_urls = peer_urls
        .choose_multiple(&mut thread_rng(), n)
        .cloned()
        .collect();
    let config = GetConfig {
        chunk_len,
        k,
        chunks: result.chunks,
        peer_urls: get_peer_urls,
    };
    let get_id = control_client
        .post(format!("{client_url}/benchmark-get"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?
        .json::<u32>()
        .await?;
    let result = loop {
        sleep(Duration::from_secs(1)).await;
        if let Some(result) = control_client
            .get(format!("{client_url}/benchmark-get/{get_id}"))
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
