// a layer of protocol that can communicate with both entropy and ipfs
// i guess that's also the reason why i choose to implement it in "raw" async
// sessions instead of event driven state machines
// maybe some day can make the conversion after an HTTP based network is
// available
// not sure whether the effort will pay off

use std::{collections::HashSet, sync::Arc};

use augustus::crypto::DigestHash as _;
use entropy_control_messages::{GetConfig, GetResult, PeerUrl, PutConfig, PutResult};
use rand::{thread_rng, RngCore as _};
use serde::{Deserialize, Serialize};
use tokio::{task::JoinSet, time::Instant};
use wirehair::{Decoder, Encoder};

pub async fn put_session(
    config: PutConfig,
    op_client: reqwest::Client,
) -> anyhow::Result<PutResult> {
    let mut buf = vec![0; config.chunk_len as usize * config.k.get()];
    thread_rng().fill_bytes(&mut buf);
    let digest = buf.sha256();
    let start = Instant::now();
    if config.k.get() == 1 {
        let chunk = put_impl_session(op_client, config.peer_urls[0].clone(), buf).await?;
        return Ok(PutResult {
            digest,
            chunks: vec![(0, chunk)],
            latency: start.elapsed(),
        });
    }

    let encoder = Arc::new(Encoder::new(buf, config.chunk_len)?);
    let mut encode_sessions = JoinSet::<anyhow::Result<_>>::new();
    let mut chunk_indexes = HashSet::<u32>::new();
    for peer_url_group in config.peer_urls {
        // for (index, peer_url_group) in config.peer_urls.into_iter().enumerate() {
        // let index = index as _;
        let mut index;
        while {
            index = rand::random();
            !chunk_indexes.insert(index)
        } {}
        let encoder = encoder.clone();
        let op_client = op_client.clone();
        encode_sessions.spawn(async move {
            let chunk = put_impl_session(op_client, peer_url_group, encoder.encode(index)?).await?;
            Ok((index, chunk))
        });
    }
    let mut chunks = Vec::new();
    while let Some(result) = encode_sessions.join_next().await {
        chunks.push(result??)
    }
    Ok(PutResult {
        digest,
        chunks,
        latency: start.elapsed(),
    })
}

async fn put_impl_session(
    op_client: reqwest::Client,
    peer_url_group: Vec<PeerUrl>,
    buf: Vec<u8>,
) -> anyhow::Result<String> {
    let mut put_sessions = JoinSet::<anyhow::Result<_>>::new();
    for (i, peer_url) in peer_url_group.into_iter().enumerate() {
        let op_client = op_client.clone();
        let buf = buf.clone();
        put_sessions.spawn(async move {
            let form =
                reqwest::multipart::Form::new().part("", reqwest::multipart::Part::bytes(buf));
            match peer_url {
                PeerUrl::Ipfs(peer_url) => {
                    #[allow(non_snake_case)]
                    #[derive(Deserialize)]
                    struct Response {
                        Hash: String,
                    }
                    let response = op_client
                        .post(format!("{peer_url}/api/v0/add"))
                        .multipart(form)
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<Response>()
                        .await?;
                    Ok(response.Hash)
                }
                PeerUrl::Entropy(url, peer_index) => {
                    assert_eq!(i, 0);
                    let response = op_client
                        .post(format!("{url}/put-chunk/{peer_index}"))
                        .multipart(form)
                        .send()
                        .await?
                        .error_for_status()?;
                    Ok(String::from_utf8(response.bytes().await?.to_vec())?)
                }
            }
        });
    }
    let chunk = put_sessions
        .join_next()
        .await
        .ok_or(anyhow::format_err!("no peer url for the chunk"))???;
    while let Some(also_chunk) = put_sessions.join_next().await {
        anyhow::ensure!(also_chunk?? == chunk, "inconsistent chunk among peers");
    }
    Ok(chunk)
}

pub async fn get_session(
    config: GetConfig,
    op_client: reqwest::Client,
) -> anyhow::Result<GetResult> {
    let start = Instant::now();
    let mut get_sessions = JoinSet::<anyhow::Result<_>>::new();
    for ((index, chunk), peer_url) in config.chunks.into_iter().zip(config.peer_urls) {
        let op_client = op_client.clone();
        get_sessions.spawn(async move {
            let buf = match peer_url {
                PeerUrl::Ipfs(peer_url) => {
                    #[derive(Serialize)]
                    struct Query {
                        arg: String,
                    }
                    op_client
                        .post(format!("{peer_url}/api/v0/cat"))
                        .query(&Query { arg: chunk })
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?
                }
                PeerUrl::Entropy(url, peer_index) => {
                    op_client
                        .post(format!("{url}/get-chunk/{peer_index}/{chunk}"))
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?
                }
            };
            Ok((index, buf))
        });
    }
    if config.k.get() == 1 {
        let (index, buf) = get_sessions.join_next().await.unwrap()??;
        assert_eq!(index, 0);
        let latency = start.elapsed();
        return Ok(GetResult {
            digest: buf.sha256(),
            latency,
        });
    }

    let mut decoder = Decoder::new(
        config.chunk_len as u64 * config.k.get() as u64,
        config.chunk_len,
    )?;
    while let Some(result) = get_sessions.join_next().await {
        let (index, buf) = result??;
        if decoder.decode(index, &buf)? {
            get_sessions.abort_all();
            let buf = decoder.recover()?;
            let latency = start.elapsed();
            get_sessions.shutdown().await;
            return Ok(GetResult {
                digest: buf.sha256(),
                latency,
            });
        }
    }
    Err(anyhow::format_err!("recover fail"))
}
