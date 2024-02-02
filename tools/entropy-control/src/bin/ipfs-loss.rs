use std::{env::args, time::Duration};

use entropy_control::{terraform_instances, TerraformOutputInstance};
use rand::{seq::SliceRandom, thread_rng, Rng, RngCore};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use tokio::{process::Command, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let replication_factor = args()
        .nth(1)
        .ok_or(anyhow::anyhow!("not specify replication factor"))?
        .parse()?;
    let op_client = reqwest::Client::new();

    let instances = terraform_instances().await?;
    println!("Spawning resume sessions");
    let mut sessions = JoinSet::new();
    for instance in instances.clone() {
        sessions.spawn(resume_session(instance.public_dns));
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }

    let peers = instances
        .clone()
        .into_iter()
        .flat_map(|instance| (0..100).map(move |i| (instance.clone(), i)))
        .collect::<Vec<_>>();
    let peer_url = |(instance, index): &(TerraformOutputInstance, usize)| {
        format!("http://{}:{}", instance.public_ip, 5000 + index)
    };

    println!("Spawning put sessions");
    let mut sessions = JoinSet::new();
    for _ in 0..1000 {
        sessions.spawn(put_session(
            op_client.clone(),
            peers
                .choose_multiple(&mut thread_rng(), replication_factor)
                .map(peer_url)
                .collect(),
        ));
    }
    let mut hashes = Vec::new();
    while let Some(result) = sessions.join_next().await {
        hashes.push(result??)
    }

    let mut honest_peers = peers.clone();
    let mut faulty_peers = Vec::new();
    for faulty_rate in [0., 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99] {
        println!("Spwaning shutdown sessions");
        let faulty_len = faulty_rate * peers.len() as f32;
        let mut sessions = JoinSet::new();
        while (faulty_peers.len() as f32) < faulty_len {
            let index = thread_rng().gen_range(0..honest_peers.len());
            let peer = honest_peers.remove(index);
            sessions.spawn(shutdown_session(op_client.clone(), peer_url(&peer)));
            faulty_peers.push(peer)
        }
        while let Some(result) = sessions.join_next().await {
            result??
        }
        println!("Waiting for faulty peers down");
        sleep(Duration::from_secs(5)).await;

        println!("Spawning get sessions");
        let mut sessions = JoinSet::new();
        for hash in &hashes {
            sessions.spawn(get_session(
                op_client.clone(),
                hash.clone(),
                peer_url(
                    honest_peers
                        .choose(&mut thread_rng())
                        .ok_or(anyhow::anyhow!("cannot choose peer"))?,
                ),
            ));
        }
        let mut alive_count = 0;
        while let Some(result) = sessions.join_next().await {
            if result?? {
                alive_count += 1
            }
        }
        println!("Alive {alive_count}");
        println!("NEAT,{replication_factor},{faulty_rate},{alive_count}")
    }
    println!("Spawning resume sessions");
    let mut sessions = JoinSet::new();
    for instance in instances {
        sessions.spawn(resume_session(instance.public_dns));
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }

    Ok(())
}

async fn put_session(op_client: reqwest::Client, peer_urls: Vec<String>) -> anyhow::Result<String> {
    let mut data = vec![0; 1 << 20];
    thread_rng().fill_bytes(&mut data);
    let mut saved_hash = None;
    for peer_url in peer_urls {
        #[allow(non_snake_case)]
        #[derive(Deserialize)]
        struct Response {
            Hash: String,
        }
        let mut count = 0;
        let hash = loop {
            match async {
                Result::<_, anyhow::Error>::Ok(
                    op_client
                        .post(format!("{peer_url}/api/v0/add"))
                        .multipart(Form::new().part("", Part::bytes(data.clone())))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<Response>()
                        .await?
                        .Hash,
                )
            }
            .await
            {
                Ok(hash) => break hash,
                Err(err) => {
                    eprintln!("{err}");
                    count += 1;
                    if count == 3 {
                        Err(err)?
                    }
                }
            }
        };
        if let Some(saved_hash) = &saved_hash {
            if hash != *saved_hash {
                anyhow::bail!("inconsistent hashes")
            }
        } else {
            saved_hash = Some(hash)
        }
    }
    saved_hash.ok_or(anyhow::anyhow!("cannot choose three peers"))
}

async fn shutdown_session(op_client: reqwest::Client, peer_url: String) -> anyhow::Result<()> {
    op_client
        .post(format!("{peer_url}/api/v0/shutdown"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn get_session(
    op_client: reqwest::Client,
    hash: String,
    peer_url: String,
) -> anyhow::Result<bool> {
    #[derive(Serialize)]
    struct Query {
        arg: String,
    }
    let result = op_client
        .post(format!("{peer_url}/api/v0/cat"))
        .query(&Query { arg: hash.clone() })
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    match result {
        Err(err) if err.is_timeout() => Ok(false),
        Err(err) => Err(err)?,
        Ok(response) => {
            response.error_for_status()?;
            op_client
                .post(format!("{peer_url}/api/v0/block/rm"))
                .query(&Query { arg: hash })
                .send()
                .await?
                .error_for_status()?;
            Ok(true)
        }
    }
}

async fn resume_session(ssh_host: String) -> anyhow::Result<()> {
    let status = Command::new("ssh")
        .arg(&ssh_host)
        .arg("./ipfs-script")
        .arg("resume-peers")
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `ipfs-script` exit with {status} ({ssh_host})")
    }
    Ok(())
}
