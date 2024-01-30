use std::process::Stdio;

use serde::Deserialize;
use tokio::{process::Command, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let hosts = ["127.0.0.1".to_string()];
    let mut sessions = JoinSet::new();
    let mut seed_addr = None;
    host_session(hosts[0].clone(), &mut seed_addr).await?;
    let seed_addr = seed_addr.unwrap();
    for host in hosts.into_iter().skip(1) {
        let seed_addr = seed_addr.clone();
        sessions.spawn(async move { host_session(host, &mut Some(seed_addr)).await });
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }
    Ok(())
}

async fn host_session(ip: String, seed_addr: &mut Option<String>) -> anyhow::Result<()> {
    for i in 0..100 {
        let status = Command::new("ssh")
            .arg(&ip)
            .arg(format!(
                "IPFS_PATH=/tmp/ipfs-{i} ipfs init --profile server"
            ))
            .stdout(Stdio::null())
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs init` exit with {status}")
        }
        let status = Command::new("ssh")
            .arg(&ip)
            .arg(format!(
                "IPFS_PATH=/tmp/ipfs-{i} ipfs config Addresses.Swarm --json '{}'",
                serde_json::to_string(&[format!("/ip4/{ip}/tcp/{}", 4000 + i)])?
            ))
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs config Addresses.Swarm` exit with {status}")
        }
        let status = Command::new("ssh")
            .arg(&ip)
            .arg(format!(
                "IPFS_PATH=/tmp/ipfs-{i} ipfs config Addresses.API --json '{}'",
                serde_json::to_string(&format!("/ip4/{ip}/tcp/{}", 5000 + i))?
            ))
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs config Addresses.API` exit with {status}")
        }
        let status = Command::new("ssh")
            .arg(&ip)
            .arg(format!(
                "IPFS_PATH=/tmp/ipfs-{i} ipfs config Addresses.Gateway --json '{}'",
                serde_json::to_string(&[format!("/ip4/{ip}/tcp/{}", 8000 + i)])?
            ))
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs config Addresses.Gateway` exit with {status}")
        }

        let status = Command::new("ssh")
            .arg(&ip)
            .arg(format!("IPFS_PATH=/tmp/ipfs-{i} ipfs bootstrap rm all"))
            .stdout(Stdio::null())
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs bootstrap rm all` exit with {status}")
        }
        if let Some(seed_addr) = seed_addr {
            let status = Command::new("ssh")
                .arg(&ip)
                .arg(format!(
                    "IPFS_PATH=/tmp/ipfs-{i} ipfs bootstrap add {seed_addr}"
                ))
                .stdout(Stdio::null())
                .status()
                .await?;
            if !status.success() {
                anyhow::bail!("Command `ipfs bootstrap add` exit with {status}")
            }
        }

        let status = Command::new("ssh")
            .arg(&ip)
            .arg(format!(
                "tmux new -d -s ipfs-{i} \"IPFS_PATH=/tmp/ipfs-{i} ipfs daemon\""
            ))
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs daemon` exit with {status}")
        }
        if seed_addr.is_none() {
            let out = Command::new("ssh")
                .arg(&ip)
                .arg(format!("IPFS_PATH=/tmp/ipfs-{i} ipfs id"))
                .output()
                .await?
                .stdout;
            #[allow(non_snake_case)]
            #[derive(Deserialize)]
            struct Out {
                Addresses: Vec<String>,
            }
            *seed_addr = Some(
                serde_json::from_slice::<Out>(&out)?
                    .Addresses
                    .into_iter()
                    .next()
                    .ok_or(anyhow::anyhow!("seed peer has no address"))?,
            );
            println!("Seed address {seed_addr:?}")
        }
    }
    Ok(())
}
