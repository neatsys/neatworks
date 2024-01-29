use std::process::Stdio;

use tokio::{process::Command, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let hosts = ["127.0.0.1".to_string()];
    let mut sessions = JoinSet::new();
    for host in hosts {
        sessions.spawn(async move { host_session(host).await });
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }
    Ok(())
}

async fn host_session(ip: String) -> anyhow::Result<()> {
    let status = Command::new("ssh")
        .arg(&ip)
        // `-x` for exact name otherwise this script itself will be interrupted as well
        .arg("pkill -INT -x ipfs")
        .status()
        .await?;
    if status.success() {
        println!("Interrupted previous IPFS peers")
    }
    let status = Command::new("ssh")
        .arg(&ip)
        .arg("rm -r /tmp/ipfs-*")
        .stderr(Stdio::null())
        .status()
        .await?;
    if status.success() {
        println!("Removed previous IPFS data")
    }
    Ok(())
}
