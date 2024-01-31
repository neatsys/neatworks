use std::{env::args, process::Stdio, time::Duration};

use entropy_control::terraform_instances;
use tokio::{process::Command, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let kubo_path = args().nth(1);
    let kubo_path = kubo_path
        .as_deref()
        .unwrap_or("../kubo_v0.26.0_linux-amd64.tar.gz");

    println!("Building ipfs-script artifact");
    let status = Command::new("cargo")
        .args([
            "--quiet",
            "build",
            "--profile",
            "artifact",
            "--package",
            "entropy-control",
            "--bin",
            "ipfs-script",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `cargo build` exit with {status}")
    }

    println!("Spawning host sessions");
    let mut sessions = JoinSet::new();
    for instance in terraform_instances().await? {
        sessions.spawn(host_session(instance.public_dns, kubo_path.to_string()));
    }
    let result = join_sessions(&mut sessions).await;
    sessions.shutdown().await;
    if result.is_err() {
        sleep(Duration::from_secs(1)).await
    }
    result
}

async fn join_sessions(sessions: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
    while let Some(result) = sessions.join_next().await {
        result??
    }
    Ok(())
}

async fn host_session(ssh_host: String, kubo_path: String) -> anyhow::Result<()> {
    // now i start to regret on using Debian instead of Ubuntu...
    let status = Command::new("ssh")
        .arg("-t")
        .arg(&ssh_host)
        .args(["sudo apt-get update && sudo apt-get install --yes rsync tmux"])
        .stdout(Stdio::null())
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `apt-get` exit with {status}")
    }

    let status = Command::new("rsync")
        .arg("target/artifact/ipfs-script")
        .arg(kubo_path)
        .arg(format!("{ssh_host}:"))
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `rsync` exit with {status}")
    }

    let status = Command::new("ssh")
        .arg(ssh_host)
        .arg("./ipfs-script")
        .arg("install")
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `ssh` exit with {status}")
    }

    Ok(())
}
