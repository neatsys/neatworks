use tokio::process::Command;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let ssh_hosts = ["localhost".to_string()];
    let ips = ["127.0.0.1".to_string()];

    let status = Command::new("cargo")
        .args([
            "--quiet",
            "build",
            "--profile",
            "artifact",
            "--package",
            "entropy-control",
            "--bin",
            "start-ipfs-daemons-impl",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `cargo build` exit with {status}")
    }

    // let mut sessions = JoinSet::new();
    let mut seed_addr = None;
    host_session(ssh_hosts[0].clone(), ips[0].clone(), &mut seed_addr).await?;
    // let seed_addr = seed_addr.unwrap();
    // for host in ssh_hosts.into_iter().skip(1) {
    //     let seed_addr = seed_addr.clone();
    //     sessions.spawn(async move { host_session(host, &mut Some(seed_addr)).await });
    // }
    // while let Some(result) = sessions.join_next().await {
    //     result??
    // }
    Ok(())
}

async fn host_session(
    ssh_host: String,
    ip: String,
    seed_addr: &mut Option<String>,
) -> anyhow::Result<()> {
    let status = Command::new("rsync")
        .arg("target/artifact/start-ipfs-daemons-impl")
        .arg(format!("{ssh_host}:"))
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `rsync` exit with {status}")
    }
    let mut command = Command::new("ssh");
    let mut command = command
        .arg(ssh_host)
        .arg("./start-ipfs-daemons-impl")
        .arg(ip);
    if let Some(seed_addr) = seed_addr {
        command = command.arg(seed_addr)
    }
    let status = command.status().await?;
    if !status.success() {
        anyhow::bail!("Command `ssh` exit with {status}")
    }
    // TODO record seed addr
    Ok(())
}
