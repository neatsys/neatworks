use std::{net::IpAddr, process::Stdio, time::Duration};

use boson_control::{terraform_instances, terraform_quorum_instances};
use tokio::{process::Command, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    println!("Spawning host sessions");
    let mut sessions = JoinSet::new();
    for instance in terraform_instances().await? {
        sessions.spawn(host_session(instance.public_dns, instance.public_ip));
    }
    for instance in terraform_quorum_instances().await? {
        sessions.spawn(host_session(instance.public_dns, instance.public_ip));
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

async fn host_session(ssh_host: String, public_ip: IpAddr) -> anyhow::Result<()> {
    // principly the script should be idempotent, but the following commands are not
    // so offer some mercy
    let status = Command::new("ssh")
        .arg(&ssh_host)
        .arg("sudo")
        .arg("ip")
        .arg("addr")
        .arg("add")
        .arg(public_ip.to_string())
        .arg("dev")
        .arg("ens5")
        .stderr(Stdio::null())
        .status()
        .await?;
    if !status.success() {
        // anyhow::bail!("Command `ssh` exit with {status}")
        use std::io::Write;
        writeln!(&mut std::io::stdout().lock(), "Command `ip` failed")?
    }

    let status = Command::new("ssh")
        .arg(&ssh_host)
        .arg("sudo")
        .arg("ethtool")
        .arg("-G")
        .arg("ens5")
        .arg("rx")
        .arg("16384")
        .stderr(Stdio::null())
        .status()
        .await?;
    if !status.success() {
        // anyhow::bail!("Command `ssh` exit with {status}")
        use std::io::Write;
        writeln!(&mut std::io::stdout().lock(), "Command `ethtool` failed")?
    }

    Ok(())
}
