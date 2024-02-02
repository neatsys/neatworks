use std::{env::args, net::IpAddr, time::Duration};

use entropy_control::{retain_instances, terraform_instances};
use tokio::{process::Command, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // let ssh_hosts = ["localhost".to_string()];
    // let ips = ["127.0.0.1".to_string()];
    let mut instances = terraform_instances().await?;
    if let Some(num_peer) = args().nth(1) {
        let num_peer = num_peer.parse::<usize>()?;
        instances = retain_instances(&instances, num_peer / 5);
        assert_eq!(instances.len(), num_peer)
    }
    // println!("{instances:?}");
    // return Ok(());

    let mut sessions = JoinSet::new();
    let mut seed_addr = None;
    host_session(
        instances[0].public_dns.clone(),
        instances[0].public_ip,
        instances[0].private_ip,
        &mut seed_addr,
    )
    .await?;
    let seed_addr = seed_addr.unwrap();
    for instance in instances.into_iter().skip(1) {
        let seed_addr = seed_addr.clone();
        sessions.spawn(async move {
            host_session(
                instance.public_dns,
                instance.public_ip,
                instance.private_ip,
                &mut Some(seed_addr),
            )
            .await
        });
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

async fn host_session(
    ssh_host: String,
    public_ip: IpAddr,
    private_ip: IpAddr,
    seed_addr: &mut Option<String>,
) -> anyhow::Result<()> {
    let mut command = Command::new("ssh");
    let mut command = command
        .arg(ssh_host)
        .arg("./ipfs-script")
        .arg("start-peers")
        .arg(public_ip.to_string())
        .arg(private_ip.to_string());
    let status;
    if let Some(seed_addr) = seed_addr {
        command = command.arg(seed_addr);
        status = command.status().await?;
    } else {
        let output = command.output().await?;
        if output.status.success() {
            *seed_addr = Some(String::from_utf8(output.stdout)?);
            println!("Seed address {seed_addr:?}")
        }
        status = output.status
    }
    if !status.success() {
        anyhow::bail!("Command `ssh` exit with {status}")
    }
    // TODO record seed addr
    Ok(())
}
