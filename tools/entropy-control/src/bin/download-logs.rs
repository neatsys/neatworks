use std::time::Duration;

use entropy_control::terraform_instances;
use tokio::{process::Command, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    println!("Spawning host sessions");
    let mut sessions = JoinSet::new();
    for instance in terraform_instances().await? {
        sessions.spawn(async move { host_session(instance.public_dns).await });
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

async fn host_session(ssh_host: String) -> anyhow::Result<()> {
    let status = Command::new("rsync")
        .arg(format!("{ssh_host}:entropy.errors"))
        .arg(format!("entropy.errors-{ssh_host}"))
        .status()
        .await?;
    if !status.success() {
        // anyhow::bail!("Command `rsync` exit with {status}")
        println!("Command `rsync` exit with {status}")
    }
    Ok(())
}
