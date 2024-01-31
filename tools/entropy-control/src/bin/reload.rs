use std::{env::args, time::Duration};

use entropy_control::terraform_instances;
use tokio::{process::Command, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arg = args().nth(1);
    let sync = arg.as_deref() == Some("sync");
    if sync {
        println!("Building entropy artifact");
        let status = Command::new("cargo")
            .args([
                "--quiet",
                "build",
                "--profile",
                "artifact",
                "--package",
                "entropy",
            ])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `cargo build` exit with {status}")
        }
    }

    println!("Spawning host sessions");
    let mut sessions = JoinSet::new();
    for instance in terraform_instances().await? {
        sessions.spawn(async move { host_session(instance.public_dns, sync).await });
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

async fn host_session(ssh_host: String, sync: bool) -> anyhow::Result<()> {
    if sync {
        let status = Command::new("rsync")
            .arg("target/artifact/entropy")
            // .arg("target/debug/entropy")
            .arg(format!("{ssh_host}:"))
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `rsync` exit with {status}")
        }
    }
    let status = Command::new("ssh")
        .arg(ssh_host)
        .arg("pkill -x entropy; sleep 1; tmux new -d -s entropy \"./entropy 2> entropy.errors\"")
        // .arg("pkill -x entropy; sleep 1; tmux new -d -s entropy \"RUST_BACKTRACE=1 ./entropy 2> entropy.errors\"")
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("Command `entropy` exit with {status}")
    }
    Ok(())
}
