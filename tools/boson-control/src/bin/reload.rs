use std::{env::args, time::Duration};

use boson_control::{terraform_instances, terraform_quorum_instances};
use tokio::{process::Command, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arg = args().nth(1);
    let sync = arg.as_deref() == Some("sync");
    if sync {
        println!("Building boson artifact");
        let status = Command::new("cargo")
            .args([
                "--quiet",
                "build",
                "--profile",
                "artifact",
                "--bin",
                "boson",
            ])
            .status()
            .await?;
        anyhow::ensure!(status.success(), "Command `cargo build` exit with {status}");
    }

    println!("Spawning host sessions");
    let mut sessions = JoinSet::new();
    for instance in terraform_instances().await? {
        sessions.spawn(async move { host_session(instance.public_dns, sync).await });
    }
    for instance in terraform_quorum_instances().await? {
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
            .arg("target/artifact/boson")
            // .arg("target/debug/boson")
            .arg(format!("{ssh_host}:"))
            .status()
            .await?;
        anyhow::ensure!(status.success(), "Command `rsync` exit with {status}");
    }
    let status = Command::new("ssh")
        .arg(ssh_host)
        .arg("pkill -x boson; sleep 1; tmux new -d -s boson \"./boson >boson.log\"")
        // .arg("pkill -x boson; sleep 1; tmux new -d -s boson \"RUST_LOG=info,augustus::lamport_mutex=debug ./boson >boson.log\"")
        // .arg("pkill -x boson; sleep 1; tmux new -d -s boson \"RUST_BACKTRACE=1 ./boson 2>&1 >boson.log\"")
        .status()
        .await?;
    anyhow::ensure!(status.success(), "Command `boson` exit with {status}");
    Ok(())
}
