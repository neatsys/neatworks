use std::env::args;

use boson_control::{instance_sessions, terraform_output};
use tokio::process::Command;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arg = args().nth(1);
    let sync = arg.as_deref() == Some("sync");
    if sync {
        println!("Building boson artifact");
        let status = Command::new("cargo")
            .args([
                "build",
                "--profile",
                "artifact",
                "--features",
                "nitro-enclaves",
                "--bin",
                "boson",
            ])
            .status()
            .await?;
        anyhow::ensure!(status.success(), "Command `cargo build` exit with {status}");
    }

    let mut instances = terraform_output("microbench_quorum_instances").await?;
    instances.extend(terraform_output("quorum_instances").await?);
    instances.extend(terraform_output("mutex_instances").await?);
    instance_sessions(&instances, |host| host_session(host, sync)).await
}

async fn host_session(ssh_host: String, sync: bool) -> anyhow::Result<()> {
    let ssh_host = format!("ec2-user@{ssh_host}");
    // println!("{ssh_host}");
    if sync {
        let status = Command::new("rsync")
            .arg("-a")
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
