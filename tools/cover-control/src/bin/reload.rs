use std::env::args;

use tokio::{process::Command, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args1 = args().nth(1);
    let sync = args1.as_deref() == Some("sync");
    if sync {
        println!("Building cover artifact");
        let status = Command::new("cargo")
            .args([
                "build",
                "--profile",
                "artifact",
                "--features",
                "nitro-enclaves",
                "--bin",
                "cover",
            ])
            .status()
            .await?;
        anyhow::ensure!(status.success(), "Command `cargo build` exit with {status}");
    }

    let output = cover_control::terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output
        .regions
        .values()
        .flat_map(|region| {
            region
                .mutex
                .iter()
                .chain(&region.cops)
                .chain(&region.cops_client)
                .chain(&region.quorum)
        })
        .chain(&output.microbench_quorum)
    {
        sessions.spawn(instance_session(
            format!("ec2-user@{}", instance.public_dns),
            sync,
        ));
    }

    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}

async fn instance_session(ssh_host: String, sync: bool) -> anyhow::Result<()> {
    // println!("{ssh_host}");
    if sync {
        let status = Command::new("rsync")
            .arg("-az")
            .arg("target/artifact/cover")
            // .arg("target/debug/cover")
            .arg(format!("{ssh_host}:"))
            .status()
            .await?;
        anyhow::ensure!(status.success(), "Command `rsync` exit with {status}");
    }
    cover_control::ssh(
        ssh_host,
        "pkill -x cover; sleep 1; tmux new -d -s cover \"./cover >cover.log\"",
        // "pkill -x cover; sleep 1; tmux new -d -s cover \"RUST_LOG=info,augustus::lamport_mutex::verifiable=debug ./cover >cover.log\""
        // "pkill -x cover; sleep 1; tmux new -d -s cover \"RUST_BACKTRACE=1 ./cover >cover.log\""
    )
    .await
}
