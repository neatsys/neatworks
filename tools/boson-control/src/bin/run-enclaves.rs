use std::env::args;

use tokio::{process::Command, task::JoinSet};

async fn instance_session(host: String, is_cops: bool) -> anyhow::Result<()> {
    let status = Command::new("rsync")
        .arg("app.eif")
        .arg(format!("{host}:"))
        .status()
        .await?;
    anyhow::ensure!(status.success());

    let command = if is_cops {
        "nitro-cli run-enclave --cpu-count 24 --memory 2048 --enclave-cid 16 --eif-path app.eif"
    } else {
        "nitro-cli run-enclave --cpu-count 2 --memory 2048 --enclave-cid 16 --eif-path app.eif"
    };
    boson_control::ssh(&host, command).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arg = args().nth(1);
    let is_cops = arg.as_deref() == Some("cops");

    let output = boson_control::terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output
        .regions
        .values()
        .flat_map(|region| region.mutex.iter().chain(&region.cops))
    {
        sessions.spawn(instance_session(
            format!("ec2-user@{}", instance.public_dns),
            is_cops,
        ));
    }

    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}
