use std::env::args;

use boson_control::{instance_sessions, terraform_output};
use tokio::process::Command;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arg = args().nth(1);
    let is_cops = arg.as_deref() == Some("cops");
    let mut instances = terraform_output("cops_instances").await?;
    if !is_cops {
        instances.extend(terraform_output("mutex_instances").await?);
        instances.extend(terraform_output("microbench_instances").await?);
    }
    instance_sessions(&instances, session).await
}

async fn session(host: String) -> anyhow::Result<()> {
    let arg = args().nth(1);
    let is_cops = arg.as_deref() == Some("cops");
    let host = format!("ec2-user@{host}");
    let status = Command::new("rsync")
        .arg("-az")
        .arg("app.eif")
        .arg(format!("{host}:"))
        .status()
        .await?;
    anyhow::ensure!(status.success());
    let status = Command::new("ssh")
        .arg(&host)
        .arg(if is_cops {
            "nitro-cli run-enclave --cpu-count 24 --memory 2048 --enclave-cid 16 --eif-path app.eif"
        } else {
            "nitro-cli run-enclave --cpu-count 2 --memory 2048 --enclave-cid 16 --eif-path app.eif"
        })
        .status()
        .await?;
    anyhow::ensure!(status.success(), "{host}");
    Ok(())
}
