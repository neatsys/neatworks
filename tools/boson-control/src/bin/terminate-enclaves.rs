use boson_control::{instance_sessions, terraform_output};
use tokio::process::Command;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut instances = terraform_output("microbench_instances").await?;
    instances.extend(terraform_output("mutex_instances").await?);
    instance_sessions(&instances, session).await
}

async fn session(host: String) -> anyhow::Result<()> {
    let host = format!("ec2-user@{host}");
    let status = Command::new("ssh")
        .arg(host)
        .arg("nitro-cli terminate-enclave --enclave-name app")
        .status()
        .await?;
    anyhow::ensure!(status.success());
    Ok(())
}
