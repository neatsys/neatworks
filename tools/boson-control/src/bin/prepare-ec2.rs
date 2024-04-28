use std::process::Stdio;

use boson_control::{instance_sessions, terraform_output};
use tokio::process::Command;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let instances = terraform_output("microbench_quorum_instances").await?;
    // TODO extend other quorums
    instance_sessions(&instances, |host| async move {
        let host = format!("ec2-user@{host}");
        let status = Command::new("ssh")
            .arg(&host)
            .arg("sudo dnf install -y tmux")
            .stdout(Stdio::null())
            .status()
            .await?;
        anyhow::ensure!(status.success());
        Ok(())
    })
    .await?;
    // TODO install tmux and nitro cli on mutex/cops instances
    Ok(())
}
