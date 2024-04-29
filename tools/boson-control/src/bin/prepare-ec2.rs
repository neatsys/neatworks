use std::process::Stdio;

use boson_control::{instance_sessions, terraform_output};
use tokio::process::Command;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut instances = terraform_output("microbench_quorum_instances").await?;
    instances.extend(terraform_output("quorum_instances").await?);
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

    let mut instances = terraform_output("microbench_instances").await?;
    instances.extend(terraform_output("mutex_instances").await?);
    // TODO extend cops instances
    instance_sessions(&instances, |host| async move {
        let host = format!("ec2-user@{host}");
        let status = Command::new("ssh")
            .arg(&host)
            .arg(
                String::from(
                    "sudo dnf install -y tmux docker-24.0.5-1.amzn2023.0.3 aws-nitro-enclaves-cli",
                ) + " && sudo usermod -aG ne ec2-user"
                    + " && sudo usermod -aG docker ec2-user"
                    + " && echo -e \"---\nmemory_mib: 2048\\ncpu_count: 4\" | sudo tee /etc/nitro_enclaves/allocator.yaml"
                    + " && sudo systemctl restart nitro-enclaves-allocator.service"
                    + " && sudo systemctl restart docker",
            )
            .stdout(Stdio::null())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "{host}");
        Ok(())
    })
    .await?;

    Ok(())
}
