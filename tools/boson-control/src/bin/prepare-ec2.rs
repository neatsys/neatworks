use std::env::args;

use tokio::task::JoinSet;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args1 = args().nth(1);
    let is_cops = args1.as_deref() == Some("cops");

    let command = String::from("sudo dnf install -y tmux htop");
    let enclave_command = String::from(
        "sudo dnf install -y tmux htop docker-24.0.5-1.amzn2023.0.3 aws-nitro-enclaves-cli",
    ) + " && sudo usermod -aG ne ec2-user"
        + " && sudo usermod -aG docker ec2-user"
        + " && sudo systemctl restart docker"
        + if is_cops {
            " && echo -e \"---\nmemory_mib: 2048\\ncpu_count: 24\" | sudo tee /etc/nitro_enclaves/allocator.yaml"
        } else {
            " && echo -e \"---\nmemory_mib: 2048\\ncpu_count: 4\" | sudo tee /etc/nitro_enclaves/allocator.yaml"
        }
        + " && sudo systemctl restart nitro-enclaves-allocator.service"
        + " && sudo sysctl -w net.core.somaxconn=20000";

    let output = boson_control::terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output
        .regions
        .values()
        .flat_map(|region| region.mutex.iter().chain(&region.cops))
    {
        sessions.spawn(boson_control::ssh(
            format!("ec2-user@{}", instance.public_dns),
            enclave_command.clone(),
        ));
    }
    for instance in output
        .regions
        .values()
        .flat_map(|region| region.quorum.iter().chain(&region.cops_client))
        .chain(&output.microbench_quorum)
    {
        sessions.spawn(boson_control::ssh(
            format!("ec2-user@{}", instance.public_dns),
            command.clone(),
        ));
    }

    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}
