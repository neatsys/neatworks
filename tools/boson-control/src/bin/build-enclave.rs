use std::process::Stdio;

use boson_control::terraform_output;
use tokio::{io::AsyncWriteExt, process::Command};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let host = terraform_output().await?.microbench.remove(0).public_dns;
    println!("* Working on microbench instance {host}");
    let host = format!("ec2-user@{host}");

    println!("* Install Nitro CLI");
    let status = Command::new("ssh")
        .arg(&host)
        .arg(
            String::from("sudo dnf install -y docker-24.0.5-1.amzn2023.0.3 aws-nitro-enclaves-cli aws-nitro-enclaves-cli-devel")
            + " && sudo usermod -aG ne ec2-user"
            + " && sudo usermod -aG docker ec2-user"
            // + " && sudo systemctl enable --now nitro-enclaves-allocator.service"
            + " && sudo systemctl enable --now docker"
        )
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* Build artifact");
    let status = Command::new("cargo")
        .args([
            "build",
            "--target",
            "x86_64-unknown-linux-musl",
            "--profile",
            "artifact",
            "--features",
            "nitro-enclaves,tikv-jemallocator",
            "--example",
            "nitro-enclaves-clock-validator",
        ])
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* Sync artifact");
    let status = Command::new("rsync")
        .arg("target/x86_64-unknown-linux-musl/artifact/examples/nitro-enclaves-clock-validator")
        .arg(format!("{host}:"))
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* Cat Dockerfile to remote");
    let mut child = Command::new("ssh")
        .args([&host, "-T"])
        .arg("cat > Dockerfile")
        .stdin(Stdio::piped())
        .spawn()?;
    let mut input = child.stdin.take().unwrap();
    input
        .write_all(
            [
                "FROM alpine:latest",
                "COPY nitro-enclaves-clock-validator .",
                "CMD [\"./nitro-enclaves-clock-validator\"]",
            ]
            .join("\n")
            .as_bytes(),
        )
        .await?;
    drop(input);
    let status = child.wait().await?;
    anyhow::ensure!(status.success());

    println!("* Build enclave image file");
    let status = Command::new("ssh")
        .arg(&host)
        .arg(
            String::from("docker build . -t app")
                + " && nitro-cli build-enclave --docker-uri app:latest --output-file app.eif",
        )
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* Sync back built enclave image file");
    let status = Command::new("rsync")
        .arg(format!("{host}:app.eif"))
        .arg(".")
        .status()
        .await?;
    anyhow::ensure!(status.success());
    Ok(())
}
