use std::{env::args, process::Stdio, time::Duration};

use serde::Deserialize;
use tokio::{process::Command, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let ip = args().nth(1);
    let ip = ip.as_deref().unwrap_or("127.0.0.1");
    let mut seed_addr = args().nth(2);

    let status = Command::new("ipfs").arg("--version").status().await?;
    if !status.success() {
        let status = Command::new("curl")
            .args([
                "--silent",
                "--remote-name",
                "https://dist.ipfs.tech/kubo/v0.26.0/kubo_v0.26.0_linux-amd64.tar.gz",
            ])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `curl` exit with {status}")
        }
        let status = Command::new("tar")
            .args(["-xf", "kubo_v0.26.0_linux-amd64.tar.gz"])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `tar` exit with {status}")
        }
        let status = Command::new("move")
            .args(["kubo/ipfs", ".local/bin"])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `mv` exit with {status}")
        }
    }

    struct Ipfs(usize);
    impl Ipfs {
        fn run<T>(&self, f: impl FnOnce(&mut Command) -> T) -> T {
            f(Command::new("ipfs").env("IPFS_PATH", format!("/tmp/ipfs-{}", self.0)))
        }
    }
    for i in 0..100 {
        let ipfs = Ipfs(i);
        let status = ipfs
            .run(|command| {
                command
                    .args(["init", "--profile", "server"])
                    .stdout(Stdio::null())
                    .status()
            })
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs init` exit with {status}")
        }
        let swarm_addrs = serde_json::to_string(&[format!("/ip4/{ip}/tcp/{}", 4000 + i)])?;
        let status = ipfs
            .run(|command| {
                command
                    .arg("config")
                    .arg("Addresses.Swarm")
                    .arg("--json")
                    .arg(swarm_addrs)
                    .status()
            })
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs config Addresses.Swarm` exit with {status}")
        }
        let api_addr = serde_json::to_string(&format!("/ip4/{ip}/tcp/{}", 5000 + i))?;
        let status = ipfs
            .run(|command| {
                command
                    .arg("config")
                    .arg("Addresses.API")
                    .arg("--json")
                    .arg(api_addr)
                    .status()
            })
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs config Addresses.API` exit with {status}")
        }
        let gateway_addr = serde_json::to_string(&format!("/ip4/{ip}/tcp/{}", 8000 + i))?;
        let status = ipfs
            .run(|command| {
                command
                    .arg("config")
                    .arg("Addresses.Gateway")
                    .arg("--json")
                    .arg(gateway_addr)
                    .status()
            })
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs config Addresses.Gateway` exit with {status}")
        }

        let status = ipfs
            .run(|command| {
                command
                    .args(["bootstrap", "rm", "all"])
                    .stdout(Stdio::null())
                    .status()
            })
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs bootstrap rm all` exit with {status}")
        }
        if let Some(seed_addr) = &seed_addr {
            let status = ipfs
                .run(|command| {
                    command
                        .args(["bootstrap", "add", seed_addr])
                        .stdout(Stdio::null())
                        .status()
                })
                .await?;
            if !status.success() {
                anyhow::bail!("Command `ipfs bootstrap add` exit with {status}")
            }
        }

        let status = Command::new("tmux")
            .arg("new")
            .arg("-d")
            .arg("-s")
            .arg(format!("ipfs-{i}"))
            .arg(format!("IPFS_PATH=/tmp/ipfs-{i} ipfs daemon"))
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `ipfs daemon` exit with {status}")
        }
        if seed_addr.is_none() {
            sleep(Duration::from_millis(100)).await;
            let out = ipfs.run(|command| command.arg("id").output()).await?.stdout;
            // println!("{:?}", std::str::from_utf8(&out));
            #[allow(non_snake_case)]
            #[derive(Deserialize)]
            struct Out {
                Addresses: Vec<String>,
            }
            seed_addr = Some(
                serde_json::from_slice::<Out>(&out)?
                    .Addresses
                    .into_iter()
                    .next()
                    .ok_or(anyhow::anyhow!("seed peer has no address"))?,
            );
            println!("Seed address {seed_addr:?}")
        }
    }

    Ok(())
}
