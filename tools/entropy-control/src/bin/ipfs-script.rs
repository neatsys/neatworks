use std::{env::args, process::Stdio, time::Duration};

use serde::Deserialize;
use tokio::{fs::remove_dir_all, process::Command, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let command = args().nth(1);
    if command.as_deref() == Some("install") {
        let status = Command::new("tar")
            .args(["-xf", "kubo_v0.26.0_linux-amd64.tar.gz"])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("Command `tar` exit with {status}")
        }
        return Ok(());
    }
    struct Ipfs(usize);
    impl Ipfs {
        fn run<T>(&self, f: impl FnOnce(&mut Command) -> T) -> T {
            f(Command::new("./kubo/ipfs").env("IPFS_PATH", format!("/tmp/ipfs-{}", self.0)))
        }
    }
    if command.as_deref() == Some("start-peers") {
        let public_ip = args()
            .nth(2)
            .ok_or(anyhow::anyhow!("public ip is not specified"))?;
        let private_ip = args()
            .nth(3)
            .ok_or(anyhow::anyhow!("private ip is not specified"))?;
        let mut seed_addr = args().nth(4);
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
            let swarm_addrs = serde_json::to_string(&[
                format!("/ip4/{public_ip}/tcp/{}", 4000 + i),
                format!("/ip4/{private_ip}/tcp/{}", 4000 + i),
            ])?;
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
            let api_addr = serde_json::to_string(&format!("/ip4/0.0.0.0/tcp/{}", 5000 + i))?;
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
            let gateway_addr = serde_json::to_string(&format!("/ip4/127.0.0.1/tcp/{}", 8000 + i))?;
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
                .arg(format!("IPFS_PATH=/tmp/ipfs-{i} ./kubo/ipfs daemon"))
                .status()
                .await?;
            if !status.success() {
                anyhow::bail!("Command `ipfs daemon` exit with {status}")
            }
            while seed_addr.is_none() {
                sleep(Duration::from_millis(500)).await;
                let out = ipfs.run(|command| command.arg("id").output()).await?.stdout;
                // println!("{:?}", std::str::from_utf8(&out));
                #[allow(non_snake_case)]
                #[derive(Deserialize)]
                struct Out {
                    Addresses: Option<Vec<String>>,
                }
                let Some(addresses) = serde_json::from_slice::<Out>(&out)?.Addresses else {
                    eprintln!("Wait seed peer to obtain address");
                    continue;
                };
                let addr = addresses
                    .into_iter()
                    .next()
                    .ok_or(anyhow::anyhow!("seed peer has no address"))?;
                print!("{addr}");
                seed_addr = Some(addr);
                // println!("Seed address {seed_addr:?}")
            }
        }
        return Ok(());
    }
    if command.as_deref() == Some("stop-peers") {
        for i in 0..100 {
            let status = Ipfs(i)
                .run(|command| command.arg("shutdown").stderr(Stdio::null()).status())
                .await?;
            if status.success() {
                // println!("Interrupted previous IPFS peers")
            }
            let _ = remove_dir_all(format!("/tmp/ipfs-{i}")).await;
        }
        return Ok(());
    }
    Err(anyhow::anyhow!("unexpected command {command:?}"))
}
