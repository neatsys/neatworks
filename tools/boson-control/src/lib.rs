use std::{collections::BTreeMap, net::IpAddr};

use serde::Deserialize;
use tokio::process::Command;

#[derive(Debug, Clone, Deserialize)]
pub struct Instance {
    pub public_ip: IpAddr,
    pub private_ip: IpAddr,
    pub public_dns: String,
}

impl Instance {
    pub fn url(&self) -> String {
        format!("http://{}:3000", self.public_dns)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TerraformOutput {
    pub microbench: Vec<Instance>,
    pub microbench_quorum: Vec<Instance>,
    pub regions: BTreeMap<String, TerraformOutputRegion>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TerraformOutputRegion {
    pub mutex: Vec<Instance>,
    pub cops: Vec<Instance>,
    pub cops_client: Vec<Instance>,
    pub quorum: Vec<Instance>,
}

pub async fn terraform_output() -> anyhow::Result<TerraformOutput> {
    let output = Command::new("terraform")
        .args([
            "-chdir=tools/boson-control/terraform",
            "output",
            "-json",
            "instances",
        ])
        .output()
        .await?
        .stdout;
    Ok(serde_json::from_slice(&output)?)
}

impl Instance {
    pub fn region(&self) -> Option<String> {
        self.public_dns.split('.').nth(1).map(ToString::to_string)
    }
}

pub async fn ssh(host: impl AsRef<str>, command: impl AsRef<str>) -> anyhow::Result<()> {
    let status = Command::new("ssh")
        .arg(host.as_ref())
        .arg(command.as_ref())
        .stdout(std::process::Stdio::null())
        .status()
        .await?;
    anyhow::ensure!(status.success());
    Ok(())
}
