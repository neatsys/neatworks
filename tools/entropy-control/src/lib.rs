use std::net::IpAddr;

use serde::Deserialize;
use tokio::process::Command;

#[derive(Deserialize)]
struct TerraformOutput {
    instances: TerraformOutputInstances,
}

#[derive(Deserialize)]
struct TerraformOutputInstances {
    value: Vec<TerraformOutputInstance>,
}

#[derive(Debug, Deserialize)]
pub struct TerraformOutputInstance {
    pub public_ip: IpAddr,
    pub public_dns: String,
}

pub async fn terraform_instances() -> anyhow::Result<Vec<TerraformOutputInstance>> {
    let output = Command::new("terraform")
        .args(["-chdir=tools/entropy-control/terraform", "output", "--json"])
        .output()
        .await?
        .stdout;
    let instances = serde_json::from_slice::<TerraformOutput>(&output)?
        .instances
        .value;
    Ok(instances)
}
