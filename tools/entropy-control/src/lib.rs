use std::{collections::HashMap, net::IpAddr};

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

#[derive(Debug, Clone, Deserialize)]
pub struct TerraformOutputInstance {
    pub public_ip: IpAddr,
    pub private_ip: IpAddr,
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

impl TerraformOutputInstance {
    pub fn region(&self) -> Option<String> {
        self.public_dns.split('.').nth(1).map(ToString::to_string)
    }
}

pub fn retain_instances(
    instances: &[TerraformOutputInstance],
    num_per_region: usize,
) -> Vec<TerraformOutputInstance> {
    let mut region_instances = HashMap::<_, Vec<_>>::new();
    for instance in instances {
        let instances = region_instances.entry(instance.region()).or_default();
        if instances.len() < num_per_region {
            instances.push(instance.clone())
        }
    }
    region_instances.into_values().collect::<Vec<_>>().concat()
}
