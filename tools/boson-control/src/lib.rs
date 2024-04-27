use std::{collections::HashMap, future::Future, net::IpAddr};

use serde::Deserialize;
use tokio::{process::Command, task::JoinSet};

#[derive(Debug, Clone, Deserialize)]
pub struct TerraformOutputInstance {
    pub public_ip: IpAddr,
    pub private_ip: IpAddr,
    pub public_dns: String,
}

pub async fn terraform_output(name: &str) -> anyhow::Result<Vec<TerraformOutputInstance>> {
    let output = Command::new("terraform")
        .args([
            "-chdir=tools/boson-control/terraform",
            "output",
            "-json",
            name,
        ])
        .output()
        .await?
        .stdout;
    let instances = serde_json::from_slice::<Vec<TerraformOutputInstance>>(&output)?;
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

pub async fn instance_sessions<F: Future<Output = anyhow::Result<()>> + Send + 'static>(
    instances: &[TerraformOutputInstance],
    session: impl Fn(String) -> F,
) -> anyhow::Result<()> {
    let mut sessions = JoinSet::new();
    for instance in instances {
        sessions.spawn(session(instance.public_dns.clone()));
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }
    Ok(())
}
