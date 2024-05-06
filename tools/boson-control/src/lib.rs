use std::{collections::BTreeMap, future::Future, net::IpAddr};

use serde::Deserialize;
use tokio::{process::Command, task::JoinSet};

#[derive(Debug, Clone, Deserialize)]
pub struct Instance {
    pub public_ip: IpAddr,
    pub private_ip: IpAddr,
    pub public_dns: String,
}

pub async fn terraform_output(name: &str) -> anyhow::Result<Vec<Instance>> {
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
    let instances = serde_json::from_slice::<Vec<Instance>>(&output)?;
    Ok(instances)
}

impl Instance {
    pub fn region(&self) -> Option<String> {
        self.public_dns.split('.').nth(1).map(ToString::to_string)
    }
}

pub fn retain_instances(instances: &[Instance], num_per_region: usize) -> Vec<Instance> {
    let mut region_instances = BTreeMap::<_, Vec<_>>::new();
    for instance in instances {
        let instances = region_instances.entry(instance.region()).or_default();
        if instances.len() < num_per_region {
            instances.push(instance.clone())
        }
    }
    region_instances.into_values().collect::<Vec<_>>().concat()
}

pub async fn instance_sessions<F: Future<Output = anyhow::Result<()>> + Send + 'static>(
    instances: &[Instance],
    session: impl Fn(String) -> F,
) -> anyhow::Result<()> {
    let mut sessions = JoinSet::new();
    for instance in instances {
        sessions.spawn(session(instance.public_dns.clone()));
    }
    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}
