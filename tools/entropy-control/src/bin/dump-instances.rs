use std::collections::HashMap;

use entropy_control::terraform_instances;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let instances = terraform_instances().await?;
    let mut region_instances = HashMap::<_, Vec<_>>::new();
    for instance in instances {
        region_instances
            .entry(instance.region())
            .or_default()
            .push(instance.clone())
    }
    for (region, instances) in region_instances {
        println!("{region:?}");
        for instance in instances {
            println!("  {instance:?}")
        }
    }
    Ok(())
}
