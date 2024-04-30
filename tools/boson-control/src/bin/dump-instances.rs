use std::{collections::BTreeMap, env::args};

use boson_control::terraform_output;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let instances = terraform_output(&if let Some(name) = args().nth(1) {
        name + "_instances"
    } else {
        String::from("instances")
    })
    .await?;
    let mut region_instances = BTreeMap::<_, Vec<_>>::new();
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
