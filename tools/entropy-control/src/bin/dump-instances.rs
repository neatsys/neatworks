use entropy_control::terraform_instances;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    println!("{:?}", terraform_instances().await);
    Ok(())
}
