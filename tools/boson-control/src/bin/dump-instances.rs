#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let output = boson_control::terraform_output().await?;
    println!("{output:#?}");
    Ok(())
}
