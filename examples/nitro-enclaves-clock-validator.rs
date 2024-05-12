use augustus::cover::NitroSecureModule;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    NitroSecureModule::run().await
}
