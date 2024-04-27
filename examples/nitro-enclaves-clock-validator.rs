use augustus::boson::NitroSecureModule;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    NitroSecureModule::run().await
}
