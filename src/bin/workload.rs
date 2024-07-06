pub mod workload {
    pub mod clients;
    pub mod servers;
    pub mod util;
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    Ok(())
}
