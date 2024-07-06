mod workload {
    mod clients;
    mod servers;
    mod util;
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    Ok(())
}
