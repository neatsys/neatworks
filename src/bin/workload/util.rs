use std::future::Future;

use tokio::select;

pub async fn run_until(
    task: impl Future<Output = anyhow::Result<()>>,
    background_task: impl Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    select! {
        result = background_task => result?,
        result = task => return result,
    }
    anyhow::bail!("unexpected termination of forever task")
}
