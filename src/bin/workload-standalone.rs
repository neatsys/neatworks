use std::{env::args, time::Duration};

use tokio::{select, time::sleep};

pub mod workload {
    pub mod clients;
    pub mod servers;
    mod util;
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mode = args().nth(1);
    'client: {
        match mode.as_deref().unwrap_or("unreplicated") {
            "unreplicated" => {
                let server_task = workload::servers::unreplicated();
                let client_task = async {
                    sleep(Duration::from_millis(100)).await;
                    workload::clients::unreplicated().await
                };
                select! {
                    result = client_task => break 'client result?,
                    result = server_task => result?,
                }
            }
            _ => anyhow::bail!("unimplemented"),
        }
        anyhow::bail!("unexpected termination of server task")
    }
    Ok(())
}
