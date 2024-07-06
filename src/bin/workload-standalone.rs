use std::{
    env::args,
    time::{Duration, Instant},
};

use neatworks::workload::events::Invoke;
use tokio::{select, time::sleep};

pub mod workload {
    pub mod clients;
    pub mod servers;
    mod util;
}

struct InvokeTask;

impl workload::clients::InvokeTask for InvokeTask {
    async fn run(
        self,
        mut sender: impl neatworks::event::SendEvent<neatworks::workload::events::Invoke<bytes::Bytes>>,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<
            neatworks::workload::events::InvokeOk<bytes::Bytes>,
        >,
    ) -> anyhow::Result<()> {
        for _ in 0..10 {
            let start = Instant::now();
            sender.send(Invoke(Default::default()))?;
            let recv = receiver.recv().await;
            anyhow::ensure!(recv.is_some());
            println!("{:?}", start.elapsed())
        }
        anyhow::Ok(())
    }
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
                    workload::clients::unreplicated(InvokeTask).await
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
