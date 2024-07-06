use std::{
    env::args,
    time::{Duration, Instant},
};

use neatworks::{pbft::PublicParameters, workload::events::Invoke};
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
        sleep(Duration::from_millis(100)).await;
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
                let client_task = workload::clients::unreplicated(InvokeTask);
                select! {
                    result = client_task => break 'client result?,
                    result = server_task => result?,
                }
            }
            "pbft" => {
                let config = PublicParameters {
                    num_replica: 4,
                    num_faulty: 1,
                    num_concurrent: 1,
                    max_batch_size: 1,
                    ..PublicParameters::durations(if cfg!(debug_assertions) {
                        Duration::from_millis(300)
                    } else {
                        Duration::from_millis(100)
                    })
                };
                let addrs = (0..4)
                    .map(|index| ([127, 0, 0, 1 + index], 3000).into())
                    .collect::<Vec<_>>();
                let server_task0 = workload::servers::pbft(config.clone(), 0, addrs.clone());
                let server_task1 = workload::servers::pbft(config.clone(), 1, addrs.clone());
                let server_task2 = workload::servers::pbft(config.clone(), 2, addrs.clone());
                let server_task3 = workload::servers::pbft(config.clone(), 3, addrs.clone());
                let client_task = workload::clients::pbft(InvokeTask, config, addrs);
                select! {
                    result = client_task => break 'client result?,
                    result = server_task0 => result?,
                    result = server_task1 => result?,
                    result = server_task2 => result?,
                    result = server_task3 => result?,
                }
            }
            _ => anyhow::bail!("unimplemented"),
        }
        anyhow::bail!("unexpected termination of server task")
    }
    Ok(())
}
