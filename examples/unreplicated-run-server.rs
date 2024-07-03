use std::{future::Future, sync::Arc};

use neatworks::{
    event::{task::run, Erase, Erased},
    net::task::udp,
    unreplicated,
};
use tokio::{net::UdpSocket, select, signal::ctrl_c, sync::mpsc::unbounded_channel};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("localhost:3000").await?);
    let mut context = unreplicated::context::Server {
        net: unreplicated::codec::server_encode(socket.clone()),
    };
    let (sender, mut receiver) = unbounded_channel();
    let net_task = udp::run(
        &socket,
        unreplicated::codec::server_decode(Erase::new(sender)),
    );
    let server_task = run(
        Erased::new(unreplicated::ServerState::new()),
        &mut context,
        &mut receiver,
    );
    run_until_interrupted(async {
        select! {
            result = net_task => result,
            result = server_task => result,
        }
    })
    .await
}

async fn run_until_interrupted(
    task: impl Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let interrupt_task = async {
        ctrl_c().await?;
        println!();
        anyhow::Ok(())
    };
    select! {
        result = task => result?,
        result = interrupt_task => return result,
    }
    anyhow::bail!("unexpected termination of forever task")
}
