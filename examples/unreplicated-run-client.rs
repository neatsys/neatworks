use std::sync::Arc;

use bytes::Bytes;
use neatworks::{
    event::{
        task::{run_with_schedule, ScheduleState},
        Erase, Erased,
    },
    net::{combinators::Forward, task::udp},
    unreplicated,
    workload::events::InvokeOk,
};
use rand::random;
use tokio::{net::UdpSocket, sync::mpsc::unbounded_channel};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("localhost:0").await?);
    let addr = socket.local_addr()?;
    let (upcall_sender, mut upcall_receiver) = unbounded_channel::<InvokeOk<Bytes>>();
    let (schedule_sender, mut schedule_receiver) = unbounded_channel();
    let mut context = unreplicated::context::Client {
        net: unreplicated::codec::client_encode(Forward(
            ([127, 0, 0, 1], 3000).into(),
            socket.clone(),
        )),
        upcall: upcall_sender,
        schedule: Erase::new(ScheduleState::new(schedule_sender)),
    };
    let (sender, mut receiver) = unbounded_channel();
    let net_task = udp::run(
        &socket,
        unreplicated::codec::client_decode(Erase::new(sender)),
    );
    let client_task = run_with_schedule(
        Erased::new(unreplicated::ClientState::new(random(), addr)),
        &mut context,
        &mut receiver,
        &mut schedule_receiver,
        |context| &mut *context.schedule,
    );
    Ok(())
}
