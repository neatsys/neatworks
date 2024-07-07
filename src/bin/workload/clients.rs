use std::{future::Future, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use neatworks::{
    event::{
        task::{run_with_schedule, Context, ScheduleState},
        Erase, SendEvent, Untyped,
    },
    net::{
        combinators::{Forward, IndexNet},
        task::udp,
    },
    pbft::{self, PublicParameters},
    unreplicated,
    workload::events::{Invoke, InvokeOk},
};
use rand::random;
use tokio::{
    net::UdpSocket,
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
};

use super::util::run_until;

pub trait InvokeTask {
    fn run(
        self,
        sender: impl SendEvent<Invoke<Bytes>>,
        receiver: UnboundedReceiver<InvokeOk<Bytes>>,
    ) -> impl Future<Output = anyhow::Result<()>>;
}

pub async fn unreplicated(invoke_task: impl InvokeTask) -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("localhost:0").await?);
    let addr = socket.local_addr()?;
    let (upcall_sender, upcall_receiver) = unbounded_channel::<InvokeOk<_>>();
    let (schedule_sender, mut schedule_receiver) = unbounded_channel();
    let (sender, mut receiver) = unbounded_channel();

    let net_task = udp::run(
        &socket,
        unreplicated::codec::client_decode(Erase::new(sender.clone())),
    );

    let mut context = unreplicated::context::Client::<Context, _, _, _> {
        net: unreplicated::codec::client_encode(Forward(
            ([127, 0, 0, 1], 3000).into(),
            socket.clone(),
        )),
        upcall: upcall_sender,
        schedule: Erase::new(ScheduleState::new(schedule_sender)),
        _m: Default::default(),
    };
    let client_task = run_with_schedule(
        Untyped::new(unreplicated::ClientState::new(random(), addr)),
        &mut context,
        &mut receiver,
        &mut schedule_receiver,
        |context| &mut *context.schedule,
    );

    run_until(
        invoke_task.run(Erase::new(sender), upcall_receiver),
        async {
            select! {
                result = net_task => result,
                result = client_task => result,
            }
        },
    )
    .await
}

pub async fn pbft(
    invoke_task: impl InvokeTask,
    config: PublicParameters,
    replica_addrs: Vec<SocketAddr>,
) -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("localhost:0").await?);
    let addr = socket.local_addr()?;
    let (upcall_sender, upcall_receiver) = unbounded_channel::<InvokeOk<_>>();
    let (schedule_sender, mut schedule_receiver) = unbounded_channel();
    let (sender, mut receiver) = unbounded_channel();

    let net_task = udp::run(
        &socket,
        pbft::messages::codec::to_client_decode(Erase::new(sender.clone())),
    );

    let mut context = pbft::client::context::Context::<Context, _, _, _> {
        net: pbft::messages::codec::to_replica_encode(IndexNet::new(
            replica_addrs,
            None,
            socket.clone(),
        )),
        upcall: upcall_sender,
        schedule: Erase::new(ScheduleState::new(schedule_sender)),
        _m: Default::default(),
    };
    let client_task = run_with_schedule(
        Untyped::new(pbft::client::State::new(random(), addr, config)),
        &mut context,
        &mut receiver,
        &mut schedule_receiver,
        |context| &mut *context.schedule,
    );

    run_until(
        invoke_task.run(Erase::new(sender), upcall_receiver),
        async {
            select! {
                result = net_task => result,
                result = client_task => result,
            }
        },
    )
    .await
}
