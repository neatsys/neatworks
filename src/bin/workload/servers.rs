use std::{net::SocketAddr, sync::Arc};

use neatworks::{
    crypto::{Crypto, CryptoFlavor},
    event::{
        task::{erase::Of, run, run_with_schedule, run_worker, ScheduleState},
        Erase, Untyped,
    },
    net::{combinators::IndexNet, task::udp},
    pbft, unreplicated,
    workload::Null,
};
use tokio::{net::UdpSocket, select, sync::mpsc::unbounded_channel};

pub async fn unreplicated() -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("localhost:3000").await?);
    let (sender, mut receiver) = unbounded_channel();

    let net_task = udp::run(
        &socket,
        unreplicated::codec::server_decode(Erase::new(sender)),
    );
    let mut context = unreplicated::context::Server {
        net: unreplicated::codec::server_encode(socket.clone()),
    };
    let server_task = run(
        Untyped::new(unreplicated::ServerState::new(Null)),
        &mut context,
        &mut receiver,
    );
    select! {
        result = net_task => result?,
        result = server_task => result?,
    }
    anyhow::bail!("unexpected termination of infinite task")
}

pub async fn pbft(
    config: pbft::PublicParameters,
    index: usize,
    addrs: Vec<SocketAddr>,
) -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind(addrs[index]).await?);

    let (crypto_sender, mut crypto_receiver) = unbounded_channel();
    let (schedule_sender, mut schedule_receiver) = unbounded_channel();
    let (sender, mut receiver) = unbounded_channel();

    let mut context = pbft::replica::context::Context::<_, _, Of<_>, _> {
        peer_net: pbft::messages::codec::to_replica_encode(IndexNet::new(
            addrs,
            index,
            socket.clone(),
        )),
        downlink_net: pbft::messages::codec::to_client_encode(socket.clone()),
        crypto_worker: crypto_sender,
        schedule: Erase::new(ScheduleState::new(schedule_sender)),
        _m: Default::default(),
    };
    let server_task = run_with_schedule(
        Untyped::new(pbft::replica::State::new(index as _, Null, config.clone())),
        &mut context,
        &mut receiver,
        &mut schedule_receiver,
        |context| &mut context.schedule,
    );
    let net_task = udp::run(
        &socket,
        pbft::messages::codec::to_replica_decode(Erase::new(sender.clone())),
    );
    let crypto = Crypto::new_hardcoded(config.num_replica, index, CryptoFlavor::Schnorrkel)?;
    let crypto_task = run_worker(crypto, Erase::new(sender), &mut crypto_receiver);

    select! {
        result = server_task => result?,
        result = net_task => result?,
        result = crypto_task => result?,
    }
    anyhow::bail!("unexpected termination of infinite task")
}
