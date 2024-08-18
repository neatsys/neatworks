use std::{net::SocketAddr, sync::Arc};

use neatworks::{
    codec::Encode,
    crypto::{Crypto, CryptoFlavor},
    event::{
        task::{self, run, run_with_schedule, run_worker, ScheduleState},
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

    type Net = Encode<unreplicated::Reply, Arc<UdpSocket>>;
    struct Context(Net);
    impl unreplicated::ServerContext<SocketAddr> for Context {
        type Net = Net;
        fn net(&mut self) -> &mut Self::Net {
            &mut self.0
        }
    }
    let mut context = Context(unreplicated::codec::server_encode(socket.clone()));
    let server_task = run(
        Untyped::new(unreplicated::ServerState::new(Null)),
        &mut context,
        &mut receiver,
    );
    let net_task = udp::run(
        &socket,
        unreplicated::codec::server_decode(Erase::new(sender)),
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

    type S = pbft::replica::State<Null, SocketAddr>;
    type PeerNet =
        Encode<pbft::messages::codec::ToReplica<SocketAddr>, IndexNet<SocketAddr, Arc<UdpSocket>>>;
    type DownlinkNet = Encode<pbft::messages::codec::ToClient, Arc<UdpSocket>>;
    type CryptoWorker = task::work::Sender<Crypto, CryptoContext>;
    type CryptoContext = task::erase::Sender<S, Context>;
    type Schedule = task::erase::ScheduleState<S, Context>;
    struct Context {
        peer_net: PeerNet,
        downlink_net: DownlinkNet,
        crypto_worker: CryptoWorker,
        schedule: Schedule,
    }
    impl pbft::replica::Context<S, SocketAddr> for Context {
        type PeerNet = PeerNet;
        type DownlinkNet = DownlinkNet;
        type CryptoWorker = CryptoWorker;
        type CryptoContext = CryptoContext;
        type Schedule = Schedule;
        fn peer_net(&mut self) -> &mut Self::PeerNet {
            &mut self.peer_net
        }
        fn downlink_net(&mut self) -> &mut Self::DownlinkNet {
            &mut self.downlink_net
        }
        fn crypto_worker(&mut self) -> &mut Self::CryptoWorker {
            &mut self.crypto_worker
        }
        fn schedule(&mut self) -> &mut Self::Schedule {
            &mut self.schedule
        }
    }
    let mut context = Context {
        peer_net: pbft::messages::codec::to_replica_encode(IndexNet::new(
            addrs,
            index,
            socket.clone(),
        )),
        downlink_net: pbft::messages::codec::to_client_encode(socket.clone()),
        crypto_worker: crypto_sender,
        schedule: Erase::new(ScheduleState::new(schedule_sender)),
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
    let crypto_task = run_worker(
        Crypto::new_hardcoded(config.num_replica, index, CryptoFlavor::Schnorrkel)?,
        Erase::new(sender),
        &mut crypto_receiver,
    );

    select! {
        result = server_task => result?,
        result = net_task => result?,
        result = crypto_task => result?,
    }
    anyhow::bail!("unexpected termination of infinite task")
}
