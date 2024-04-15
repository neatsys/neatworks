use std::net::SocketAddr;

use augustus::{
    app::Detach,
    crypto::{Crypto, CryptoFlavor},
    event::{erased, OnEvent, OnTimer, SendEvent, Session, Unify},
    net::{session::Udp, All, IndexNet, InvokeNet, MessageNet, SendMessage},
    pbft,
    util::{BincodeDe, Queue},
    worker::{Submit, Worker},
    workload::InvokeOk,
};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

struct PingPong<N, const PING: bool>(N);

#[derive(Debug, Serialize, Deserialize)]
enum Message {
    Ping(usize),
    Pong(usize),
}

impl<N: SendMessage<All, Message>, const PING: bool> OnEvent for PingPong<N, PING> {
    type Event = Message;

    fn on_event(
        &mut self,
        event: Self::Event,
        _: &mut impl augustus::event::Timer,
    ) -> anyhow::Result<()> {
        match dbg!(event) {
            Message::Ping(n) if !PING => self.0.send(All, Message::Pong(n)),
            Message::Pong(n) if PING => {
                if n == 10 {
                    anyhow::bail!(Exit)
                }
                self.0.send(All, Message::Ping(n + 1))
            }
            _ => Ok(()),
        }
    }
}

impl<N, const PING: bool> OnTimer for PingPong<N, PING> {
    fn on_timer(
        &mut self,
        _: augustus::event::TimerId,
        _: &mut impl augustus::event::Timer,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}

impl<N: SendMessage<All, Message>, const PING: bool> PingPong<N, PING> {
    fn init(&mut self) -> anyhow::Result<()> {
        if PING {
            self.0.send(All, Message::Ping(0))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
struct Exit;

async fn ping_pong_session<const PING: bool>(index: usize) -> anyhow::Result<()> {
    let client_addr = SocketAddr::from(([127, 0, 0, 1], 5000 + index as u16));
    let replica_addrs = (0..2)
        .map(|i| SocketAddr::from(([127, 0, 0, 1], 4000 + i)))
        .collect::<Vec<_>>();
    let client_udp = Udp(UdpSocket::bind(client_addr).await?.into());
    let replica_udp = Udp(UdpSocket::bind(replica_addrs[index]).await?.into());

    let mut queue_session = erased::Session::new();
    let mut client_session = erased::Session::new();
    let mut replica_session = erased::Session::new();
    let mut session = Session::new();

    let mut queue = erased::Blanket(erased::Unify(Queue::new(erased::session::Sender::from(
        client_session.sender(),
    ))));
    let mut client = erased::Blanket(erased::Buffered::from(pbft::Client::new(
        index as _,
        client_addr,
        pbft::ToReplicaMessageNet::new(IndexNet::new(
            client_udp.clone(),
            replica_addrs.clone(),
            None,
        )),
        Box::new(erased::session::Sender::from(queue_session.sender()))
            as Box<dyn SendEvent<InvokeOk> + Send + Sync>,
        2,
        0,
    )));
    let mut replica = erased::Blanket(erased::Buffered::from(pbft::Replica::new(
        index as _,
        Detach(BincodeDe::<_, Message>::from(session.sender())),
        pbft::ToReplicaMessageNet::new(IndexNet::new(replica_udp.clone(), replica_addrs, index)),
        pbft::ToClientMessageNet::new(replica_udp.clone()),
        Box::new(pbft::CryptoWorker::from(Worker::Inline(
            Crypto::new_hardcoded_replication(2, index, CryptoFlavor::Plain)?,
            erased::session::Sender::from(replica_session.sender()),
        ))) as Box<dyn Submit<Crypto, dyn pbft::SendCryptoEvent<SocketAddr>> + Send + Sync>,
        2,
        0,
    )));
    let mut ping_pong = Unify(PingPong::<_, PING>(MessageNet::<_, Message>::new(
        InvokeNet(erased::session::Sender::from(queue_session.sender())),
    )));
    ping_pong.init()?;

    let mut client_sender = erased::session::Sender::from(client_session.sender());
    let client_udp_session =
        client_udp.recv_session(|buf| pbft::to_client_on_buf(buf, &mut client_sender));
    let mut replica_sender = erased::session::Sender::from(replica_session.sender());
    let replica_udp_session =
        replica_udp.recv_session(|buf| pbft::to_replica_on_buf(buf, &mut replica_sender));
    let queue_session = queue_session.run(&mut queue);
    let client_session = client_session.run(&mut client);
    let replica_session = replica_session.run(&mut replica);
    let session = session.run(&mut ping_pong);

    let result = tokio::select! {
        result = client_udp_session => result,
        result = replica_udp_session => result,
        result = queue_session => result,
        result = client_session => result,
        result = replica_session => result,
        result = session => result,
    };
    match result {
        Ok(()) => anyhow::bail!("unreachable"),
        Err(err) if err.is::<Exit>() => Ok(()),
        err => err,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio::spawn(ping_pong_session::<false>(0));
    ping_pong_session::<true>(1).await
}
