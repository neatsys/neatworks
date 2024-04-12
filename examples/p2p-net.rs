use std::{env::args, net::SocketAddr};

use augustus::{
    crypto::peer::{Crypto, Verifiable},
    event::{
        erased::{
            session::{Buffered, Sender},
            Blanket, Session,
        },
        SendEvent, SendEventOnce,
    },
    kademlia::{
        Buckets, CryptoWorker, FindPeer, FindPeerOk, Peer, PeerId, PeerRecord, Query,
        SendCryptoEvent,
    },
    net::{
        events::Recv,
        kademlia::{Control, Multicast, PeerNet},
        session::Udp,
        SendMessage,
    },
    worker::{Submit, Worker},
};
use bincode::Options;
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::{net::UdpSocket, spawn, sync::oneshot};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
enum Message {
    FindPeer(Verifiable<FindPeer<SocketAddr>>),
    FindPeerOk(Verifiable<FindPeerOk<SocketAddr>>),
    #[from(ignore)]
    Hello(PeerId),
    HelloOk,
    #[from(ignore)]
    Join(PeerId),
}

type MessageNet<T> = augustus::net::MessageNet<T, Message>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let addr = socket.local_addr()?;
    println!("SocketAddr {addr}");
    let socket_net = Udp(socket.into());

    let mut peer_session = Session::new();
    let mut control_session = Session::new();
    let peer_id;
    let mut peer;
    let mut send_hello = None;

    let seed_crypto = Crypto::new_random(&mut StdRng::seed_from_u64(117418));
    let (bootstrapped_sender, bootstrapped_receiver) = oneshot::channel::<()>();
    if let Some(seed_addr) = args().nth(1) {
        let crypto = Crypto::new_random(&mut thread_rng());
        let peer_record = PeerRecord::new(crypto.public_key(), addr);
        peer_id = peer_record.id;
        println!("PeerId {peer_id}");

        let mut buckets = Buckets::new(peer_record);
        let seed_peer = PeerRecord::new(seed_crypto.public_key(), seed_addr.parse()?);
        send_hello = Some(seed_peer.id);
        buckets.insert(seed_peer)?;
        peer = Peer::new(
            buckets,
            MessageNet::new(socket_net.clone()),
            Sender::from(control_session.sender()),
            Box::new(CryptoWorker::from(Worker::Inline(
                crypto,
                Sender::from(peer_session.sender()),
            )))
                as Box<dyn Submit<Crypto, dyn SendCryptoEvent<SocketAddr>> + Send + Sync>,
        );
        peer.bootstrap(bootstrapped_sender)?
    } else {
        let peer_record = PeerRecord::new(seed_crypto.public_key(), addr);
        peer_id = peer_record.id;
        println!("SEED PeerId {peer_id}");

        let buckets = Buckets::new(peer_record);
        peer = Peer::new(
            buckets,
            MessageNet::new(socket_net.clone()),
            Sender::from(control_session.sender()),
            Box::new(CryptoWorker::from(Worker::Inline(
                seed_crypto,
                Sender::from(peer_session.sender()),
            ))),
        );
        bootstrapped_sender.send_once(())? // skip bootstrap on seed peer
    }

    let mut peer_net = PeerNet(Sender::from(control_session.sender()));
    let hello_session = spawn({
        let mut peer_net = peer_net.clone();
        async move {
            bootstrapped_receiver.await.unwrap();
            println!("Bootstrap finished");
            if let Some(seed_id) = send_hello {
                peer_net.send(seed_id, Message::Hello(peer_id)).unwrap()
            }
        }
    });

    let mut peer_sender = Sender::from(peer_session.sender());
    let socket_session = socket_net.recv_session(|buf| {
        let message = bincode::options()
            .allow_trailing_bytes()
            .deserialize::<Message>(buf)?;
        match message {
            Message::FindPeer(message) => peer_sender.send(Recv(message))?,
            Message::FindPeerOk(message) => peer_sender.send(Recv(message))?,
            Message::Hello(peer_id) => {
                println!("Replying Hello from {peer_id}");
                peer_net.send(peer_id, Message::HelloOk)?;
                peer_net.send(
                    Multicast(peer_id, 3.try_into().unwrap()),
                    Message::Join(peer_id),
                )?
            }
            Message::HelloOk => {
                println!("Received HelloOk")
            }
            Message::Join(peer_id) => {
                println!("Joining peer {peer_id}")
            }
        }
        Ok(())
    });

    let mut control = Blanket(Buffered::from(Control::new(
        MessageNet::new(socket_net.clone()),
        Box::new(Sender::from(peer_session.sender())) as Box<dyn SendEvent<Query> + Send + Sync>,
    )));
    let mut peer = Blanket(Buffered::from(peer));
    let peer_session = peer_session.run(&mut peer);
    let control_session = control_session.run(&mut control);
    tokio::select! {
        result = socket_session => result?,
        result = peer_session => result?,
        result = control_session => result?,
        Err(err) = hello_session => Err(err)?,
    }
    Err(anyhow::anyhow!("unreachable"))
}

// cSpell:words kademlia bincode seedable
