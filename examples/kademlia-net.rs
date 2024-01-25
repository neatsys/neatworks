use std::{env::args, net::SocketAddr};

use augustus::{
    crypto::Crypto,
    event::{SendEvent, Session},
    kademlia::{self, Buckets, Peer, PeerId},
    net::{
        kademlia::{Control, ControlEvent, Net},
        SendMessage, Udp,
    },
    worker::spawn_backend,
};
use bincode::Options;
use primitive_types::H256;
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Kademlia(kademlia::Message<SocketAddr>),
    Hello(PeerId),
    HelloOk,
}

struct MessageNet<T>(T);

impl<T: SendMessage<SocketAddr, Vec<u8>>, N> SendMessage<SocketAddr, N> for MessageNet<T>
where
    N: Into<kademlia::Message<SocketAddr>>,
{
    fn send(&mut self, dest: SocketAddr, message: N) -> anyhow::Result<()> {
        self.0.send(
            dest,
            bincode::options().serialize(&Message::Kademlia(message.into()))?,
        )
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let addr = socket.local_addr()?;
    println!("SocketAddr {addr}");
    let socket_net = Udp(socket.into());

    let mut control_session = Session::<ControlEvent<Message, SocketAddr>>::new();
    let (crypto_worker, mut crypto_session);
    let peer_id;
    let mut peer;
    let bootstrap_finished = CancellationToken::new();
    let mut send_hello = None;

    let seed_crypto = Crypto::new_random(&mut StdRng::seed_from_u64(117418));
    if let Some(seed_addr) = args().nth(1) {
        let crypto = Crypto::new_random(&mut thread_rng());
        let peer_record = crypto.peer(addr);
        peer_id = peer_record.id;
        println!("PeerId {}", H256::from_slice(&peer_id));
        (crypto_worker, crypto_session) = spawn_backend(crypto);

        let mut buckets = Buckets::new(peer_record);
        let seed_peer = seed_crypto.peer(seed_addr.parse()?);
        send_hello = Some(seed_peer.id);
        buckets.insert(seed_peer);
        peer = Peer::new(
            buckets,
            MessageNet(socket_net.clone()),
            control_session.sender(),
            crypto_worker,
        );
        let cancel = bootstrap_finished.clone();
        peer.bootstrap(Box::new(move || Ok(cancel.cancel())))?;
    } else {
        let peer_record = seed_crypto.peer(addr);
        peer_id = peer_record.id;
        println!("SEED PeerId {}", H256::from_slice(&peer_id));
        (crypto_worker, crypto_session) = spawn_backend(seed_crypto);

        let buckets = Buckets::new(peer_record);
        peer = Peer::new(
            buckets,
            MessageNet(socket_net.clone()),
            control_session.sender(),
            crypto_worker,
        );
        bootstrap_finished.cancel(); // skip bootstrap on seed peer
    }

    bootstrap_finished.cancelled().await;
    println!("Bootstrap finished");
    let mut peer_session = Session::<kademlia::Event<SocketAddr>>::new();
    let mut peer_sender = peer_session.sender();
    let mut peer_net = Net(control_session.sender());
    if let Some(peer_id) = send_hello {
        peer_net.send(peer_id, Message::Hello(peer_id))?
    }
    let socket_session = socket_net.recv_session(|buf| {
        let message = bincode::options()
            .allow_trailing_bytes()
            .deserialize::<Message>(buf)?;
        match message {
            Message::Kademlia(kademlia::Message::FindPeer(find_peer)) => {
                peer_sender.send(kademlia::Event::IngressFindPeer(find_peer))?
            }
            Message::Kademlia(kademlia::Message::FindPeerOk(find_peer_ok)) => {
                peer_sender.send(kademlia::Event::IngressFindPeerOk(find_peer_ok))?
            }
            Message::Hello(peer_id) => {
                println!("Replying Hello from {}", H256::from(&peer_id));
                peer_net.send(peer_id, Message::HelloOk)?
            }
            Message::HelloOk => {
                println!("Received HelloOk")
            }
        }
        Ok(())
    });

    let crypto_session = crypto_session.run(peer_session.sender());
    let mut control = Control::<_, Message, _, SocketAddr>::new(
        augustus::net::MessageNet::<_, Message>::new(socket_net.clone()),
        peer_session.sender(),
    );
    let peer_session = peer_session.run(&mut peer);
    let control_session = control_session.run(&mut control);
    tokio::select! {
        result = socket_session => result?,
        result = crypto_session => result?,
        result = peer_session => result?,
        result = control_session => result?,
    }
    Err(anyhow::anyhow!("unreachable"))
}
