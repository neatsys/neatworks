use std::net::SocketAddr;

use augustus::{
    bulk,
    crypto::{
        peer::{Crypto, PublicKey},
        H256,
    },
    entropy::{CodecWorker, Get, MessageNet, Peer, Put, SendCodecEvent},
    event::{
        self,
        erased::{
            session::{Buffered, Sender},
            Blanket, Session,
        },
        session, Once, SendEvent, Unify, Unreachable,
    },
    kademlia::{self, Buckets, PeerRecord, SendCryptoEvent},
    net::{dispatch, kademlia::Control, Detach, Dispatch},
    worker::{Submit, Worker},
};
use entropy_control_messages::StartPeersConfig;
use rand::{rngs::StdRng, seq::SliceRandom};
use tokio::{
    fs::{create_dir, remove_dir_all},
    net::TcpListener,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

use crate::Upcall;

#[derive(Debug, derive_more::From)]
pub enum Invoke {
    Put(Put<H256>),
    Get(Get<H256>),
}

pub async fn session(
    record: PeerRecord<PublicKey, SocketAddr>,
    crypto: Crypto,
    mut rng: StdRng,
    mut records: Vec<PeerRecord<PublicKey, SocketAddr>>,
    mut invoke: UnboundedReceiver<Invoke>,
    upcall: UnboundedSender<Upcall>,
    config: StartPeersConfig,
) -> anyhow::Result<()> {
    let peer_id = record.id;
    let path = format!("/tmp/entropy-{peer_id:x}");
    let path = std::path::Path::new(&path);
    let _ = remove_dir_all(path).await;
    create_dir(path).await?;
    let addr = record.addr;
    // let listener = TcpListener::bind(SocketAddr::from(([0; 4], addr.port()))).await?;
    let listener = TcpListener::bind(addr).await?;
    // let quic = augustus::net::session::Quic::new(SocketAddr::from(([0; 4], addr.port())))?;
    // let quic = augustus::net::session::Quic::new(addr)?;

    let ip = record.addr.ip();
    let mut buckets = Buckets::new(record);
    // we don't really need this to be deterministic actually... just too late when realized
    records.shuffle(&mut rng);
    for record in records {
        if record.id == peer_id {
            continue;
        }
        buckets.insert(record)?
    }

    let mut kademlia_session = Session::new();
    let mut kademlia_control_session = Session::new();
    let (mut bulk_sender, bulk_receiver) = unbounded_channel();
    let (fs_sender, fs_receiver) = unbounded_channel();
    let mut dispatch_control_session = session::Session::new();
    let mut peer_session = Session::new();

    let mut kademlia_peer = Blanket(Buffered::from(
        // TODO when there's dynamical joining peer that actually need to do bootstrap (and wait for
        // it done)
        kademlia::Peer::<_, _, _, Unreachable, _>::new(
            buckets,
            Box::new(MessageNet::new(dispatch::Net::from(
                dispatch_control_session.sender(),
            ))) as Box<dyn kademlia::Net<SocketAddr> + Send + Sync>,
            Box::new(Sender::from(kademlia_control_session.sender()))
                as Box<dyn kademlia::Upcall<SocketAddr> + Send + Sync>,
            Box::new(kademlia::CryptoWorker::from(Worker::Inline(
                crypto.clone(),
                Sender::from(kademlia_session.sender()),
            )))
                as Box<dyn Submit<Crypto, dyn SendCryptoEvent<SocketAddr>> + Send + Sync>,
        ),
    ));
    let mut kademlia_control = Blanket(Buffered::from(Control::new(
        Box::new(dispatch::Net::from(dispatch_control_session.sender()))
            as Box<dyn augustus::net::kademlia::Net<SocketAddr, bytes::Bytes> + Send + Sync>,
        Sender::from(kademlia_session.sender()),
    )));
    let mut peer = Blanket(Buffered::from(Peer::new(
        peer_id,
        crypto,
        config.fragment_len,
        config.chunk_k,
        config.chunk_n,
        config.chunk_m,
        MessageNet::<_, SocketAddr>::new(Detach(Sender::from(kademlia_control_session.sender()))),
        bulk_sender.clone(),
        upcall,
        // TODO change to spawn (why i changed it to inline however?)
        Box::new(CodecWorker::from(Worker::Inline(
            (),
            Sender::from(peer_session.sender()),
        ))) as Box<dyn Submit<(), dyn SendCodecEvent> + Send + Sync>,
        fs_sender,
    )));
    let mut dispatch_control = Unify(event::Buffered::from(Dispatch::new(
        augustus::net::session::Tcp::new(addr)?,
        // quic.clone(),
        {
            let mut peer_sender = Sender::from(peer_session.sender());
            let mut kademlia_sender = Sender::from(kademlia_session.sender());
            move |buf: &_| {
                augustus::entropy::on_buf::<SocketAddr>(
                    buf,
                    &mut peer_sender,
                    &mut kademlia_sender,
                    &mut bulk_sender,
                )
            }
        },
        Once(dispatch_control_session.sender()),
    )?));

    let invoke_session = {
        let mut peer_sender = Sender::from(peer_session.sender());
        async move {
            while let Some(invoke) = invoke.recv().await {
                match invoke {
                    Invoke::Put(put) => peer_sender.send(put)?,
                    Invoke::Get(get) => peer_sender.send(get)?,
                }
            }
            anyhow::Ok(())
        }
    };
    let accept_session =
        augustus::net::session::tcp::accept_session(listener, dispatch_control_session.sender());
    // augustus::net::session::quic::accept_session(quic, dispatch_control_session.sender());
    let kademlia_session = kademlia_session.run(&mut kademlia_peer);
    let bulk_session = bulk::session(
        ip,
        bulk_receiver,
        MessageNet::<_, SocketAddr>::new(Detach(Sender::from(kademlia_control_session.sender()))),
        Sender::from(peer_session.sender()),
    );
    let kademlia_control_session = kademlia_control_session.run(&mut kademlia_control);
    let fs_session =
        augustus::entropy::fs::session(path, fs_receiver, Sender::from(peer_session.sender()));
    let peer_session = peer_session.run(&mut peer);
    let dispatch_control_session = dispatch_control_session.run(&mut dispatch_control);

    tokio::select! {
        result = invoke_session => result?,
        result = accept_session => result?,
        result = kademlia_session => result?,
        result = kademlia_control_session => result?,
        result = bulk_session => result?,
        result = fs_session => result?,
        result = peer_session => result?,
        result = dispatch_control_session => result?,
    }
    Err(anyhow::format_err!("unexpected shutdown"))
}

// cSpell:words kademlia reqwest oneshot upcall ipfs quic rustix seedable
