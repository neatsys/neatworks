use std::net::SocketAddr;

use augustus::{
    app,
    crypto::{Crypto, CryptoFlavor},
    event::{
        self,
        erased::{session::Sender, Blanket, Buffered, Session},
        Once,
    },
    net::{
        dispatch,
        session::{tcp, Tcp},
        Dispatch, IndexNet,
    },
    pbft,
    worker::{Submit, Worker},
};
use boson_control_messages::{CopsClient, CopsServer, CopsVariant};
use tokio::net::TcpListener;

pub async fn pbft_client_session(config: CopsClient) -> anyhow::Result<()> {
    Ok(())
}

pub async fn pbft_server_session(config: CopsServer) -> anyhow::Result<()> {
    let id = config.id;
    let addrs = config.addrs;
    let addr = addrs[id as usize];
    let CopsVariant::Replicated(config) = config.variant else {
        anyhow::bail!("unimplemented")
    };
    let num_replica = addrs.len();
    let num_faulty = config.num_faulty;
    let app = app::BTreeMap::new(); // TODO

    let tcp_listener = TcpListener::bind(addr).await?;
    let mut dispatch_session = event::Session::new();
    let mut replica_session = Session::new();

    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender = Sender::from(replica_session.sender());
            move |buf: &_| pbft::to_replica_on_buf(buf, &mut sender)
        },
        Once(dispatch_session.sender()),
    )?));
    let mut replica = Blanket(Buffered::from(pbft::Replica::new(
        id,
        app,
        pbft::ToReplicaMessageNet::new(IndexNet::new(
            dispatch::Net::from(dispatch_session.sender()),
            addrs,
            id as usize,
        )),
        pbft::ToClientMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
        Box::new(pbft::CryptoWorker::from(Worker::Inline(
            Crypto::new_hardcoded_replication(num_replica, id, CryptoFlavor::Schnorrkel)?,
            Sender::from(replica_session.sender()),
        ))) as Box<dyn Submit<Crypto, dyn pbft::SendCryptoEvent<SocketAddr>> + Send + Sync>,
        num_replica,
        num_faulty,
    )));

    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let replica_session = replica_session.run(&mut replica);
    tokio::select! {
        result = tcp_accept_session => result?,
        result = dispatch_session => result?,
        result = replica_session => result?,
    }
    anyhow::bail!("unreachable")
}

// cSpell:words pbft
