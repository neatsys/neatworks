use std::net::SocketAddr;

use augustus::{
    app,
    crypto::{Crypto, CryptoFlavor},
    event::{
        self,
        erased::{session::Sender, Blanket, Buffered, Session, Unify},
        Once, SendEvent,
    },
    lamport_mutex::{
        self, events::RequestOk, Causal, Lamport, LamportClock, Processor, Replicated,
    },
    net::{
        deserialize, dispatch,
        events::Recv,
        session::{tcp, Tcp},
        Detach, Dispatch, IndexNet, InvokeNet,
    },
    pbft,
    worker::{Submit, Worker},
    workload::{events::InvokeOk, Queue},
};
use boson_control_messages::{MutexReplicated, MutexUntrusted};
use tokio::{net::TcpListener, sync::mpsc::UnboundedReceiver};
use tokio_util::sync::CancellationToken;

pub enum Event {
    Request,
    Release,
}

pub async fn untrusted_session(
    config: MutexUntrusted,
    mut events: UnboundedReceiver<Event>,
    upcall: impl SendEvent<RequestOk> + Send + Sync + 'static,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let id = config.id;
    let addr = config.addrs[id as usize];

    let tcp_listener = TcpListener::bind(config.addrs[config.id as usize]).await?;
    let mut dispatch_session = event::Session::new();
    let mut processor_session = Session::new();
    let mut causal_net_session = Session::new();

    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender = Sender::from(causal_net_session.sender());
            move |buf: &_| lamport_mutex::on_buf(buf, &mut sender)
        },
        Once(dispatch_session.sender()),
    )?));
    let mut processor = Blanket(Unify(Processor::new(
        id,
        config.addrs.len(),
        |id| (0u32, id),
        Detach(Sender::from(causal_net_session.sender())),
        upcall,
    )));
    let mut causal_net = Blanket(Unify(Causal::new(
        (0, id),
        Box::new(Sender::from(processor_session.sender()))
            as Box<dyn lamport_mutex::SendRecvEvent<LamportClock> + Send + Sync>,
        Box::new(Lamport(Sender::from(causal_net_session.sender()), id))
            as Box<dyn SendEvent<lamport_mutex::Update<LamportClock>> + Send + Sync>,
        lamport_mutex::MessageNet::<_, LamportClock>::new(IndexNet::new(
            dispatch::Net::from(dispatch_session.sender()),
            config.addrs,
            // intentionally sending loopback messages as expected by processor protocol
            None,
        )),
    )?));

    let event_session = {
        let mut sender = Sender::from(processor_session.sender());
        async move {
            while let Some(event) = events.recv().await {
                match event {
                    Event::Request => sender.send(lamport_mutex::events::Request)?,
                    Event::Release => sender.send(lamport_mutex::events::Release)?,
                }
            }
            anyhow::Ok(())
        }
    };
    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let processor_session = processor_session.run(&mut processor);
    let causal_net_session = causal_net_session.run(&mut causal_net);

    tokio::select! {
        () = cancel.cancelled() => return Ok(()),
        result = event_session => result?,
        result = tcp_accept_session => result?,
        result = dispatch_session => result?,
        result = processor_session => result?,
        result = causal_net_session => result?,
    }
    anyhow::bail!("unreachable")
}

pub async fn replicated_session(
    config: MutexReplicated,
    mut events: UnboundedReceiver<Event>,
    upcall: impl SendEvent<RequestOk> + Send + Sync + 'static,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let id = config.id;
    let client_addr = config.client_addrs[config.id as usize];
    let addr = config.addrs[config.id as usize];
    let num_replica = config.addrs.len();
    let num_faulty = config.num_faulty;
    let addrs = config.addrs;

    let client_tcp_listener = TcpListener::bind(client_addr).await?;
    let tcp_listener = TcpListener::bind(addr).await?;
    let mut client_dispatch_session = event::Session::new();
    let mut client_session = Session::new();
    let mut dispatch_session = event::Session::new();
    let mut replica_session = Session::new();
    let mut queue_session = Session::new();
    let mut processor_session = Session::new();

    let mut client_dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(client_addr)?,
        {
            let mut sender = Sender::from(client_session.sender());
            move |buf: &_| pbft::to_client_on_buf(buf, &mut sender)
        },
        Once(client_dispatch_session.sender()),
    )?));
    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender = Sender::from(replica_session.sender());
            move |buf: &_| pbft::to_replica_on_buf(buf, &mut sender)
        },
        Once(dispatch_session.sender()),
    )?));
    let mut client = Blanket(Buffered::from(pbft::Client::new(
        id as _,
        client_addr,
        pbft::ToReplicaMessageNet::new(IndexNet::new(
            dispatch::Net::from(client_dispatch_session.sender()),
            addrs.clone(),
            None,
        )),
        Box::new(Sender::from(queue_session.sender()))
            as Box<dyn SendEvent<InvokeOk> + Send + Sync>,
        num_replica,
        num_faulty,
    )));
    let mut replica = Blanket(Buffered::from(pbft::Replica::new(
        id,
        app::OnBuf({
            let mut sender = Replicated::new(Sender::from(processor_session.sender()));
            move |buf: &_| sender.send(Recv(deserialize::<lamport_mutex::Message>(buf)?))
        }),
        pbft::ToReplicaMessageNet::new(IndexNet::new(
            dispatch::Net::from(dispatch_session.sender()),
            addrs,
            id as usize,
        )),
        pbft::ToClientMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
        Box::new(pbft::CryptoWorker::from(Worker::Inline(
            Crypto::new_hardcoded(num_replica, id, CryptoFlavor::Schnorrkel)?,
            Sender::from(replica_session.sender()),
        ))) as Box<dyn Submit<Crypto, dyn pbft::SendCryptoEvent<SocketAddr>> + Send + Sync>,
        num_replica,
        num_faulty,
    )));
    let mut queue = Blanket(Unify(Queue::new(Sender::from(client_session.sender()))));
    let mut processor = Blanket(Unify(Processor::new(
        id,
        num_replica,
        |_| 0u32,
        augustus::net::MessageNet::<_, lamport_mutex::Message>::new(InvokeNet(Sender::from(
            queue_session.sender(),
        ))),
        upcall,
    )));

    let event_session = {
        let mut sender = Sender::from(processor_session.sender());
        async move {
            while let Some(event) = events.recv().await {
                match event {
                    Event::Request => sender.send(lamport_mutex::events::Request)?,
                    Event::Release => sender.send(lamport_mutex::events::Release)?,
                }
            }
            anyhow::Ok(())
        }
    };
    let client_tcp_accept_session =
        tcp::accept_session(client_tcp_listener, client_dispatch_session.sender());
    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let client_dispatch_session = client_dispatch_session.run(&mut client_dispatch);
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let client_session = client_session.run(&mut client);
    let replica_session = replica_session.run(&mut replica);
    let queue_session = queue_session.run(&mut queue);
    let processor_session = processor_session.run(&mut processor);

    tokio::select! {
        () = cancel.cancelled() => return Ok(()),
        result = event_session => result?,
        result = client_tcp_accept_session => result?,
        result = client_dispatch_session => result?,
        result = tcp_accept_session => result?,
        result = dispatch_session => result?,
        result = client_session => result?,
        result = replica_session => result?,
        result = queue_session => result?,
        result = processor_session => result?,
    }
    anyhow::bail!("unreachable")
}

// cSpell:words lamport upcall pbft
