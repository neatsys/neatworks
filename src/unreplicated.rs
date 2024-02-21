use std::{collections::BTreeMap, fmt::Debug, net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    event::{OnEvent, SendEvent, Timer, TimerId},
    net::{deserialize, Addr, MessageNet, SendMessage},
    replication::{Invoke, Request},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Vec<u8>,
}

pub trait ToClientNet<A>: SendMessage<A, Reply> {}
impl<T: SendMessage<A, Reply>, A> ToClientNet<A> for T {}

pub trait ToReplicaNet<A>: SendMessage<u8, Request<A>> {}
impl<T: SendMessage<u8, Request<A>>, A> ToReplicaNet<A> for T {}

#[derive(Debug, Clone, derive_more::From)]
pub enum ClientEvent {
    Invoke(Vec<u8>),
    Ingress(Reply),
    ResendTimeout,
}

impl From<Invoke> for ClientEvent {
    fn from(Invoke(op): Invoke) -> Self {
        Self::Invoke(op)
    }
}

pub trait ClientUpcall: SendEvent<(u32, Vec<u8>)> {}
impl<T: SendEvent<(u32, Vec<u8>)>> ClientUpcall for T {}

#[derive(Debug)]
pub struct Client<N, U, A> {
    id: u32,
    addr: A,
    seq: u32,
    invoke: Option<ClientInvoke>,

    net: N,
    upcall: U,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ClientInvoke {
    op: Vec<u8>,
    resend_timer: TimerId,
}

impl<N, U, A> Client<N, U, A> {
    pub fn new(id: u32, addr: A, net: N, upcall: U) -> Self {
        Self {
            id,
            addr,
            net,
            upcall,
            seq: 0,
            invoke: Default::default(),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<ClientEvent> for Client<N, U, A> {
    fn on_event(
        &mut self,
        event: ClientEvent,
        timer: &mut impl Timer<ClientEvent>,
    ) -> anyhow::Result<()> {
        match event {
            ClientEvent::Invoke(op) => self.on_invoke(op, |period| {
                timer.set(period, || ClientEvent::ResendTimeout)
            }),
            ClientEvent::ResendTimeout => self.on_resend_timeout(),
            ClientEvent::Ingress(reply) => self.on_ingress(reply, |timer_id| timer.unset(timer_id)),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> Client<N, U, A> {
    fn on_invoke(
        &mut self,
        op: Vec<u8>,
        mut set_resend_timer: impl FnMut(Duration) -> anyhow::Result<TimerId>,
    ) -> anyhow::Result<()> {
        if self.invoke.is_some() {
            anyhow::bail!("concurrent invocation")
        }
        self.seq += 1;
        let invoke = ClientInvoke {
            op,
            resend_timer: set_resend_timer(Duration::from_millis(1000))?,
        };
        self.invoke = Some(invoke);
        self.do_send()
    }

    fn on_resend_timeout(&mut self) -> anyhow::Result<()> {
        // TODO logging
        self.do_send()
    }

    fn on_ingress(
        &mut self,
        reply: Reply,
        mut unset_timer: impl FnMut(TimerId) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(invoke) = self.invoke.take() else {
            return Ok(());
        };
        unset_timer(invoke.resend_timer)?;
        self.upcall.send((self.id, reply.result))
    }

    fn do_send(&mut self) -> anyhow::Result<()> {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self.invoke.as_ref().unwrap().op.clone(),
        };
        self.net.send(0, request)
    }
}

#[derive(Debug)]
pub enum ReplicaEvent<A> {
    Ingress(Request<A>),
    Dummy, //
}

pub struct Replica<S, N, A> {
    replies: BTreeMap<u32, Reply>,
    _addr_marker: std::marker::PhantomData<A>,
    app: S,
    net: N,
}

impl<S: Debug, N, A> Debug for Replica<S, N, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("replies", &self.replies)
            .field("app", &self.app)
            .finish_non_exhaustive()
    }
}

impl<S, N, A> Replica<S, N, A> {
    pub fn new(app: S, net: N) -> Self {
        Self {
            app,
            net,
            _addr_marker: Default::default(),
            replies: Default::default(),
        }
    }
}

impl<S: App, N: ToClientNet<A>, A> OnEvent<ReplicaEvent<A>> for Replica<S, N, A> {
    fn on_event(
        &mut self,
        event: ReplicaEvent<A>,
        _: &mut impl Timer<ReplicaEvent<A>>,
    ) -> anyhow::Result<()> {
        match event {
            ReplicaEvent::Ingress(request) => self.on_ingress(request),
            ReplicaEvent::Dummy => unreachable!(),
        }
    }
}

impl<S: App, N: ToClientNet<A>, A> Replica<S, N, A> {
    fn on_ingress(&mut self, request: Request<A>) -> anyhow::Result<()> {
        match self.replies.get(&request.client_id) {
            Some(reply) if reply.seq > request.seq => return Ok(()),
            Some(reply) if reply.seq == request.seq => {
                return self.net.send(request.client_addr, reply.clone())
            }
            _ => {}
        }
        let reply = Reply {
            seq: request.seq,
            result: self.app.execute(&request.op)?,
        };
        self.replies.insert(request.client_id, reply.clone());
        self.net.send(request.client_addr, reply)
    }
}

pub type ToClientMessageNet<T> = MessageNet<T, Reply>;

pub fn to_client_on_buf(buf: &[u8], sender: &mut impl SendEvent<Reply>) -> anyhow::Result<()> {
    sender.send(deserialize(buf)?)
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, Request<A>>;

pub fn to_replica_on_buf(
    buf: &[u8],
    sender: &mut impl SendEvent<ReplicaEvent<SocketAddr>>,
) -> anyhow::Result<()> {
    sender.send(ReplicaEvent::Ingress(deserialize(buf)?))
}

pub mod erased {
    use std::net::SocketAddr;

    use crate::{
        app::App,
        event::{
            erased::{OnEvent, Timer},
            SendEvent,
        },
        net::{deserialize, events::Recv, Addr},
        replication::{Invoke, Request},
    };

    use super::{ClientUpcall, Reply, ToClientNet, ToReplicaNet};

    pub type Client<A> = super::Client<
        Box<dyn ToReplicaNet<A> + Send + Sync>,
        Box<dyn ClientUpcall + Send + Sync>,
        A,
    >;

    impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<Invoke> for super::Client<N, U, A> {
        fn on_event(
            &mut self,
            Invoke(op): Invoke,
            timer: &mut impl Timer<Self>,
        ) -> anyhow::Result<()> {
            self.on_invoke(op, |period| timer.set(period, Resend))
        }
    }

    #[derive(Debug, Clone)]
    pub struct Resend;

    impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<Resend> for super::Client<N, U, A> {
        fn on_event(&mut self, Resend: Resend, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
            // TODO logging
            self.do_send()
        }
    }

    impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<Recv<Reply>> for super::Client<N, U, A> {
        fn on_event(
            &mut self,
            Recv(reply): Recv<Reply>,
            timer: &mut impl Timer<Self>,
        ) -> anyhow::Result<()> {
            self.on_ingress(reply, |timer_id| timer.unset(timer_id))
        }
    }

    pub type Replica<S, A> = super::Replica<S, Box<dyn ToClientNet<A> + Send + Sync>, A>;

    impl<S: App, N: ToClientNet<A>, A> OnEvent<Recv<Request<A>>> for super::Replica<S, N, A> {
        fn on_event(
            &mut self,
            Recv(request): Recv<Request<A>>,
            _: &mut impl Timer<Self>,
        ) -> anyhow::Result<()> {
            self.on_ingress(request)
        }
    }

    pub fn to_client_on_buf(
        buf: &[u8],
        sender: &mut impl SendEvent<Recv<Reply>>,
    ) -> anyhow::Result<()> {
        sender.send(Recv(deserialize(buf)?))
    }

    pub fn to_replica_on_buf(
        buf: &[u8],
        sender: &mut impl SendEvent<Recv<Request<SocketAddr>>>,
    ) -> anyhow::Result<()> {
        sender.send(Recv(deserialize(buf)?))
    }
}

pub mod check {
    use std::{
        any::Any,
        collections::{BTreeMap, BTreeSet, VecDeque},
        mem::replace,
        sync::mpsc::{channel, Receiver, Sender},
    };

    use serde::{Deserialize, Serialize};

    use crate::{
        app::KVStore,
        event::{
            erased::{OnEvent, UnreachableTimer},
            SendEvent, TimerId,
        },
        net::{events::Recv, SendMessage},
        replication::{
            check::DryCloseLoop, CloseLoop, Invoke, InvokeOk, ReplicaNet, Request, Workload,
        },
    };

    use super::{erased, ClientInvoke, Reply};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub enum Addr {
        Client(usize),
        Replica,
    }

    pub struct State<I> {
        pub clients: Vec<erased::Client<Addr>>,
        pub close_loops: Vec<CloseLoop<I>>,
        pub replica: erased::Replica<KVStore, Addr>,

        message_events: BTreeSet<MessageEvent>,
        timer_events: BTreeMap<Addr, VecDeque<TimerEvent>>,
        timer_id: u32,

        transient_net: Transient<MessageEvent>,
        transient_message_events: Receiver<MessageEvent>,
        transient_invokes: Vec<Receiver<Invoke>>,
        transient_upcalls: Vec<Receiver<InvokeOk>>,
    }

    #[derive(Debug, Clone)]
    pub enum Event {
        Message(MessageEvent),
        Timer(TimerEvent),
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MessageEvent {
        dest: Addr,
        message: Message,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    enum Message {
        Request(Request<Addr>),
        Reply(Reply),
    }

    #[derive(Debug, Clone)]
    pub struct TimerEvent {
        dest: Addr,
        timer_id: u32,
        timer: Timer,
    }

    #[derive(Debug, Clone)]
    pub enum Timer {
        Resend,
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct DryState {
        clients: Vec<DryClient>,
        close_loops: Vec<DryCloseLoop>,
        replica: DryReplica,
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct DryClient {
        id: u32,
        addr: Addr,
        seq: u32,
        invoke: Option<ClientInvoke>,
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct DryReplica {
        replies: BTreeMap<u32, Reply>,
        app: KVStore,
    }

    impl<I> From<State<I>> for DryState {
        fn from(value: State<I>) -> Self {
            let clients = value
                .clients
                .into_iter()
                .map(|client| DryClient {
                    id: client.id,
                    addr: client.addr,
                    seq: client.seq,
                    invoke: client.invoke,
                })
                .collect();
            let close_loops = value.close_loops.into_iter().map(Into::into).collect();
            let replica = DryReplica {
                replies: value.replica.replies,
                app: value.replica.app,
            };
            Self {
                clients,
                close_loops,
                replica,
            }
        }
    }

    pub struct Transient<M>(Sender<M>);

    impl<M> Clone for Transient<M> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<N: Into<M>, M> SendEvent<N> for Transient<M> {
        fn send(&mut self, event: N) -> anyhow::Result<()> {
            self.0.send(event.into()).unwrap();
            Ok(())
        }
    }

    impl SendMessage<Addr, Request<Addr>> for Transient<MessageEvent> {
        fn send(&mut self, dest: Addr, message: Request<Addr>) -> anyhow::Result<()> {
            self.0
                .send(MessageEvent {
                    dest,
                    message: Message::Request(message),
                })
                .unwrap();
            Ok(())
        }
    }

    impl SendMessage<Addr, Reply> for Transient<MessageEvent> {
        fn send(&mut self, dest: Addr, message: Reply) -> anyhow::Result<()> {
            self.0
                .send(MessageEvent {
                    dest,
                    message: Message::Reply(message),
                })
                .unwrap();
            Ok(())
        }
    }

    impl<I> Default for State<I> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<I> State<I> {
        pub fn new() -> Self {
            let (transient_event_sender, transient_event_receiver) = channel();
            let transient_net = Transient(transient_event_sender);
            let replica = erased::Replica::new(KVStore::new(), Box::new(transient_net.clone()));
            Self {
                replica,
                transient_net,
                transient_message_events: transient_event_receiver,
                clients: Default::default(),
                close_loops: Default::default(),
                message_events: Default::default(),
                timer_events: Default::default(),
                timer_id: 0,
                transient_upcalls: Default::default(),
                transient_invokes: Default::default(),
            }
        }

        pub fn push_client(&mut self, workload: I) -> anyhow::Result<()> {
            let index = self.clients.len();
            let id = index as u32 + 1000;
            let (transient_upcall_sender, transient_upcall_receiver) = channel();
            let client = erased::Client::new(
                id,
                Addr::Client(index),
                Box::new(ReplicaNet::new(
                    self.transient_net.clone(),
                    vec![Addr::Replica],
                    None,
                )),
                Box::new(Transient(transient_upcall_sender)),
            );
            self.clients.push(client);
            self.transient_upcalls.push(transient_upcall_receiver);

            let (transient_invoke_sender, transient_invoke_receiver) = channel();
            let mut close_loop = CloseLoop::new(workload);
            // close_loop.invocations.get_or_insert_with(Default::default);
            close_loop.insert_client(id, Transient(transient_invoke_sender))?;
            self.close_loops.push(close_loop);
            self.transient_invokes.push(transient_invoke_receiver);
            Ok(())
        }
    }

    impl<I: Clone + Iterator<Item = Workload>> crate::search::State for State<I> {
        type Event = Event;

        fn events(&self) -> Vec<Self::Event> {
            let mut events = self
                .message_events
                .iter()
                .cloned()
                .map(Event::Message)
                .collect::<Vec<_>>();
            for timer_events in self.timer_events.values() {
                if let Some(event) = timer_events.front() {
                    events.push(Event::Timer(event.clone()))
                }
            }
            events
        }

        fn duplicate(&self) -> anyhow::Result<Self> {
            let (transient_event_sender, transient_event_receiver) = channel();
            let transient_net = Transient(transient_event_sender);
            let mut clients = Vec::new();
            let mut transient_upcalls = Vec::new();

            for client in &self.clients {
                let (transient_upcall_sender, transient_upcall_receiver) = channel();
                let client = erased::Client {
                    id: client.id,
                    addr: client.addr,
                    seq: client.seq,
                    invoke: client.invoke.clone(),
                    net: Box::new(ReplicaNet::new(
                        transient_net.clone(),
                        vec![Addr::Replica],
                        None,
                    )),
                    upcall: Box::new(Transient(transient_upcall_sender)),
                };
                clients.push(client);
                transient_upcalls.push(transient_upcall_receiver)
            }

            let mut close_loops = Vec::new();
            let mut transient_invokes = Vec::new();
            for close_loop in &self.close_loops {
                let (transient_invoke_sender, transient_invoke_receiver) = channel();
                let close_loop =
                    close_loop.duplicate(|| Transient(transient_invoke_sender.clone()))?;
                close_loops.push(close_loop);
                transient_invokes.push(transient_invoke_receiver);
            }

            let replica = erased::Replica {
                replies: self.replica.replies.clone(),
                app: self.replica.app.clone(),
                net: Box::new(transient_net.clone()),
                _addr_marker: Default::default(),
            };

            Ok(Self {
                clients,
                close_loops,
                replica,
                message_events: self.message_events.clone(),
                timer_events: self.timer_events.clone(),
                timer_id: self.timer_id,
                transient_net,
                transient_message_events: transient_event_receiver,
                transient_invokes,
                transient_upcalls,
            })
        }

        fn step(&mut self, event: Self::Event) -> anyhow::Result<()> {
            match event {
                Event::Message(MessageEvent {
                    dest: Addr::Replica,
                    message: Message::Request(message),
                }) => self
                    .replica
                    .on_event(Recv(message), &mut UnreachableTimer)?,
                Event::Message(MessageEvent {
                    dest: Addr::Client(i),
                    message: Message::Reply(message),
                }) => {
                    let mut timer = StateTimer {
                        addr: self.clients[i].addr,
                        timer_events: &mut self.timer_events,
                        timer_id: &mut self.timer_id,
                    };
                    self.clients[i].on_event(Recv(message), &mut timer)?
                }
                Event::Timer(TimerEvent {
                    dest: Addr::Client(i),
                    timer_id: _,
                    timer: Timer::Resend,
                }) => {
                    self.timer_events
                        .get_mut(&Addr::Client(i))
                        .unwrap()
                        .rotate_left(1);
                    let mut timer = StateTimer {
                        addr: self.clients[i].addr,
                        timer_events: &mut self.timer_events,
                        timer_id: &mut self.timer_id,
                    };
                    self.clients[i].on_event(erased::Resend, &mut timer)?
                }
                _ => anyhow::bail!("unexpected event"),
            }
            self.flush()
        }
    }

    impl<I: Iterator<Item = Workload>> State<I> {
        pub fn launch(&mut self) -> anyhow::Result<()> {
            if self.close_loops.len() != self.clients.len() {
                anyhow::bail!("workload number does not match client number")
            }
            for close_loop in &mut self.close_loops {
                close_loop.launch()?
            }
            self.flush()
        }

        fn flush(&mut self) -> anyhow::Result<()> {
            let mut run = true;
            while replace(&mut run, false) {
                for event in self.transient_message_events.try_iter() {
                    self.message_events.insert(event);
                }
                for (i, transient_invokes) in self.transient_invokes.iter().enumerate() {
                    for invoke in transient_invokes.try_iter() {
                        let mut timer = StateTimer {
                            addr: self.clients[i].addr,
                            timer_events: &mut self.timer_events,
                            timer_id: &mut self.timer_id,
                        };
                        self.clients[i].on_event(invoke, &mut timer)?;
                        run = true
                    }
                }
                for (i, transient_upcalls) in self.transient_upcalls.iter().enumerate() {
                    for upcall in transient_upcalls.try_iter() {
                        self.close_loops[i].on_event(upcall, &mut UnreachableTimer)?;
                        run = true
                    }
                }
            }
            Ok(())
        }
    }

    struct StateTimer<'a> {
        addr: Addr,
        timer_events: &'a mut BTreeMap<Addr, VecDeque<TimerEvent>>,
        timer_id: &'a mut u32,
    }

    impl crate::event::erased::Timer<erased::Client<Addr>> for StateTimer<'_> {
        fn set<M: Clone + Send + Sync + 'static>(
            &mut self,
            _period: std::time::Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            erased::Client<Addr>: OnEvent<M>,
        {
            // this is bad, really bad, at least not good
            // it's so hard to keep everything not bad in a codebase that can do everything
            if (&event as &dyn Any).is::<erased::Resend>() {
                *self.timer_id += 1;
                let timer_id = *self.timer_id;
                self.timer_events
                    .entry(self.addr)
                    .or_default()
                    .push_back(TimerEvent {
                        dest: self.addr,
                        timer_id,
                        timer: Timer::Resend,
                    });
                return Ok(TimerId(timer_id));
            }
            Err(anyhow::anyhow!("expected event type"))
        }

        fn unset(&mut self, TimerId(timer_id): TimerId) -> anyhow::Result<()> {
            let timer_events = self
                .timer_events
                .get_mut(&self.addr)
                .ok_or(anyhow::anyhow!("address not found"))?;
            let i = timer_events
                .iter()
                .position(|event| event.timer_id == timer_id)
                .ok_or(anyhow::anyhow!("timer not found"))?;
            timer_events.remove(i);
            Ok(())
        }
    }
}
