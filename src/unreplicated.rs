use std::{collections::BTreeMap, fmt::Debug, net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    event::{OnEvent, SendEvent, Timer, TimerId},
    net::{deserialize, Addr, MessageNet, SendMessage},
    rpc::{Invoke, InvokeOk, Payload, Request},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Payload,
}

pub trait ToClientNet<A>: SendMessage<A, Reply> {}
impl<T: SendMessage<A, Reply>, A> ToClientNet<A> for T {}

pub trait ToReplicaNet<A>: SendMessage<u8, Request<A>> {}
impl<T: SendMessage<u8, Request<A>>, A> ToReplicaNet<A> for T {}

#[derive(Debug, Clone, derive_more::From)]
pub enum ClientEvent {
    Invoke(Payload),
    Ingress(Reply),
    ResendTimeout,
}

impl From<Invoke> for ClientEvent {
    fn from(Invoke(op): Invoke) -> Self {
        Self::Invoke(op)
    }
}

pub trait ClientUpcall: SendEvent<InvokeOk> {}
impl<T: SendEvent<InvokeOk>> ClientUpcall for T {}

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
    op: Payload,
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
        op: Payload,
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
            result: Payload(self.app.execute(&request.op)?),
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
        rpc::{Invoke, Request},
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

// notes about this iteration of model checking design. code below may get
// extracted into a reusable module that generally supports model checking of
// various protocols in latter days, but the code that matters for this
// discussion will probably stay here
//
// although trying out searching in state space is more as a jourey out of
// curious, the `impl State` that aggreates node states and network state and
// steps on them is also demanded by simulation-style unit tests. a decent
// protocol test suite probably involves infrastructures like
//`SimulatedTransport` in SpecPaxos codebase, which effectively does one "probe"
// of random-DFS as DSLabs defines, with restricted (and even deterministic)
// randomness that makes common sense e.g. no re-deliver, prefer deliver
// messages over times, etc.
//
// (so a minor concern is that if there's boilerplate code similar to the one
// below for every protocol, whether `pub mod check` naming convention is
// appropriate. `check` somehow implies model checking, which somehow implies
// the DSLabs approach, while a simluation test is kind of a different thing)
//
// the `impl OnEvent` owns `impl SendEvent` in this codebase. it's a tradeoff
// on interface and code organization that so far seems good in other part of
// the current codebase. it does expose some difficulties for model checking
// though
//
// the senders are probably not `Eq + Hash` which is required by BFS. so i
// introduce the concept of "dry state" that is `Eq + Hash` and fulfills BFS's
// demand: two states with identical dry state will have identical succesor
// state space. interestingly DSLabs also has an "equal and hash wrapper" thing
// for BFS and similarly "dehydrates" timers stuff from the state. i don't know
// why that is necessary for DSLabs though
//
// there are almost only two choices for implementing simulation sender: either
// channel sender, or shared mutable data structure which in Rust must be
// guarded by `Mutex` or similar. Both SpecPaxos and DSLabs go with the second
// choice (with unsafe bare metal data structure that carefully used). currently
// i choose the first one but that doesn't matter. both of them look ugly.
// i don't like the idea that stepping a state must always come with creating
// (potentially a bunch of) mutexes or channels. as far as i know the common
// choice of model checking is to send the events by returning them from event
// handlers (as the "side effect" of event handling), instead of by invoking on
// some owned internal objects. that approach comes with its own performance and
// engineering overhead and i like it even less
//
// another concern is about cloning. the cloning semantic on `impl OnEvent`s
// is already funny. for example if it owns channel-based `impl SendEvent`, then
// both the original object and the cloned one will send event to the same
// receiver, which may or may not be expected. even if they impl Clone in this
// way, `State` cannot be cloned by recursively cloning underlying
// `impl OnEvent`s, because it must not share channels/shared data structure
// with the original `State`. this is why the `State` trait does not subtyping
// `Clone`, but has its own `duplicate` defined, hopefulling avoid some
// pitfalls. the `impl State`s like the one below probably cannot derive
// `Clone`, and even if it can, model checking probably should not make use of
// its `Clone::clone`
//
// the codebase intentially be flexible on event types. indeed, it does not
// perform model checking for networking apps, but perform model checking for
// general event-driven apps. each `impl State` defines its own set of events
// that to be exposed to model checker for reordering, duplicating and dropping,
// and other events e.g. the `Invoke` and `InvokeOk` between clients and close
// loops can be considered as internal events and hided from model checker since
// it is little useful to e.g. reorder them with message/timer events.
//
// the more flexibility results in a weaker framework that can write less
// generic code for common case (since we indeed assume less common cases),
// which results in more boilerplates on app side to swallow that complexity. we
// already saw that in various artifact executables, and now it's getting worse:
// we will have to repeat the boilerplate for every protocol that demands unit
// tests, which is...every protocol
//
// actually, it's even worse than the artifact boilerplate. here we must unify
// every event type down to single representation, in order to have one "model"
// that has full control of what's happening first and what's happening later.
// protocols usually have no interest in helping on this, especially the ones
// work with type erased style event. as the result, we have to polyfill a lot
// of event hierarchy, and adaptions into/out of it. we must have the "into"
// adapter on `impl SendEvent` and `impl Timer` side (and the latter one is
// awfully based on `dyn Any`), and have the "out of" adapter on `State::step`
// (and `State::flush` below, which is tentatively decided as the conventional
// method for consuming internal events without consulting model checker). what
// makes it a problem is that all these boilerplates have to be protocol
// specific. there's hardly any code to be reused across protocols
//
// that's it. the `impl Into<DryState>` boilerplate. the `State::duplicate`
// boilerplate. the explicit event hierarchy, with the boilerplate to unify/
// dispatch events. all these probably have interior complexity and can only be
// abstracted at least at derivable macros level, that is, we cannot write a
// bunch of trait and let compiler to generate them automatically. i guess this
// is the cost we have to pay to perform model checking
// * in a static-typed language
// * which happens to lack runtime reflection
// * and we do not have a strict model of how protocols we are/will be writing
//   looks in mind
// fortunately the performance speedup kind of pays off. however, when things
// come to unit testing, a less-effort approach is very desirable
pub mod check {
    use std::{
        any::Any,
        collections::{BTreeMap, BTreeSet},
        mem::replace,
        sync::mpsc::{channel, Receiver, Sender},
        time::Duration,
    };

    use serde::{Deserialize, Serialize};

    use crate::{
        app::KVStore,
        event::{
            erased::{OnEvent, UnreachableTimer},
            SendEvent, TimerId,
        },
        net::{events::Recv, IndexNet, SendMessage},
        rpc::{check::DryCloseLoop, CloseLoop, Invoke, InvokeOk, Request, Workload},
    };

    use super::{erased, ClientInvoke, Reply};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub enum Addr {
        Client(usize),
        Replica,
    }

    pub struct State<I> {
        pub clients: Vec<ClientState<I>>,
        pub replica: erased::Replica<KVStore, Addr>,

        message_events: BTreeSet<MessageEvent>,
        timer_id: u32,

        transient_net: Transient<MessageEvent>,
        transient_message_events: Receiver<MessageEvent>,
    }

    pub struct ClientState<I> {
        pub state: erased::Client<Addr>,
        timer_events: Vec<TimerEvent<Timer>>,
        pub close_loop: CloseLoop<I>,
        transient_invokes: Receiver<Invoke>,
        transient_upcalls: Receiver<InvokeOk>,
    }

    #[derive(Debug, Clone)]
    pub enum Event {
        Message(MessageEvent),
        Timer(TimerEvent<Timer>),
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct MessageEvent {
        dest: Addr,
        message: Message,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    enum Message {
        Request(Request<Addr>),
        Reply(Reply),
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct TimerEvent<T> {
        timer_id: u32,
        period: Duration,
        timer: T,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub enum Timer {
        Resend(usize),
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DryState {
        clients: Vec<DryClientState>,
        replica: DryReplica,
        message_events: BTreeSet<MessageEvent>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DryClientState {
        id: u32,
        addr: Addr,
        seq: u32,
        invoke: Option<ClientInvoke>,

        close_loop: DryCloseLoop,
        timer_events: Vec<TimerEvent<Timer>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DryReplica {
        replies: BTreeMap<u32, Reply>,
        app: KVStore,
    }

    impl<I> From<State<I>> for DryState {
        fn from(value: State<I>) -> Self {
            let clients = value
                .clients
                .into_iter()
                .map(|client| DryClientState {
                    id: client.state.id,
                    addr: client.state.addr,
                    seq: client.state.seq,
                    invoke: client.state.invoke,
                    close_loop: client.close_loop.into(),
                    timer_events: client.timer_events,
                })
                .collect();
            let replica = DryReplica {
                replies: value.replica.replies,
                app: value.replica.app,
            };
            Self {
                clients,
                replica,
                message_events: value.message_events,
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
                message_events: Default::default(),
                timer_id: 0,
            }
        }

        pub fn push_client(&mut self, workload: I) -> anyhow::Result<()> {
            let index = self.clients.len();
            let id = index as u32 + 1000;
            let (transient_upcall_sender, transient_upcall_receiver) = channel();
            let client = erased::Client::new(
                id,
                Addr::Client(index),
                Box::new(IndexNet::new(
                    self.transient_net.clone(),
                    vec![Addr::Replica],
                    None,
                )),
                Box::new(Transient(transient_upcall_sender)),
            );

            let (transient_invoke_sender, transient_invoke_receiver) = channel();
            let mut close_loop = CloseLoop::new(workload);
            // close_loop.invocations.get_or_insert_with(Default::default);
            close_loop.insert_client(id, Transient(transient_invoke_sender))?;

            self.clients.push(ClientState {
                state: client,
                timer_events: Default::default(),
                close_loop,
                transient_invokes: transient_invoke_receiver,
                transient_upcalls: transient_upcall_receiver,
            });
            Ok(())
        }
    }

    // this is bad, really bad, at least not good
    // it's so hard to keep everything not bad in a codebase that can do everything
    fn to_client_timer_event(index: usize) -> impl Fn(&dyn Any) -> anyhow::Result<Timer> {
        move |event| {
            if event.is::<erased::Resend>() {
                return Ok(Timer::Resend(index));
            }
            Err(anyhow::anyhow!("unexpected event type"))
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
            for client in &self.clients {
                let mut prev_period = None;
                for event in &client.timer_events {
                    if let Some(prev_period) = prev_period {
                        if event.period >= prev_period {
                            break;
                        }
                    }
                    events.push(Event::Timer(event.clone()));
                    prev_period = Some(event.period)
                }
            }
            events
        }

        fn duplicate(&self) -> anyhow::Result<Self> {
            let (transient_event_sender, transient_event_receiver) = channel();
            let transient_net = Transient(transient_event_sender);

            let mut clients = Vec::new();
            for client in &self.clients {
                let (transient_upcall_sender, transient_upcall_receiver) = channel();
                let duplicated_client = erased::Client {
                    id: client.state.id,
                    addr: client.state.addr,
                    seq: client.state.seq,
                    invoke: client.state.invoke.clone(),
                    net: Box::new(IndexNet::new(
                        transient_net.clone(),
                        vec![Addr::Replica],
                        None,
                    )),
                    upcall: Box::new(Transient(transient_upcall_sender)),
                };

                let (transient_invoke_sender, transient_invoke_receiver) = channel();
                let close_loop = client
                    .close_loop
                    .duplicate(|| Transient(transient_invoke_sender.clone()))?;

                clients.push(ClientState {
                    state: duplicated_client,
                    timer_events: client.timer_events.clone(),
                    close_loop,
                    transient_invokes: transient_invoke_receiver,
                    transient_upcalls: transient_upcall_receiver,
                });
            }

            let replica = erased::Replica {
                replies: self.replica.replies.clone(),
                app: self.replica.app.clone(),
                net: Box::new(transient_net.clone()),
                _addr_marker: Default::default(),
            };

            Ok(Self {
                clients,
                replica,
                message_events: self.message_events.clone(),
                timer_id: self.timer_id,
                transient_net,
                transient_message_events: transient_event_receiver,
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
                    let client = &mut self.clients[i];
                    let mut timer = AnyTimer {
                        to_event: to_client_timer_event(i),
                        timer_events: &mut client.timer_events,
                        timer_id: &mut self.timer_id,
                    };
                    client.state.on_event(Recv(message), &mut timer)?
                }
                Event::Timer(TimerEvent {
                    timer_id,
                    period: _,
                    timer: Timer::Resend(i),
                }) => {
                    let client = &mut self.clients[i];
                    let i = client
                        .timer_events
                        .iter()
                        .position(|event| event.timer_id == timer_id)
                        .ok_or(anyhow::anyhow!("timer not found"))?;
                    let event = client.timer_events.remove(i);
                    client.timer_events.push(event);
                    let mut timer = AnyTimer {
                        to_event: to_client_timer_event(i),
                        timer_events: &mut client.timer_events,
                        timer_id: &mut self.timer_id,
                    };
                    client.state.on_event(erased::Resend, &mut timer)?
                }
                _ => anyhow::bail!("unexpected event"),
            }
            self.flush()
        }
    }

    impl<I: Iterator<Item = Workload>> State<I> {
        pub fn launch(&mut self) -> anyhow::Result<()> {
            for client in &mut self.clients {
                client.close_loop.launch()?
            }
            self.flush()
        }

        fn flush(&mut self) -> anyhow::Result<()> {
            for event in self.transient_message_events.try_iter() {
                self.message_events.insert(event);
            }
            for (i, client) in self.clients.iter_mut().enumerate() {
                let mut rerun = true;
                while replace(&mut rerun, false) {
                    for invoke in client.transient_invokes.try_iter() {
                        rerun = true;
                        let mut timer = AnyTimer {
                            to_event: to_client_timer_event(i),
                            timer_events: &mut client.timer_events,
                            timer_id: &mut self.timer_id,
                        };
                        client.state.on_event(invoke, &mut timer)?
                    }
                    for upcall in client.transient_upcalls.try_iter() {
                        rerun = true;
                        client.close_loop.on_event(upcall, &mut UnreachableTimer)?
                    }
                }
            }
            Ok(())
        }
    }

    struct AnyTimer<'a, F, M> {
        pub to_event: F,
        pub timer_events: &'a mut Vec<TimerEvent<M>>, // generalize the data structure?
        pub timer_id: &'a mut u32,
    }

    impl<F: FnMut(&dyn Any) -> anyhow::Result<M>, M, S> crate::event::erased::Timer<S>
        for AnyTimer<'_, F, M>
    {
        fn set<N: Clone + Send + Sync + 'static>(
            &mut self,
            period: Duration,
            event: N,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEvent<N>,
        {
            let event = (self.to_event)(&event as _)?;
            *self.timer_id += 1;
            let timer_id = *self.timer_id;
            self.timer_events.push(TimerEvent {
                timer_id,
                period,
                timer: event,
            });
            Ok(TimerId(timer_id))
        }

        fn unset(&mut self, TimerId(timer_id): TimerId) -> anyhow::Result<()> {
            let i = self
                .timer_events
                .iter()
                .position(|event| event.timer_id == timer_id)
                .ok_or(anyhow::anyhow!("timer not found"))?;
            self.timer_events.remove(i);
            Ok(())
        }
    }
}
