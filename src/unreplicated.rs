use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    event::{erased::OnEvent as On, OnEvent, OnTimer, SendEvent, Timer, TimerId},
    net::{deserialize, events::Recv, Addr, MessageNet, SendMessage},
    util::{Payload, Request},
    workload::{Invoke, InvokeOk},
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

#[derive(Debug)]
pub enum ClientEvent {
    Invoke(Payload),
    Ingress(Reply),
}

impl From<Invoke> for ClientEvent {
    fn from(Invoke(op): Invoke) -> Self {
        Self::Invoke(op)
    }
}

pub trait ClientUpcall: SendEvent<InvokeOk> {}
impl<T: SendEvent<InvokeOk>> ClientUpcall for T {}

#[derive(Clone)]
#[derive_where(Debug, PartialEq, Eq, Hash; A)]
pub struct Client<N, U, A> {
    id: u32,
    addr: A,
    seq: u32,
    invoke: Option<ClientInvoke>,

    #[derive_where(skip)]
    net: N,
    #[derive_where(skip)]
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

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent for Client<N, U, A> {
    type Event = ClientEvent;

    fn on_event(&mut self, event: Self::Event, timer: &mut impl Timer) -> anyhow::Result<()> {
        match event {
            ClientEvent::Invoke(op) => On::on_event(self, Invoke(op), timer),
            ClientEvent::Ingress(reply) => On::on_event(self, Recv(reply), timer),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> On<Invoke> for Client<N, U, A> {
    fn on_event(&mut self, Invoke(op): Invoke, timer: &mut impl Timer) -> anyhow::Result<()> {
        anyhow::ensure!(self.invoke.is_none(), "concurrent invocation");
        self.seq += 1;
        let invoke = ClientInvoke {
            op,
            resend_timer: timer.set(Duration::from_millis(1000))?,
        };
        self.invoke = Some(invoke);
        self.do_send()
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnTimer for Client<N, U, A> {
    fn on_timer(&mut self, timer_id: TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        // TODO logging
        assert_eq!(self.invoke.as_ref().unwrap().resend_timer, timer_id);
        self.do_send()
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> On<Recv<Reply>> for Client<N, U, A> {
    fn on_event(&mut self, Recv(reply): Recv<Reply>, timer: &mut impl Timer) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(invoke) = self.invoke.take() else {
            return Ok(());
        };
        timer.unset(invoke.resend_timer)?;
        self.upcall.send((self.id, reply.result))
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> Client<N, U, A> {
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
    Dummy, // prevent unrealistic optimization in the typed message deployment
}

#[derive(Clone)]
#[derive_where(Debug, PartialEq, Eq, Hash; S)]
pub struct Replica<S, N, A> {
    replies: BTreeMap<u32, Reply>,
    app: S,
    #[derive_where(skip)]
    net: N,
    _addr_marker: std::marker::PhantomData<A>,
}

impl<S, N, A> Replica<S, N, A> {
    pub fn new(app: S, net: N) -> Self {
        Self {
            app,
            net,
            replies: Default::default(),
            _addr_marker: Default::default(),
        }
    }
}

impl<S: App, N: ToClientNet<A>, A> OnEvent for Replica<S, N, A> {
    type Event = ReplicaEvent<A>;

    fn on_event(&mut self, event: Self::Event, timer: &mut impl Timer) -> anyhow::Result<()> {
        match event {
            ReplicaEvent::Ingress(request) => On::on_event(self, Recv(request), timer),
            ReplicaEvent::Dummy => unreachable!(),
        }
    }
}

impl<S: App, N: ToClientNet<A>, A> On<Recv<Request<A>>> for Replica<S, N, A> {
    fn on_event(
        &mut self,
        Recv(request): Recv<Request<A>>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
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

impl<S, N, A> OnTimer for Replica<S, N, A> {
    fn on_timer(&mut self, _: TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        unreachable!()
    }
}

pub type ToClientMessageNet<T> = MessageNet<T, Reply>;

pub fn to_client_on_buf(
    buf: &[u8],
    sender: &mut impl SendEvent<ClientEvent>,
) -> anyhow::Result<()> {
    sender.send(ClientEvent::Ingress(deserialize(buf)?))
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, Request<A>>;

pub fn to_replica_on_buf<A: Addr>(
    buf: &[u8],
    sender: &mut impl SendEvent<ReplicaEvent<A>>,
) -> anyhow::Result<()> {
    sender.send(ReplicaEvent::Ingress(deserialize(buf)?))
}

pub mod erased {
    use crate::{
        app::App,
        event::SendEvent,
        net::{deserialize, events::Recv, Addr},
        util::Request,
    };

    use super::{ClientUpcall, Reply, ToClientNet, ToReplicaNet};

    pub type Client<A> = super::Client<
        Box<dyn ToReplicaNet<A> + Send + Sync>,
        Box<dyn ClientUpcall + Send + Sync>,
        A,
    >;

    pub fn to_client_on_buf(
        buf: &[u8],
        sender: &mut impl SendEvent<Recv<Reply>>,
    ) -> anyhow::Result<()> {
        sender.send(Recv(deserialize(buf)?))
    }

    pub type Replica<A> =
        super::Replica<Box<dyn App + Send + Sync>, Box<dyn ToClientNet<A> + Send + Sync>, A>;

    pub fn to_replica_on_buf<A: Addr>(
        buf: &[u8],
        sender: &mut impl SendEvent<Recv<Request<A>>>,
    ) -> anyhow::Result<()> {
        sender.send(Recv(deserialize(buf)?))
    }
}

pub mod check {
    use std::{
        collections::{BTreeMap, BTreeSet},
        mem::replace,
    };

    use derive_where::derive_where;
    use serde::{Deserialize, Serialize};

    use crate::{
        app::KVStore,
        event::{
            erased::{events::Init, OnEvent},
            linear::Timer,
            OnTimer as _, TimerId, Transient, UnreachableTimer,
        },
        net::{events::Recv, SendMessage},
        util::Request,
        workload::{CloseLoop, Invoke, InvokeOk, Workload},
    };

    use super::Reply;

    // we don't really do (de)serialization on this address type, just to conform `Addr`
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub enum Addr {
        Client(usize),
        Replica,
    }

    type Client = super::Client<Transient<MessageEvent>, Transient<InvokeOk>, Addr>;
    type Replica = super::Replica<KVStore, Transient<MessageEvent>, Addr>;

    #[derive_where(PartialEq, Eq, Hash; W::Attach)]
    #[derive_where(Debug, Clone; W, W::Attach)]
    pub struct State<W: Workload> {
        pub clients: Vec<ClientState<W>>,
        pub replica: Replica,
        message_events: BTreeSet<MessageEvent>,
    }

    #[derive_where(PartialEq, Eq, Hash; W::Attach)]
    #[derive_where(Debug, Clone; W, W::Attach)]
    pub struct ClientState<W: Workload> {
        pub state: Client,
        timer: Timer,
        pub close_loop: CloseLoop<W, Transient<Invoke>>,
    }

    #[derive(Debug, Clone)]
    pub enum Event {
        Message(MessageEvent),
        Timer(TimerEvent),
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct MessageEvent {
        dest: Addr,
        message: Message,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From)]
    enum Message {
        Request(Request<Addr>),
        Reply(Reply),
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct TimerEvent {
        timer_id: TimerId,
        client_index: usize,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DryReplica {
        replies: BTreeMap<u32, Reply>,
        app: KVStore,
    }

    impl<M: Into<Message>> SendMessage<Addr, M> for Transient<MessageEvent> {
        fn send(&mut self, dest: Addr, message: M) -> anyhow::Result<()> {
            self.push(MessageEvent {
                dest,
                message: message.into(),
            });
            Ok(())
        }
    }

    impl<M: Into<Message>> SendMessage<u8, M> for Transient<MessageEvent> {
        fn send(&mut self, dest: u8, message: M) -> anyhow::Result<()> {
            assert_eq!(dest, 0);
            self.push(MessageEvent {
                dest: Addr::Replica,
                message: message.into(),
            });
            Ok(())
        }
    }

    impl<W: Workload> Default for State<W> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<W: Workload> State<W> {
        pub fn new() -> Self {
            Self {
                replica: Replica::new(KVStore::new(), Transient::default()),
                clients: Default::default(),
                message_events: Default::default(),
            }
        }

        pub fn push_client(&mut self, workload: W) -> anyhow::Result<()> {
            let index = self.clients.len();
            self.clients.push(ClientState {
                state: Client::new(
                    index as u32 + 1000,
                    Addr::Client(index),
                    Transient::default(),
                    Transient::default(),
                ),
                timer: Timer::default(),
                close_loop: CloseLoop::new(Transient::default(), workload),
            });
            Ok(())
        }
    }

    impl<W: Clone + Workload> crate::search::State for State<W>
    where
        W::Attach: Clone,
    {
        type Event = Event;

        fn events(&self) -> Vec<Self::Event> {
            let mut events = self
                .message_events
                .iter()
                .cloned()
                .map(Event::Message)
                .collect::<Vec<_>>();
            for (index, client) in self.clients.iter().enumerate() {
                events.extend(client.timer.events().into_iter().map(|timer_id| {
                    Event::Timer(TimerEvent {
                        timer_id,
                        client_index: index,
                    })
                }));
            }
            events
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
                    dest: Addr::Client(index),
                    message: Message::Reply(message),
                }) => {
                    let client = &mut self.clients[index];
                    client.state.on_event(Recv(message), &mut client.timer)?
                }
                Event::Timer(TimerEvent {
                    timer_id,
                    client_index: index,
                }) => {
                    let client = &mut self.clients[index];
                    client.timer.step_timer(&timer_id)?;
                    client.state.on_timer(timer_id, &mut client.timer)?
                }
                _ => anyhow::bail!("unexpected event {event:?}"),
            }
            self.flush()
        }
    }

    impl<W: Workload> State<W> {
        pub fn launch(&mut self) -> anyhow::Result<()> {
            for client in &mut self.clients {
                client.close_loop.on_event(Init, &mut UnreachableTimer)?
            }
            self.flush()
        }

        fn flush(&mut self) -> anyhow::Result<()> {
            self.message_events.extend(self.replica.net.drain(..));
            for client in &mut self.clients {
                self.message_events.extend(client.state.net.drain(..));
                let mut rerun = true;
                while replace(&mut rerun, false) {
                    for invoke in client.close_loop.sender.drain(..) {
                        rerun = true;
                        client.state.on_event(invoke, &mut client.timer)?
                    }
                    for upcall in client.state.upcall.drain(..) {
                        rerun = true;
                        client.close_loop.on_event(upcall, &mut UnreachableTimer)?
                    }
                }
            }
            Ok(())
        }
    }
}
// notes about this iteration of model checking design. code below may get
// extracted into a reusable module that generally supports model checking of
// various protocols in latter days, but the code that matters for this
// discussion will probably stay here
// (add: the extraction has been done. everything still here will remain here)
//
// although trying out searching in state space is more as a journey out of
// curious, the `impl State` that aggregates node states and network state and
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
// the DSLabs approach, while a simulation test is kind of a different thing)
//
// the `impl OnEvent` owns `impl SendEvent` in this codebase. it's a tradeoff
// on interface and code organization that so far seems good in other part of
// the current codebase. it does expose some restrictions for model checking
// though
//
// there are almost only two choices for implementing simulation sender: either
// channel sender, or shared mutable data structure which in Rust must be
// guarded by `Mutex` or similar. both SpecPaxos and DSLabs go with the second
// choice (with unsafe bare metal data structure that carefully used).
//
// as far as i know the common choice of model checking is to send the events by
// returning them from event handlers (as the "side effect" of event handling),
// instead of by invoking on some owned internal objects. that approach comes
// with its own performance and engineering overhead so i did not take it
// neither
//
// i instead take a (rather clever) alternative approach based on the second
// choice. the `impl SendEvent` is simple buffer that owns events. while
// `impl OnEvent` owns `impl SendEvent`, `impl State` owns `impl OnEvent`, so
// when necessary it just inspect all the way down to the buffer and take the
// events out. so far the most significant downside is that the `impl SendEvent`
// type must be in clear text i.e. cannot be `dyn SendEvent`. so more verbose
// type parameters is the cost of model checking. anyway there's alternative
// approach to add a optional method on `SendEvent` trait which takes all
// buffered events out and dedicate to model checking usage
//
// the codebase intentionally be flexible on event types. indeed, it does not
// perform model checking for networking apps, but perform model checking for
// general event-driven apps. each `impl State` defines its own set of events
// that to be exposed to model checker for reordering, duplicating and dropping,
// and other events e.g. the `Invoke` and `InvokeOk` between clients and close
// loops can be considered as internal events and hided from model checker since
// it is little useful to e.g. reorder them with message/timer events.
//
// the more flexibility results in a weaker framework that can write less
// generic code for common case (since we indeed assume less common cases),
// which results in more boilerplate on app side to swallow that complexity. we
// already saw that in various artifact executables, and now it's getting worse:
// we will have to repeat the boilerplate for every protocol that demands unit
// tests, which is...every protocol
//
// actually, it's even worse than the artifact boilerplate. here we must unify
// every event type down to single representation, in order to have one "model"
// that has full control of what's happening first and what's happening later.
// (type erased interfaces, while perform this unification automatically, also
// turn every event into a universal blackbox that is highly undesirable for
// testing.)
//
// as the result, we have to polyfill a lot of event hierarchy, and adaptions
// into/out of it. we must have the "into" adapter on `impl SendEvent` and
// `impl Timer` side, and have the "out of" adapter on `State::step` (and
// `State::flush` below, which is tentatively decided as the conventional method
// for consuming internal events without consulting model checker). what makes
// it a problem is that all these boilerplate have to be protocol specific.
// there's hardly any code to be reused across protocols
//
// that's it. i have been continuously simplifying the required boilerplate, but
// many of it still persists, and must be available before writing even single
//"hello world" level unit test. all these probably have interior complexity and
// can only be abstracted at least at derivable macros level, that is, we cannot
// write a bunch of trait and let compiler to generate them automatically. i
// guess this is the cost we have to pay to perform model checking
// * in a static-typed language
// * which happens to lack runtime reflection
// * and demands to support protocols that may/can do anything
// fortunately the performance speedup kind of pays off. however, when things
// come to unit testing, a less-effort approach is very desirable

// cSpell:words paxos blackbox upcall
