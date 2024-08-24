use bytes::Bytes;
use derive_more::From;
use serde::{Deserialize, Serialize};

use crate::{
    crypto::{Crypto, Verifiable},
    event::{
        combinators::{erase::Transient as EraseTransient, Transient},
        Erase, OnErasedEvent, ScheduleEvent, UntypedEvent, Work,
    },
    net::{combinators::All, events::Recv, SendMessage},
    workload::{app::kvstore, events::Invoke, CloseLoop, Workload},
};

use super::{
    client,
    messages::{Commit, NewView, PrePrepare, Prepare, QueryNewView, Reply, Request, ViewChange},
    replica::{self, PeerNet},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Addr {
    Client(u8),
    Replica(u8),
}

impl crate::net::Addr for Addr {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, From)]
pub enum Message {
    Request(Request<Addr>),
    Reply(Reply),
    PrePrepare(Verifiable<PrePrepare>, Vec<Request<Addr>>),
    Prepare(Verifiable<Prepare>),
    Commit(Verifiable<Commit>),
    ViewChange(Verifiable<ViewChange>),
    NewView(Verifiable<NewView>),
    QueryNewView(QueryNewView),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Timer {
    ClientResend,
    DoViewChange(u32),
    ProgressPrepare(u32),
    ProgressViewChange,
    StateTransfer(u32),
}

mod timer {
    use crate::pbft::{client::events::*, replica::events::*};

    use super::Timer;

    impl From<Resend> for Timer {
        fn from(Resend: Resend) -> Self {
            Self::ClientResend
        }
    }

    impl From<DoViewChange> for Timer {
        fn from(DoViewChange(view_num): DoViewChange) -> Self {
            Self::DoViewChange(view_num)
        }
    }

    impl From<ProgressPrepare> for Timer {
        fn from(ProgressPrepare(op_num): ProgressPrepare) -> Self {
            Self::ProgressPrepare(op_num)
        }
    }

    impl From<ProgressViewChange> for Timer {
        fn from(ProgressViewChange: ProgressViewChange) -> Self {
            Self::ProgressViewChange
        }
    }

    impl From<StateTransfer> for Timer {
        fn from(StateTransfer(op_num): StateTransfer) -> Self {
            Self::StateTransfer(op_num)
        }
    }
}

#[derive(Debug, Clone)]
// `D` for timer data...not a good choice
pub enum Event<D> {
    Message(Addr, Message),
    Timer(Addr, D, Timer),
}

impl<'a, N, W: Workload<Op = Bytes, Result = Bytes>, T, D>
    OnErasedEvent<Event<D>, ClientContext<'a, N, W, T>> for client::State<Addr>
where
    ClientContext<'a, N, W, T>: client::Context<Addr>,
{
    fn on_event(
        &mut self,
        event: Event<D>,
        context: &mut ClientContext<'a, N, W, T>,
    ) -> anyhow::Result<()> {
        match event {
            Event::Message(_, Message::Reply(message)) => self.on_event(Recv(message), context),
            Event::Timer(_, _, Timer::ClientResend) => {
                // context.schedule.tick(id)?;
                self.on_event(client::events::Resend, context)
            }
            _ => anyhow::bail!("unimplemented"),
        }?;
        fix_invoke(self, context)
    }
}

impl<'a, N, T, D> OnErasedEvent<Event<D>, ReplicaContext<'a, N, T>> for ReplicaState
where
    ReplicaContext<'a, N, T>: replica::Context<ReplicaState, Addr>,
{
    fn on_event(
        &mut self,
        event: Event<D>,
        context: &mut ReplicaContext<'a, N, T>,
    ) -> anyhow::Result<()> {
        match event {
            Event::Message(_, Message::Request(message)) => self.on_event(Recv(message), context),
            Event::Message(_, Message::PrePrepare(message, requests)) => {
                self.on_event(Recv((message, requests)), context)
            }
            Event::Message(_, Message::Prepare(message)) => self.on_event(Recv(message), context),
            Event::Message(_, Message::Commit(message)) => self.on_event(Recv(message), context),
            Event::Message(_, Message::ViewChange(message)) => {
                self.on_event(Recv(message), context)
            }
            Event::Message(_, Message::NewView(message)) => self.on_event(Recv(message), context),
            Event::Message(_, Message::QueryNewView(message)) => {
                self.on_event(Recv(message), context)
            }
            Event::Timer(_, _, timer) => {
                // context.schedule.tick(id)?;
                match timer {
                    Timer::ProgressPrepare(op_num) => {
                        self.on_event(replica::events::ProgressPrepare(op_num), context)
                    }
                    Timer::DoViewChange(view_num) => {
                        self.on_event(replica::events::DoViewChange(view_num), context)
                    }
                    Timer::ProgressViewChange => {
                        self.on_event(replica::events::ProgressViewChange, context)
                    }
                    Timer::StateTransfer(op_num) => {
                        self.on_event(replica::events::StateTransfer(op_num), context)
                    }
                    _ => anyhow::bail!("unimplemented"),
                }
            }
            _ => anyhow::bail!("unimplemented"),
        }?;
        fix_submit(self, context)
    }
}

fn fix_invoke<'a, N, W: Workload<Op = Bytes, Result = Bytes>, T>(
    client: &mut client::State<Addr>,
    context: &mut ClientContext<'a, N, W, T>,
) -> anyhow::Result<()>
where
    ClientContext<'a, N, W, T>: client::Context<Addr>,
{
    if let Some(invoke) = context.upcall.sender.take() {
        client.on_event(invoke, context)?
    }
    Ok(())
}

fn fix_submit<'a, N, T>(
    replica: &mut ReplicaState,
    context: &mut ReplicaContext<'a, N, T>,
) -> anyhow::Result<()>
where
    ReplicaContext<'a, N, T>: replica::Context<ReplicaState, Addr>,
{
    // is it critical to preserve FIFO ordering?
    while let Some(work) = context.crypto_worker.pop() {
        // feels like there are definitely some trait impl that can be reused here, replacing
        // either the direct call to `work` or to `event`, or both
        // also, feel like there's definitely a way to express without dual Transient, one is
        // probably unavoidable but two is very likely redundant
        // that said, these are test code, should not go too paranoid on the principles (and
        // performance)...
        // (that said, i still tried but eventually got denied by lifetimes. well that i would
        // accept to not fight against)
        let mut sender = Erase::new(Transient::new());
        work(context.crypto, &mut sender)?;
        for UntypedEvent(event) in sender.drain(..) {
            event(replica, context)?
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct NetworkContext<'a, N> {
    state: &'a mut N,
    all: Vec<Addr>,
}

impl<N: SendMessage<Addr, M>, M: Clone> SendMessage<All, M> for NetworkContext<'_, N> {
    fn send(&mut self, All: All, message: M) -> anyhow::Result<()> {
        for addr in self.all.clone() {
            SendMessage::send(self.state, addr, message.clone())?
        }
        Ok(())
    }
}

// only for client, feel lazy to make distinct wrappers for client and replica
impl<N: SendMessage<Addr, M>, M> SendMessage<u8, M> for NetworkContext<'_, N> {
    fn send(&mut self, remote: u8, message: M) -> anyhow::Result<()> {
        SendMessage::send(self.state, Addr::Replica(remote), message)
    }
}

impl<N: SendMessage<Addr, M>, M> SendMessage<Addr, M> for NetworkContext<'_, N> {
    fn send(&mut self, remote: Addr, message: M) -> anyhow::Result<()> {
        SendMessage::send(self.state, remote, message)
    }
}

type ReplicaState = replica::State<kvstore::App, Addr>;

pub struct ClientContext<'a, N, W, T> {
    pub net: N,
    pub upcall: &'a mut CloseLoop<W, Option<Invoke<Bytes>>>,
    pub schedule: &'a mut T,
}

impl<'a, N, W: Workload<Op = Bytes, Result = Bytes>, T> client::Context<Addr>
    for ClientContext<'a, N, W, T>
where
    N: SendMessage<u8, Request<Addr>> + SendMessage<All, Request<Addr>>,
    T: ScheduleEvent<client::events::Resend>,
{
    type Net = N;
    type Upcall = CloseLoop<W, Option<Invoke<Bytes>>>;
    type Schedule = T;
    fn net(&mut self) -> &mut Self::Net {
        &mut self.net
    }
    fn upcall(&mut self) -> &mut Self::Upcall {
        self.upcall
    }
    fn schedule(&mut self) -> &mut Self::Schedule {
        self.schedule
    }
}

pub struct ReplicaContext<'a, N, T> {
    pub net: N,
    pub crypto: &'a mut Crypto,
    pub crypto_worker: Transient<Work<Crypto, EraseTransient<ReplicaState, Self>>>,
    pub schedule: &'a mut T,
}

impl<'a, N, T> replica::Context<ReplicaState, Addr> for ReplicaContext<'a, N, T>
where
    N: PeerNet<Addr> + SendMessage<Addr, Reply>,
    T: replica::Schedule,
{
    type PeerNet = N;
    type DownlinkNet = N;
    type CryptoWorker = Transient<Work<Crypto, Self::CryptoContext>>;
    type CryptoContext = EraseTransient<ReplicaState, Self>;
    type Schedule = T;
    fn peer_net(&mut self) -> &mut Self::PeerNet {
        &mut self.net
    }
    fn downlink_net(&mut self) -> &mut Self::DownlinkNet {
        &mut self.net
    }
    fn crypto_worker(&mut self) -> &mut Self::CryptoWorker {
        &mut self.crypto_worker
    }
    fn schedule(&mut self) -> &mut Self::Schedule {
        self.schedule
    }
}

mod search {
    use std::borrow::Borrow;

    use bytes::Bytes;
    use derive_where::derive_where;

    use crate::{
        crypto::Crypto,
        event::{combinators::Transient, OnErasedEvent as _, SendEvent},
        model::search::state::{Network, Schedule, TimerId},
        pbft::{client, replica},
        workload::{events::Invoke, CloseLoop, Workload},
    };

    use super::{Addr, Message, NetworkContext, ReplicaState, Timer};

    #[derive(Debug)]
    pub struct State<W, N> {
        pub clients: Vec<(client::State<Addr>, ClientContextState<W>)>,
        pub replicas: Vec<(ReplicaState, ReplicaContextState)>,
        network: N,
    }

    #[derive(Debug, Clone)]
    #[derive_where(PartialEq, Eq, Hash)]
    pub struct ClientContextState<W> {
        #[derive_where(skip)]
        pub upcall: CloseLoop<W, Option<Invoke<Bytes>>>,
        pub schedule: Schedule<Timer>,
    }

    #[derive(Debug, Clone)]
    #[derive_where(PartialEq, Eq, Hash)]
    pub struct ReplicaContextState {
        #[derive_where(skip)]
        pub crypto: Crypto,
        pub schedule: Schedule<Timer>,
    }

    pub type ClientContext<'a, N, W> =
        super::ClientContext<'a, NetworkContext<'a, N>, W, Schedule<Timer>>;
    pub type ReplicaContext<'a, N> =
        super::ReplicaContext<'a, NetworkContext<'a, N>, Schedule<Timer>>;

    pub type Event = super::Event<TimerId>;

    impl<W: Workload<Op = Bytes, Result = Bytes>, N> SendEvent<Event> for State<W, N>
    where
        for<'a> ClientContext<'a, N, W>: client::Context<Addr>,
        for<'a> ReplicaContext<'a, N>: replica::Context<ReplicaState, Addr>,
    {
        fn send(&mut self, event: Event) -> anyhow::Result<()> {
            match event {
                Event::Message(Addr::Client(index), _) | Event::Timer(Addr::Client(index), ..) => {
                    let Some((client, context)) = self.clients.get_mut(index as usize) else {
                        anyhow::bail!("missing client for index {index}")
                    };
                    if let Event::Timer(_, id, _) = event {
                        context.schedule.tick(id)?
                    }
                    let mut context = ClientContext {
                        net: NetworkContext {
                            state: &mut self.network,
                            all: (0..self.replicas.len() as u8).map(Addr::Replica).collect(),
                        },
                        upcall: &mut context.upcall,
                        schedule: &mut context.schedule,
                    };
                    client.on_event(event, &mut context)
                }
                Event::Message(Addr::Replica(index), _)
                | Event::Timer(Addr::Replica(index), ..) => {
                    let all = (0..self.replicas.len() as u8)
                        .filter(|id| *id != index)
                        .map(Addr::Replica)
                        .collect();
                    let Some((replica, context)) = self.replicas.get_mut(index as usize) else {
                        anyhow::bail!("missing replica for index {index}")
                    };
                    if let Event::Timer(_, id, _) = event {
                        context.schedule.tick(id)?
                    }
                    let mut context = ReplicaContext {
                        net: NetworkContext {
                            state: &mut self.network,
                            all,
                        },
                        crypto_worker: Transient::new(),
                        schedule: &mut context.schedule,
                        crypto: &mut context.crypto,
                    };
                    replica.on_event(event, &mut context)
                }
            }?;
            Ok(())
        }
    }

    impl<W: Workload<Op = Bytes, Result = Bytes>, N> crate::model::search::State for State<W, N>
    where
        for<'a> ClientContext<'a, N, W>: client::Context<Addr>,
        for<'a> ReplicaContext<'a, N>: replica::Context<ReplicaState, Addr>,
        N: Borrow<Network<Addr, Message>>,
    {
        type Event = Event;

        fn events(&self) -> impl Iterator<Item = Self::Event> + '_ {
            let client_timers =
                self.clients
                    .iter()
                    .enumerate()
                    .flat_map(|(index, (_, context))| {
                        assert!(context.upcall.sender.is_none());
                        context.schedule.events().map(move |(id, event)| {
                            Event::Timer(Addr::Client(index as _), id, event)
                        })
                    });
            let replica_timers =
                self.replicas
                    .iter()
                    .enumerate()
                    .flat_map(|(index, (_, context))| {
                        context.schedule.events().map(move |(id, event)| {
                            Event::Timer(Addr::Replica(index as _), id, event)
                        })
                    });
            self.network
                .borrow()
                .events()
                .map(|(addr, message)| Event::Message(addr, message))
                .chain(client_timers)
                .chain(replica_timers)
        }
    }
}

mod simulate {
    use std::borrow::BorrowMut;

    use arbtest::arbitrary::Unstructured;
    use bytes::Bytes;
    use derive_where::derive_where;

    use crate::{
        crypto::Crypto,
        event::{combinators::Transient, OnErasedEvent as _, ScheduleEvent},
        model::simulate::{NetworkState, ProgressExhausted, Temporal},
        pbft::{client, replica},
        workload::{events::Invoke, CloseLoop, Workload},
    };

    use super::{Addr, Message, NetworkContext, ReplicaState, Timer};

    #[derive(Debug)]
    pub struct State<W, N> {
        pub clients: Vec<(client::State<Addr>, ClientContextState<W>)>,
        pub replicas: Vec<(ReplicaState, ReplicaContextState)>,
        network: N,
        schedule: Temporal<Event>,
    }

    #[derive(Debug, Clone)]
    #[derive_where(PartialEq, Eq, Hash)]
    pub struct ClientContextState<W> {
        #[derive_where(skip)]
        pub upcall: CloseLoop<W, Option<Invoke<Bytes>>>,
    }

    #[derive(Debug, Clone)]
    #[derive_where(PartialEq, Eq, Hash)]
    pub struct ReplicaContextState {
        #[derive_where(skip)]
        pub crypto: Crypto,
    }

    pub type ClientContext<'a, N, W> =
        super::ClientContext<'a, NetworkContext<'a, N>, W, Schedule<'a>>;
    pub type ReplicaContext<'a, N> = super::ReplicaContext<'a, NetworkContext<'a, N>, Schedule<'a>>;

    pub type Event = super::Event<()>;

    pub struct Schedule<'a> {
        addr: super::Addr,
        temporal: &'a mut Temporal<Event>,
    }

    impl<M: Into<Timer>> ScheduleEvent<M> for Schedule<'_> {
        fn set(
            &mut self,
            period: std::time::Duration,
            event: M,
        ) -> anyhow::Result<crate::event::ActiveTimer>
        where
            M: Send + Clone + 'static,
        {
            self.temporal
                .set(period, super::Event::Timer(self.addr, (), event.into()))
        }

        fn unset(&mut self, id: crate::event::ActiveTimer) -> anyhow::Result<()> {
            self.temporal.unset(id)
        }
    }

    impl<W: Workload<Op = Bytes, Result = Bytes>, N> State<W, N>
    where
        for<'a> ClientContext<'a, N, W>: client::Context<Addr>,
        for<'a> ReplicaContext<'a, N>: replica::Context<ReplicaState, Addr>,
        N: BorrowMut<NetworkState<Addr, Message>>,
    {
        pub fn step(
            &mut self,
            u: &mut Unstructured,
            temporal: &mut Temporal<Event>,
        ) -> anyhow::Result<()> {
            let event = match self.network.borrow_mut().choose(u) {
                Ok((addr, message)) => Event::Message(addr, message),
                Err(err) if err.is::<ProgressExhausted>() => temporal.pop()?,
                Err(err) => return Err(err),
            };
            match event {
                Event::Message(addr @ Addr::Client(index), _)
                | Event::Timer(addr @ Addr::Client(index), ..) => {
                    let Some((client, context)) = self.clients.get_mut(index as usize) else {
                        anyhow::bail!("missing client for index {index}")
                    };
                    let mut context = ClientContext {
                        net: NetworkContext {
                            state: &mut self.network,
                            all: (0..self.replicas.len() as u8).map(Addr::Replica).collect(),
                        },
                        upcall: &mut context.upcall,
                        schedule: &mut Schedule { addr, temporal },
                    };
                    client.on_event(event, &mut context)
                }
                Event::Message(addr @ Addr::Replica(index), _)
                | Event::Timer(addr @ Addr::Replica(index), ..) => {
                    let all = (0..self.replicas.len() as u8)
                        .filter(|id| *id != index)
                        .map(Addr::Replica)
                        .collect();
                    let Some((replica, context)) = self.replicas.get_mut(index as usize) else {
                        anyhow::bail!("missing replica for index {index}")
                    };
                    let mut context = ReplicaContext {
                        net: NetworkContext {
                            state: &mut self.network,
                            all,
                        },
                        crypto_worker: Transient::new(),
                        schedule: &mut Schedule { addr, temporal },
                        crypto: &mut context.crypto,
                    };
                    replica.on_event(event, &mut context)
                }
            }
        }
    }
}
