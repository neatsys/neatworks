use bytes::Bytes;
use derive_more::From;
use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    crypto::{Crypto, Verifiable},
    event::{
        combinators::Transient, Erase, OnErasedEvent as _, SendEvent, TimerId, UntypedEvent, Work,
    },
    model::{NetworkState, ScheduleState},
    net::{combinators::All, events::Recv, SendMessage},
    workload::{app::kvstore, events::Invoke, CloseLoop, Workload},
};

use super::{
    client,
    messages::{Commit, NewView, PrePrepare, Prepare, QueryNewView, Reply, Request, ViewChange},
    replica,
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

#[derive(Debug)]
pub struct State<W> {
    pub clients: Vec<(client::State<Addr>, ClientContextState<W>)>,
    pub replicas: Vec<(ReplicaState, ReplicaContextState)>,
    network: NetworkState<Addr, Message>,
}

type ReplicaState = replica::State<kvstore::App, Addr>;

#[derive(Debug, Clone)]
#[derive_where(PartialEq, Eq, Hash)]
pub struct ClientContextState<W> {
    #[derive_where(skip)]
    pub upcall: CloseLoop<W, Option<Invoke<Bytes>>>,
    schedule: ScheduleState<Timer>,
}

struct ClientContextCarrier;

impl<'a, W> client::context::On<ClientContext<'a, W>> for ClientContextCarrier {
    type Schedule = &'a mut ScheduleState<Timer>;
}

type ClientContext<'a, W> = client::context::Context<
    ClientContextCarrier,
    NetworkContext<'a>,
    &'a mut CloseLoop<W, Option<Invoke<Bytes>>>,
    Addr,
>;

#[derive(Debug, Clone)]
#[derive_where(PartialEq, Eq, Hash)]
pub struct ReplicaContextState {
    #[derive_where(skip)]
    crypto: Crypto,
    schedule: ScheduleState<Timer>,
}

#[derive(Debug)]
pub struct ReplicaContextCarrier;

impl<'a> replica::context::On<ReplicaContext<'a, Self>, ReplicaState> for ReplicaContextCarrier {
    type CryptoWorker = Transient<Work<Crypto, Self::CryptoContext>>;
    type CryptoContext =
        crate::event::combinators::erase::Transient<ReplicaState, ReplicaContext<'a, Self>>;
    type Schedule = ScheduleState<Timer>;
}

#[derive(Debug)]
pub struct ReplicaContext<'a, O: replica::context::On<Self, ReplicaState>> {
    net: NetworkContext<'a>,
    crypto_worker: O::CryptoWorker,
    schedule: &'a mut O::Schedule,
    crypto: &'a mut Crypto,
}

impl<'a> replica::Context<ReplicaState, Addr> for ReplicaContext<'a, ReplicaContextCarrier> {
    type PeerNet = NetworkContext<'a>;
    type DownlinkNet = NetworkContext<'a>;
    type CryptoWorker =
        <ReplicaContextCarrier as replica::context::On<Self, ReplicaState>>::CryptoWorker;
    type CryptoContext =
        <ReplicaContextCarrier as replica::context::On<Self, ReplicaState>>::CryptoContext;
    type Schedule = <ReplicaContextCarrier as replica::context::On<Self, ReplicaState>>::Schedule;
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

#[derive(Debug)]
pub struct NetworkContext<'a> {
    state: &'a mut NetworkState<Addr, Message>,
    all: Vec<Addr>,
}

impl<M: Clone + Into<Message>> SendMessage<All, M> for NetworkContext<'_> {
    fn send(&mut self, All: All, message: M) -> anyhow::Result<()> {
        for addr in self.all.clone() {
            SendMessage::send(&mut self.state, addr, message.clone())?
        }
        Ok(())
    }
}

// only for client, feel lazy to make distinct wrappers for client and replica
impl<M: Into<Message>> SendMessage<u8, M> for NetworkContext<'_> {
    fn send(&mut self, remote: u8, message: M) -> anyhow::Result<()> {
        SendMessage::send(&mut self.state, Addr::Replica(remote), message)
    }
}

impl<M: Into<Message>> SendMessage<Addr, M> for NetworkContext<'_> {
    fn send(&mut self, remote: Addr, message: M) -> anyhow::Result<()> {
        SendMessage::send(&mut self.state, remote, message)
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    Message(Addr, Message),
    Timer(Addr, TimerId, Timer),
}

impl<W: Workload<Op = Bytes, Result = Bytes>> SendEvent<Event> for State<W> {
    fn send(&mut self, event: Event) -> anyhow::Result<()> {
        match event {
            Event::Message(Addr::Client(index), _) | Event::Timer(Addr::Client(index), ..) => {
                let Some((client, context)) = self.clients.get_mut(index as usize) else {
                    anyhow::bail!("missing client for index {index}")
                };
                let mut context = ClientContext {
                    net: NetworkContext {
                        state: &mut self.network,
                        all: (0..self.replicas.len() as u8).map(Addr::Replica).collect(),
                    },
                    upcall: &mut context.upcall,
                    schedule: &mut context.schedule,
                    _m: Default::default(),
                };
                match event {
                    Event::Message(_, Message::Reply(message)) => {
                        client.on_event(Recv(message), &mut context)
                    }
                    Event::Timer(_, id, Timer::ClientResend) => {
                        context.schedule.tick(id)?;
                        client.on_event(client::events::Resend, &mut context)
                    }
                    _ => anyhow::bail!("unimplemented"),
                }?;
                Self::fix_invoke(client, context)
            }
            Event::Message(Addr::Replica(index), _) | Event::Timer(Addr::Replica(index), ..) => {
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
                    schedule: &mut context.schedule,
                    crypto: &mut context.crypto,
                };
                match event {
                    Event::Message(_, Message::Request(message)) => {
                        replica.on_event(Recv(message), &mut context)
                    }
                    Event::Message(_, Message::PrePrepare(message, requests)) => {
                        replica.on_event(Recv((message, requests)), &mut context)
                    }
                    Event::Message(_, Message::Prepare(message)) => {
                        replica.on_event(Recv(message), &mut context)
                    }
                    Event::Message(_, Message::Commit(message)) => {
                        replica.on_event(Recv(message), &mut context)
                    }
                    Event::Message(_, Message::ViewChange(message)) => {
                        replica.on_event(Recv(message), &mut context)
                    }
                    Event::Message(_, Message::NewView(message)) => {
                        replica.on_event(Recv(message), &mut context)
                    }
                    Event::Message(_, Message::QueryNewView(message)) => {
                        replica.on_event(Recv(message), &mut context)
                    }
                    Event::Timer(_, id, timer) => {
                        context.schedule.tick(id)?;
                        match timer {
                            Timer::ProgressPrepare(op_num) => replica
                                .on_event(replica::events::ProgressPrepare(op_num), &mut context),
                            Timer::DoViewChange(view_num) => replica
                                .on_event(replica::events::DoViewChange(view_num), &mut context),
                            Timer::ProgressViewChange => {
                                replica.on_event(replica::events::ProgressViewChange, &mut context)
                            }
                            Timer::StateTransfer(op_num) => replica
                                .on_event(replica::events::StateTransfer(op_num), &mut context),
                            _ => anyhow::bail!("unimplemented"),
                        }
                    }
                    _ => anyhow::bail!("unimplemented"),
                }?;
                Self::fix_submit(replica, context)
            }
        }?;
        Ok(())
    }
}

impl<W: Workload<Op = Bytes, Result = Bytes>> State<W> {
    fn fix_invoke(
        client: &mut client::State<Addr>,
        mut context: ClientContext<'_, W>,
    ) -> anyhow::Result<()> {
        if let Some(invoke) = context.upcall.sender.take() {
            client.on_event(invoke, &mut context)?
        }
        Ok(())
    }

    fn fix_submit(
        replica: &mut ReplicaState,
        mut context: ReplicaContext<'_, ReplicaContextCarrier>,
    ) -> anyhow::Result<()> {
        // is it critical to preserve FIFO ordering?
        while let Some(work) = context.crypto_worker.pop() {
            // feels like there are definitely some trait impl that can be reused here, replacing
            // either the direct call to `work` or to `event`, or both
            // also, feel like there's definitely a way to express without dual Transient, one is
            // probably unavoidable but two is very likely redundant
            // that said, these are test code, should not go too paranoid on the principles (and
            // performance)...
            // (that said, i will tried, but eventually denied by lifetimes. well that i would
            // accept to not fight against)
            let mut sender = Erase::new(Transient::new());
            work(context.crypto, &mut sender)?;
            for UntypedEvent(event) in sender.drain(..) {
                event(replica, &mut context)?
            }
        }
        Ok(())
    }
}
