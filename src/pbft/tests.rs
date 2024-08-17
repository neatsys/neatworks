use bytes::Bytes;
use derive_more::From;
use serde::{Deserialize, Serialize};

use crate::{
    crypto::Verifiable,
    event::{combinators::Transient, Erase, OnErasedEvent as _, SendEvent, TimerId, UntypedEvent},
    net::{combinators::All, events::Recv, SendMessage},
    workload::{app::kvstore, Workload},
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

#[derive(Debug)]
pub struct State<CC, RC, N> {
    pub clients: Vec<(client::State<Addr>, CC)>,
    pub replicas: Vec<(ReplicaState, RC)>,
    network: N,
}

type ReplicaState = replica::State<kvstore::App, Addr>;

mod search {
    use bytes::Bytes;
    use derive_where::derive_where;

    use crate::{
        crypto::Crypto,
        event::{
            combinators::{erase::Transient as EraseTransient, Transient},
            Work,
        },
        model::search::state::{Network, Schedule},
        pbft::{client, replica},
        workload::{events::Invoke, CloseLoop, Workload},
    };

    use super::{Addr, Message, ReplicaState, Timer};

    pub type State<W> =
        super::State<ClientContextState<W>, ReplicaContextState, Network<Addr, Message>>;

    pub type NetworkContext<'a> = super::NetworkContext<'a, Network<Addr, Message>>;

    #[derive(Debug, Clone)]
    #[derive_where(PartialEq, Eq, Hash)]
    pub struct ClientContextState<W> {
        #[derive_where(skip)]
        pub upcall: CloseLoop<W, Option<Invoke<Bytes>>>,
        pub schedule: Schedule<Timer>,
    }

    pub struct ClientContext<'a, W> {
        pub net: NetworkContext<'a>,
        pub upcall: &'a mut CloseLoop<W, Option<Invoke<Bytes>>>,
        pub schedule: &'a mut Schedule<Timer>,
    }

    impl<'a, W: Workload<Op = Bytes, Result = Bytes>> client::Context<Addr> for ClientContext<'a, W> {
        type Net = NetworkContext<'a>;
        type Upcall = CloseLoop<W, Option<Invoke<Bytes>>>;
        type Schedule = Schedule<Timer>;
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

    #[derive(Debug, Clone)]
    #[derive_where(PartialEq, Eq, Hash)]
    pub struct ReplicaContextState {
        #[derive_where(skip)]
        pub crypto: Crypto,
        pub schedule: Schedule<Timer>,
    }

    pub struct ReplicaContext<'a> {
        pub net: NetworkContext<'a>,
        pub crypto: &'a mut Crypto,
        pub crypto_worker: Transient<Work<Crypto, EraseTransient<ReplicaState, Self>>>,
        pub schedule: &'a mut Schedule<Timer>,
    }

    impl<'a> replica::Context<ReplicaState, Addr> for ReplicaContext<'a> {
        type PeerNet = NetworkContext<'a>;
        type DownlinkNet = NetworkContext<'a>;
        type CryptoWorker = Transient<Work<Crypto, Self::CryptoContext>>;
        type CryptoContext = EraseTransient<ReplicaState, Self>;
        type Schedule = Schedule<Timer>;
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
}

#[derive(Debug, Clone)]
pub enum Event {
    Message(Addr, Message),
    Timer(Addr, TimerId, Timer),
}

impl<W: Workload<Op = Bytes, Result = Bytes>> SendEvent<Event> for search::State<W> {
    fn send(&mut self, event: Event) -> anyhow::Result<()> {
        match event {
            Event::Message(Addr::Client(index), _) | Event::Timer(Addr::Client(index), ..) => {
                let Some((client, context)) = self.clients.get_mut(index as usize) else {
                    anyhow::bail!("missing client for index {index}")
                };
                let mut context = search::ClientContext {
                    net: NetworkContext {
                        state: &mut self.network,
                        all: (0..self.replicas.len() as u8).map(Addr::Replica).collect(),
                    },
                    upcall: &mut context.upcall,
                    schedule: &mut context.schedule,
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
                let mut context = search::ReplicaContext {
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

impl<W: Workload<Op = Bytes, Result = Bytes>> search::State<W> {
    fn fix_invoke(
        client: &mut client::State<Addr>,
        mut context: search::ClientContext<'_, W>,
    ) -> anyhow::Result<()> {
        if let Some(invoke) = context.upcall.sender.take() {
            client.on_event(invoke, &mut context)?
        }
        Ok(())
    }

    fn fix_submit(
        replica: &mut ReplicaState,
        mut context: search::ReplicaContext<'_>,
    ) -> anyhow::Result<()> {
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
                event(replica, &mut context)?
            }
        }
        Ok(())
    }
}
