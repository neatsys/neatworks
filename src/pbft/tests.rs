use bytes::Bytes;
use derive_more::From;
use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    crypto::{Crypto, Verifiable},
    event::{
        combinators::{Inline, Transient},
        Erase, UntypedEvent,
    },
    model::{NetworkState, ScheduleState},
    workload::{app::kvstore, events::Invoke, CloseLoop},
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

impl From<client::events::Resend> for Timer {
    fn from(client::events::Resend: client::events::Resend) -> Self {
        Self::ClientResend
    }
}

impl From<replica::events::DoViewChange> for Timer {
    fn from(replica::events::DoViewChange(view_num): replica::events::DoViewChange) -> Self {
        Self::DoViewChange(view_num)
    }
}

impl From<replica::events::ProgressPrepare> for Timer {
    fn from(replica::events::ProgressPrepare(op_num): replica::events::ProgressPrepare) -> Self {
        Self::ProgressPrepare(op_num)
    }
}

impl From<replica::events::ProgressViewChange> for Timer {
    fn from(replica::events::ProgressViewChange: replica::events::ProgressViewChange) -> Self {
        Self::ProgressViewChange
    }
}

impl From<replica::events::StateTransfer> for Timer {
    fn from(replica::events::StateTransfer(op_num): replica::events::StateTransfer) -> Self {
        Self::StateTransfer(op_num)
    }
}

#[derive(Debug)]
pub struct State<W> {
    pub clients: Vec<(client::State<Addr>, ClientLocalContext<W>)>,
    pub replicas: Vec<(ReplicaState, ReplicaLocalContext)>,
}

type ReplicaState = replica::State<kvstore::App, Addr>;

#[derive(Debug, Clone)]
#[derive_where(PartialEq, Eq, Hash)]
pub struct ClientLocalContext<W> {
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
    &'a mut NetworkState<Addr, Message>,
    &'a mut CloseLoop<W, Option<Invoke<Bytes>>>,
    Addr,
>;

#[derive(Debug, Clone)]
#[derive_where(PartialEq, Eq, Hash)]
pub struct ReplicaLocalContext {
    #[derive_where(skip)]
    crypto: Crypto,
    schedule: ScheduleState<Timer>,
}

#[derive(Debug)]
pub struct ReplicaContextCarrier;

impl<'a> replica::context::On<ReplicaContext<'a>, ReplicaState> for ReplicaContextCarrier {
    type CryptoWorker = Inline<&'a mut Crypto, &'a mut Self::CryptoContext>;
    type CryptoContext = Erase<
        ReplicaState,
        ReplicaContext<'a>,
        Transient<UntypedEvent<ReplicaState, ReplicaContext<'a>>>,
    >;
    type Schedule = &'a mut ScheduleState<Timer>;
}

pub type ReplicaContext<'a> = replica::context::Context<
    ReplicaContextCarrier,
    &'a mut NetworkState<Addr, Message>,
    &'a mut NetworkState<Addr, Message>,
    ReplicaState,
>;
