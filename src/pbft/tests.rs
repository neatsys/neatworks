use std::collections::BTreeSet;

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    app::KVStore,
    crypto::{
        events::{Signed, Verified},
        Crypto, Verifiable,
    },
    event::{linear::Timer, Transient},
    worker::Worker,
    workload::{CloseLoop, Invoke, InvokeOk, Workload},
};

use super::{Commit, PrePrepare, Prepare, Reply, Request};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Addr {
    Client(u8),
    Replica(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, derive_more::From)]
pub enum MessageEvent {
    Request(Request<Addr>),
    PrePrepare(Verifiable<PrePrepare>, Vec<Request<Addr>>),
    Prepare(Verifiable<Prepare>),
    Commit(Verifiable<Commit>),
    Reply(Reply),
}

#[derive(Debug, Clone, derive_more::From)]
pub enum CryptoEvent {
    SignedPrePrepare(Signed<PrePrepare>, Vec<Request<Addr>>),
    VerifiedPrePrepare(Verified<PrePrepare>, Vec<Request<Addr>>),
    SignedPrepare(Signed<Prepare>),
    VerifiedPrepare(Verified<Prepare>),
    SignedCommit(Signed<Commit>),
    VerifiedCommit(Verified<Commit>),
}

type Client = super::Client<Transient<MessageEvent>, Transient<InvokeOk>, Addr>;
type Replica = super::Replica<
    Transient<MessageEvent>,
    Transient<MessageEvent>,
    Worker<Crypto, Transient<CryptoEvent>>,
    KVStore,
    Addr,
>;

#[derive_where(PartialEq, Eq, Hash; W::Attach)]
#[derive_where(Debug, Clone; W, W::Attach)]
pub struct State<W: Workload> {
    pub clients: Vec<ClientState<W>>,
    pub replicas: Vec<Replica>,
    message_events: BTreeSet<MessageEvent>,
}

#[derive_where(PartialEq, Eq, Hash; W::Attach)]
#[derive_where(Debug, Clone; W, W::Attach)]
pub struct ClientState<W: Workload> {
    pub state: Client,
    timer: Timer,
    pub close_loop: CloseLoop<W, Transient<Invoke>>,
}
