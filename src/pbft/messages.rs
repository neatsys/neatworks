use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    codec::Payload,
    crypto::{Verifiable, H256},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub seq: u32,
    pub op: Payload,
    pub client_id: u32,
    pub client_addr: A,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PrePrepare {
    pub view_num: u32,
    pub op_num: u32,
    pub digest: H256,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Prepare {
    pub view_num: u32,
    pub op_num: u32,
    pub digest: H256,
    pub replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Commit {
    pub view_num: u32,
    pub op_num: u32,
    pub digest: H256,
    pub replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Reply {
    pub seq: u32,
    pub result: Payload,
    pub view_num: u32,
    pub replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ViewChange {
    pub view_num: u32,
    pub log: Vec<(Verifiable<PrePrepare>, Quorum<Prepare>)>,
    pub replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NewView {
    pub view_num: u32,
    pub view_changes: Quorum<ViewChange>,
    pub pre_prepares: Vec<Verifiable<PrePrepare>>,
    // the `min_s` and `max_s` are implied by the op numbers of `pre_prepares`
    // new primary should always send nonempty `pre_prepares`, pad a no-op if necessary
}

// TODO currently every NewView covers the "whole history", so a single NewView can bring a replica
// into the view no matter where it previously was at
// after checkpoint is implemented, QueryNewView should be with the latest checkpoint this replica
// knows, and all the necessary sub-map of the `new_views` should be replied (which probably implies
// yet another message type, sending multiple NewView should work but only in a poor fashion)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QueryNewView {
    pub view_num: u32,
    pub replica_id: u8,
}

pub type Quorum<M> = BTreeMap<u8, Verifiable<M>>;
pub type Quorums<K, M> = BTreeMap<K, Quorum<M>>;
