use derive_where::derive_where;

use crate::{
    cops::{DefaultVersion, DepOrd},
    crypto::Verifiable,
};

#[derive(Debug)]
pub struct Announce {
    updated: DefaultVersion,
    prev: QuorumVersion,
    merged: QuorumVersion,
}

#[derive(Debug, Clone)]
pub struct AnnounceOk {
    clock: DefaultVersion,
    index: usize,
}

#[derive(Debug, Clone)]
#[derive_where(PartialOrd, PartialEq)]
pub struct QuorumVersion {
    plain: DefaultVersion, // redundant, just for easier use
    #[derive_where(skip)]
    cert: Vec<Verifiable<AnnounceOk>>,
}

impl DepOrd for QuorumVersion {
    fn dep_cmp(&self, other: &Self, id: crate::cops::KeyId) -> std::cmp::Ordering {
        self.plain.dep_cmp(&other.plain, id)
    }

    fn deps(&self) -> impl Iterator<Item = crate::cops::KeyId> + '_ {
        self.plain.deps()
    }
}

pub struct QuorumClient {}
