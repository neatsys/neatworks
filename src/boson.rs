use std::collections::HashMap;

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    cops::{self, DefaultVersion, DepOrd},
    crypto::{Crypto, Verifiable},
    event::{erased::OnEvent, SendEvent},
    lamport_mutex,
    net::{events::Recv, All, SendMessage},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Announce {
    prev: QuorumClock,
    merged: Vec<QuorumClock>,
    id: u64,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct AnnounceOk {
    plain: DefaultVersion,
    id: u64,
    signer_id: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[derive_where(PartialOrd, PartialEq)]
pub struct QuorumClock {
    plain: DefaultVersion, // redundant, just for easier use
    #[derive_where(skip)]
    cert: Vec<Verifiable<AnnounceOk>>,
}

impl DepOrd for QuorumClock {
    fn dep_cmp(&self, other: &Self, id: crate::cops::KeyId) -> std::cmp::Ordering {
        self.plain.dep_cmp(&other.plain, id)
    }

    fn deps(&self) -> impl Iterator<Item = crate::cops::KeyId> + '_ {
        self.plain.deps()
    }
}

impl QuorumClock {
    pub fn verify(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        if self.plain == DefaultVersion::default() {
            anyhow::ensure!(self.cert.is_empty()); // not necessary, just as sanity check
            return Ok(());
        }
        anyhow::ensure!(self.cert.len() > num_faulty);
        let indexes = self
            .cert
            .iter()
            .map(|verifiable| verifiable.signer_id)
            .collect::<Vec<_>>();
        crypto.verify_batched(&indexes, &self.cert)
    }
}

impl From<cops::events::Update<QuorumClock>> for Announce {
    fn from(value: cops::events::Update<QuorumClock>) -> Self {
        Self {
            prev: value.prev,
            merged: value.deps,
            id: value.id,
        }
    }
}

pub struct QuorumClient<U, N> {
    num_faulty: usize,
    working_announces: HashMap<u64, WorkingAnnounce>,
    upcall: U,
    net: N,
}

struct WorkingAnnounce {
    plain: DefaultVersion,
    replies: HashMap<usize, Verifiable<AnnounceOk>>,
}

// technically should define two dedicated events for interaction with client
// user, but reusing `Announce` and `(u64, QuorumClock)` seems to be ok

impl<U, N: SendMessage<All, Announce>> OnEvent<Announce> for QuorumClient<U, N> {
    fn on_event(
        &mut self,
        announce: Announce,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let replaced = self.working_announces.insert(
            announce.id,
            WorkingAnnounce {
                plain: announce.prev.plain.clone(),
                replies: Default::default(),
            },
        );
        anyhow::ensure!(
            replaced.is_none(),
            "concurrent announce on id {}",
            announce.id
        );
        self.net.send(All, announce)
    }
}

impl<U: SendEvent<(u64, QuorumClock)>, N> OnEvent<Recv<Verifiable<AnnounceOk>>>
    for QuorumClient<U, N>
{
    fn on_event(
        &mut self,
        Recv(announce_ok): Recv<Verifiable<AnnounceOk>>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let Some(working_state) = self.working_announces.get_mut(&announce_ok.id) else {
            return Ok(());
        };
        if working_state.plain != announce_ok.plain {
            return Ok(());
        }
        working_state
            .replies
            .insert(announce_ok.signer_id, announce_ok.clone());
        if working_state.replies.len() > self.num_faulty {
            let working_state = self.working_announces.remove(&announce_ok.id).unwrap();
            let announce_ok = announce_ok.into_inner();
            let clock = QuorumClock {
                plain: announce_ok.plain,
                cert: working_state.replies.into_values().collect(),
            };
            self.upcall.send((announce_ok.id, clock))?
        }
        Ok(())
    }
}

pub struct Lamport<E>(pub E, pub u64);

impl<E: SendEvent<Announce>> SendEvent<lamport_mutex::events::Update<QuorumClock>> for Lamport<E> {
    fn send(&mut self, update: lamport_mutex::Update<QuorumClock>) -> anyhow::Result<()> {
        let announce = Announce {
            prev: update.prev,
            merged: vec![update.remote],
            id: self.1,
        };
        self.0.send(announce)
    }
}

impl<E: SendEvent<lamport_mutex::events::UpdateOk<QuorumClock>>> SendEvent<(u64, QuorumClock)>
    for Lamport<E>
{
    fn send(&mut self, (id, clock): (u64, QuorumClock)) -> anyhow::Result<()> {
        anyhow::ensure!(id == self.1);
        self.0.send(lamport_mutex::events::UpdateOk(clock))
    }
}

pub struct Cops<E>(pub E);

impl<E: SendEvent<Announce>> SendEvent<cops::events::Update<QuorumClock>> for Cops<E> {
    fn send(&mut self, update: cops::events::Update<QuorumClock>) -> anyhow::Result<()> {
        let announce = Announce {
            prev: update.prev,
            merged: update.deps,
            id: update.id,
        };
        self.0.send(announce)
    }
}

impl<E: SendEvent<cops::events::UpdateOk<QuorumClock>>> SendEvent<(u64, QuorumClock)> for Cops<E> {
    fn send(&mut self, (id, clock): (u64, QuorumClock)) -> anyhow::Result<()> {
        let update_ok = cops::events::UpdateOk {
            id,
            version_deps: clock,
        };
        self.0.send(update_ok)
    }
}

// cSpell:words lamport upcall
