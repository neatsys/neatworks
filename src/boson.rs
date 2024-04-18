use std::collections::HashMap;

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    cops::{self, DefaultVersion, DepOrd},
    crypto::{Crypto, Verifiable},
    event::{erased::OnEvent, OnTimer, SendEvent},
    lamport_mutex,
    net::{events::Recv, Addr, All, SendMessage},
    worker::Submit,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Announce<A> {
    prev: QuorumClock,
    merged: Vec<QuorumClock>,
    id: u64,
    addr: A,
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

pub struct QuorumClient<U, N, A> {
    addr: A,
    num_faulty: usize,
    working_announces: HashMap<u64, WorkingAnnounce>,
    upcall: U,
    net: N,
}

struct WorkingAnnounce {
    plain: DefaultVersion,
    replies: HashMap<usize, Verifiable<AnnounceOk>>,
}

struct SubmitAnnounce(QuorumClock, Vec<QuorumClock>, u64);

impl<U, N: SendMessage<All, Announce<A>>, A: Clone> OnEvent<SubmitAnnounce>
    for QuorumClient<U, N, A>
{
    fn on_event(
        &mut self,
        SubmitAnnounce(prev, merged, id): SubmitAnnounce,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let replaced = self.working_announces.insert(
            id,
            WorkingAnnounce {
                plain: prev.plain.clone(),
                replies: Default::default(),
            },
        );
        anyhow::ensure!(replaced.is_none(), "concurrent announce on id {id}");
        let announce = Announce {
            prev,
            merged,
            id,
            addr: self.addr.clone(),
        };
        self.net.send(All, announce)
    }
}

// feel lazy to define event type for replying
impl<U: SendEvent<(u64, QuorumClock)>, N, A> OnEvent<Recv<Verifiable<AnnounceOk>>>
    for QuorumClient<U, N, A>
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

impl<E: SendEvent<SubmitAnnounce>> SendEvent<lamport_mutex::events::Update<QuorumClock>>
    for Lamport<E>
{
    fn send(&mut self, update: lamport_mutex::Update<QuorumClock>) -> anyhow::Result<()> {
        self.0
            .send(SubmitAnnounce(update.prev, vec![update.remote], self.1))
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

impl<E: SendEvent<SubmitAnnounce>> SendEvent<cops::events::Update<QuorumClock>> for Cops<E> {
    fn send(&mut self, update: cops::events::Update<QuorumClock>) -> anyhow::Result<()> {
        self.0
            .send(SubmitAnnounce(update.prev, update.deps, update.id))
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

pub struct QuorumServer<CW, N> {
    id: usize,
    crypto_worker: CW,
    _m: std::marker::PhantomData<N>,
}

impl<CW, N> QuorumServer<CW, N> {
    pub fn new(id: usize, crypto_worker: CW) -> Self {
        Self {
            id,
            crypto_worker,
            _m: Default::default(),
        }
    }
}

impl<CW: Submit<Crypto, N>, N: SendMessage<A, Verifiable<AnnounceOk>>, A: Addr>
    OnEvent<Recv<Announce<A>>> for QuorumServer<CW, N>
{
    fn on_event(
        &mut self,
        Recv(announce): Recv<Announce<A>>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let plain = announce.prev.plain.update(
            announce.merged.iter().map(|clock| &clock.plain),
            announce.id,
        );
        let announce_ok = AnnounceOk {
            plain,
            id: announce.id,
            signer_id: self.id,
        };
        self.crypto_worker.submit(Box::new(move |crypto, net| {
            net.send(announce.addr, crypto.sign(announce_ok))
        }))
    }
}

impl<CW, N> OnTimer for QuorumServer<CW, N> {
    fn on_timer(
        &mut self,
        _: crate::event::TimerId,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}

#[derive_where(Debug, Clone; CW)]
pub struct VerifyQuorumClock<CW, E> {
    num_faulty: usize,
    crypto_worker: CW,
    _m: std::marker::PhantomData<E>,
}

impl<CW, E> VerifyQuorumClock<CW, E> {
    pub fn new(num_faulty: usize, crypto_worker: CW) -> Self {
        Self {
            num_faulty,
            crypto_worker,
            _m: Default::default(),
        }
    }
}

impl<CW: Submit<Crypto, E>, E: SendEvent<Recv<Announce<A>>>, A: Addr> SendEvent<Recv<Announce<A>>>
    for VerifyQuorumClock<CW, E>
{
    fn send(&mut self, Recv(announce): Recv<Announce<A>>) -> anyhow::Result<()> {
        let num_faulty = self.num_faulty;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if announce.prev.verify(num_faulty, crypto).is_ok()
                && announce
                    .merged
                    .iter()
                    .all(|clock| clock.verify(num_faulty, crypto).is_ok())
            {
                sender.send(Recv(announce))
            } else {
                Ok(())
            }
        }))
    }
}

impl<
        CW: Submit<Crypto, E>,
        E: SendEvent<Recv<lamport_mutex::Clocked<M, QuorumClock>>>,
        M: Send + Sync + 'static,
    > SendEvent<Recv<lamport_mutex::Clocked<M, QuorumClock>>> for VerifyQuorumClock<CW, E>
{
    fn send(
        &mut self,
        Recv(clocked): Recv<lamport_mutex::Clocked<M, QuorumClock>>,
    ) -> anyhow::Result<()> {
        let num_faulty = self.num_faulty;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if clocked.clock.verify(num_faulty, crypto).is_ok() {
                sender.send(Recv(clocked))
            } else {
                Ok(())
            }
        }))
    }
}

impl<CW: Submit<Crypto, E>, E: SendEvent<Recv<cops::ToClientMessage<QuorumClock>>>>
    SendEvent<Recv<cops::ToClientMessage<QuorumClock>>> for VerifyQuorumClock<CW, E>
{
    fn send(
        &mut self,
        Recv(message): Recv<cops::ToClientMessage<QuorumClock>>,
    ) -> anyhow::Result<()> {
        let num_faulty = self.num_faulty;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if match &message {
                cops::ToClientMessage::PutOk(message) => &message.version_deps,
                cops::ToClientMessage::GetOk(message) => &message.version_deps,
            }
            .verify(num_faulty, crypto)
            .is_ok()
            {
                sender.send(Recv(message))
            } else {
                Ok(())
            }
        }))
    }
}

impl<
        CW: Submit<Crypto, E>,
        E: SendEvent<Recv<cops::ToReplicaMessage<QuorumClock, A>>>,
        A: Addr,
    > SendEvent<Recv<cops::ToReplicaMessage<QuorumClock, A>>> for VerifyQuorumClock<CW, E>
{
    fn send(
        &mut self,
        Recv(message): Recv<cops::ToReplicaMessage<QuorumClock, A>>,
    ) -> anyhow::Result<()> {
        let num_faulty = self.num_faulty;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if match &message {
                cops::ToReplicaMessage::Get(_) => true,
                cops::ToReplicaMessage::Put(message) => message
                    .deps
                    .values()
                    .all(|clock| clock.verify(num_faulty, crypto).is_ok()),
                cops::ToReplicaMessage::SyncKey(message) => {
                    message.version_deps.verify(num_faulty, crypto).is_ok()
                }
            } {
                sender.send(Recv(message))
            } else {
                Ok(())
            }
        }))
    }
}

// cSpell:words lamport upcall
