use std::collections::HashMap;

use derive_where::derive_where;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    cops::{self, DefaultVersion, DepOrd},
    crypto::peer::{
        events::{Signed, Verified},
        Crypto, PublicKey, Verifiable,
    },
    event::{erased::OnEvent, OnTimer, SendEvent},
    lamport_mutex,
    net::{events::Recv, Addr, All, SendMessage},
    worker::Submit,
};

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct Announce<A> {
    prev: QuorumClock,
    merged: Vec<QuorumClock>,
    id: u64,
    addr: A,
    key: PublicKey,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct AnnounceOk {
    plain: DefaultVersion,
    id: u64,
    key: PublicKey,
}

#[derive(Debug, Clone, Hash, Default, Serialize, Deserialize)]
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

impl lamport_mutex::Clock for QuorumClock {
    fn arbitrary_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.plain.sum().cmp(&other.plain.sum())
    }
}

impl QuorumClock {
    pub fn verify(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        if self.plain == DefaultVersion::default() {
            anyhow::ensure!(self.cert.is_empty()); // not necessary, just as sanity check
            return Ok(());
        }
        anyhow::ensure!(self.cert.len() > num_faulty);
        let keys = self
            .cert
            .iter()
            .map(|verifiable| verifiable.key)
            .collect::<Vec<_>>();
        crypto.verify_batch(&keys, &self.cert)
    }
}

pub mod quorum_client {
    use crate::{
        crypto::peer::{
            events::{Signed, Verified},
            Crypto,
        },
        event::SendEvent,
        net::Addr,
        worker::Submit,
    };

    use super::{Announce, AnnounceOk};

    pub struct CryptoWorker<W, E>(W, std::marker::PhantomData<E>);

    impl<W, E> From<W> for CryptoWorker<W, E> {
        fn from(value: W) -> Self {
            Self(value, Default::default())
        }
    }

    pub trait SendCryptoEvent<A>:
        SendEvent<Signed<Announce<A>>> + SendEvent<Verified<AnnounceOk>>
    {
    }
    impl<T: SendEvent<Signed<Announce<A>>> + SendEvent<Verified<AnnounceOk>>, A> SendCryptoEvent<A>
        for T
    {
    }

    impl<W: Submit<Crypto, E>, E: SendCryptoEvent<A> + 'static, A: Addr>
        Submit<Crypto, dyn SendCryptoEvent<A>> for CryptoWorker<W, E>
    {
        fn submit(
            &mut self,
            work: crate::worker::Work<Crypto, dyn SendCryptoEvent<A>>,
        ) -> anyhow::Result<()> {
            self.0
                .submit(Box::new(move |crypto, sender| work(crypto, sender)))
        }
    }
}

pub struct QuorumClient<CW, U, N, A> {
    addr: A,
    key: PublicKey,
    num_faulty: usize,
    working_announces: HashMap<u64, WorkingAnnounce>,
    crypto_worker: CW,
    upcall: U,
    net: N,
}

struct WorkingAnnounce {
    prev_plain: DefaultVersion,
    replies: HashMap<PublicKey, Verifiable<AnnounceOk>>,
    pending: Option<Vec<Verifiable<AnnounceOk>>>,
}

impl<CW, U, N, A> QuorumClient<CW, U, N, A> {
    pub fn new(
        addr: A,
        key: PublicKey,
        num_faulty: usize,
        crypto_worker: CW,
        upcall: U,
        net: N,
    ) -> Self {
        Self {
            addr,
            key,
            num_faulty,
            upcall,
            net,
            crypto_worker,
            working_announces: Default::default(),
        }
    }
}

struct SubmitAnnounce(QuorumClock, Vec<QuorumClock>, u64);

impl<CW: Submit<Crypto, dyn quorum_client::SendCryptoEvent<A>>, U, N, A: Addr>
    OnEvent<SubmitAnnounce> for QuorumClient<CW, U, N, A>
{
    fn on_event(
        &mut self,
        SubmitAnnounce(prev, merged, id): SubmitAnnounce,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let replaced = self.working_announces.insert(
            id,
            WorkingAnnounce {
                prev_plain: prev.plain.clone(),
                replies: Default::default(),
                pending: None,
            },
        );
        anyhow::ensure!(replaced.is_none(), "concurrent announce on id {id}");
        let announce = Announce {
            prev,
            merged,
            id,
            addr: self.addr.clone(),
            key: self.key,
        };
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            sender.send(Signed(crypto.sign(announce)))
        }))
    }
}

impl<CW, U, N: SendMessage<All, Verifiable<Announce<A>>>, A> OnEvent<Signed<Announce<A>>>
    for QuorumClient<CW, U, N, A>
{
    fn on_event(
        &mut self,
        Signed(announce): Signed<Announce<A>>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        self.net.send(All, announce)
    }
}

// feel lazy to define event type for replying
impl<
        CW: Submit<Crypto, dyn quorum_client::SendCryptoEvent<A>>,
        U: SendEvent<(u64, QuorumClock)>,
        N,
        A,
    > OnEvent<Recv<Verifiable<AnnounceOk>>> for QuorumClient<CW, U, N, A>
{
    fn on_event(
        &mut self,
        Recv(announce_ok): Recv<Verifiable<AnnounceOk>>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let Some(working_state) = self.working_announces.get_mut(&announce_ok.id) else {
            return Ok(());
        };
        // sufficient rule out?
        if announce_ok
            .plain
            .dep_cmp(&working_state.prev_plain, announce_ok.id)
            .is_le()
        {
            return Ok(());
        }
        if let Some(pending) = &mut working_state.pending {
            pending.push(announce_ok);
            return Ok(());
        }
        working_state.pending = Some(Default::default());
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if crypto.verify(&announce_ok.key, &announce_ok).is_ok() {
                sender.send(Verified(announce_ok))
            } else {
                Ok(())
            }
        }))
    }
}

impl<
        CW: Submit<Crypto, dyn quorum_client::SendCryptoEvent<A>>,
        U: SendEvent<(u64, QuorumClock)>,
        N,
        A,
    > OnEvent<Verified<AnnounceOk>> for QuorumClient<CW, U, N, A>
{
    fn on_event(
        &mut self,
        Verified(announce_ok): Verified<AnnounceOk>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let Some(working_state) = self.working_announces.get_mut(&announce_ok.id) else {
            anyhow::bail!("missing working state")
        };
        anyhow::ensure!(announce_ok
            .plain
            .dep_cmp(&working_state.prev_plain, announce_ok.id)
            .is_gt());
        working_state
            .replies
            .insert(announce_ok.key, announce_ok.clone());
        if working_state.replies.len() > self.num_faulty {
            let working_state = self.working_announces.remove(&announce_ok.id).unwrap();
            let announce_ok = announce_ok.into_inner();
            let clock = QuorumClock {
                plain: announce_ok.plain,
                cert: working_state.replies.into_values().collect(),
            };
            self.upcall.send((announce_ok.id, clock))?
        } else {
            let Some(pending) = &mut working_state.pending else {
                anyhow::bail!("missing pending queue")
            };
            if let Some(announce_ok) = pending.pop() {
                self.crypto_worker.submit(Box::new(move |crypto, sender| {
                    if crypto.verify(&announce_ok.key, &announce_ok).is_ok() {
                        sender.send(Verified(announce_ok))
                    } else {
                        Ok(())
                    }
                }))?
            } else {
                working_state.pending = None
            }
        }
        Ok(())
    }
}

impl<CW, U, N, A> OnTimer for QuorumClient<CW, U, N, A> {
    fn on_timer(
        &mut self,
        _: crate::event::TimerId,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        unreachable!()
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
    key: PublicKey,
    crypto_worker: CW,
    _m: std::marker::PhantomData<N>,
}

impl<CW, N> QuorumServer<CW, N> {
    pub fn new(key: PublicKey, crypto_worker: CW) -> Self {
        Self {
            key,
            crypto_worker,
            _m: Default::default(),
        }
    }
}

impl<CW: Submit<Crypto, N>, N: SendMessage<A, Verifiable<AnnounceOk>>, A: Addr>
    OnEvent<Recv<Verifiable<Announce<A>>>> for QuorumServer<CW, N>
{
    fn on_event(
        &mut self,
        Recv(announce): Recv<Verifiable<Announce<A>>>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        // TODO check announcer permission
        let plain = announce.prev.plain.update(
            announce.merged.iter().map(|clock| &clock.plain),
            announce.id,
        );
        let announce_ok = AnnounceOk {
            plain,
            id: announce.id,
            key: self.key,
        };
        debug!("signing {announce_ok:?}");
        self.crypto_worker.submit(Box::new(move |crypto, net| {
            net.send(announce.into_inner().addr, crypto.sign(announce_ok))
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

trait VerifyClock: Send + Sync + 'static {
    fn verify_clock(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()>;
}

impl<CW: Submit<Crypto, E>, E: SendEvent<Recv<M>>, M: VerifyClock> SendEvent<Recv<M>>
    for VerifyQuorumClock<CW, E>
{
    fn send(&mut self, Recv(message): Recv<M>) -> anyhow::Result<()> {
        let num_faulty = self.num_faulty;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if message.verify_clock(num_faulty, crypto).is_ok() {
                sender.send(Recv(message))
            } else {
                warn!("clock verification failed");
                Ok(())
            }
        }))
    }
}

impl<A: Addr> VerifyClock for Verifiable<Announce<A>> {
    fn verify_clock(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        crypto.verify(&self.key, self)?;
        self.prev.verify(num_faulty, crypto)?;
        for clock in &self.merged {
            clock.verify(num_faulty, crypto)?
        }
        Ok(())
    }
}

impl<M: Send + Sync + 'static> VerifyClock for lamport_mutex::Clocked<M, QuorumClock> {
    fn verify_clock(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        self.clock.verify(num_faulty, crypto)
    }
}

impl VerifyClock for cops::PutOk<QuorumClock> {
    fn verify_clock(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        self.version_deps.verify(num_faulty, crypto)
    }
}

impl VerifyClock for cops::GetOk<QuorumClock> {
    fn verify_clock(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        self.version_deps.verify(num_faulty, crypto)
    }
}

impl<A: Addr> VerifyClock for cops::Put<QuorumClock, A> {
    fn verify_clock(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        for clock in self.deps.values() {
            clock.verify(num_faulty, crypto)?
        }
        Ok(())
    }
}

impl<A: Addr> VerifyClock for cops::Get<A> {
    fn verify_clock(&self, _: usize, _: &Crypto) -> anyhow::Result<()> {
        Ok(())
    }
}

impl VerifyClock for cops::SyncKey<QuorumClock> {
    fn verify_clock(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        self.version_deps.verify(num_faulty, crypto)
    }
}

// cSpell:words lamport upcall
