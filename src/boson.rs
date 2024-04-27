use std::collections::HashMap;

use derive_where::derive_where;
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinSet};
use tracing::{debug, warn};

use crate::{
    cops::{self, DefaultVersion, DepOrd},
    crypto::peer::{
        events::{Signed, Verified},
        Crypto, PublicKey, Verifiable,
    },
    event::{erased::OnEvent, OnTimer, SendEvent},
    lamport_mutex,
    net::{events::Recv, Addr, All, Payload, SendMessage},
    worker::Submit,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Update<C>(pub C, pub Vec<C>, pub u64);

// feel lazy to define event type for replying
pub type UpdateOk<C> = (u64, C);

pub struct Lamport<E>(pub E, pub u64);

impl<E: SendEvent<Update<C>>, C> SendEvent<lamport_mutex::events::Update<C>> for Lamport<E> {
    fn send(&mut self, update: lamport_mutex::Update<C>) -> anyhow::Result<()> {
        self.0
            .send(Update(update.prev, vec![update.remote], self.1))
    }
}

impl<E: SendEvent<lamport_mutex::events::UpdateOk<C>>, C> SendEvent<UpdateOk<C>> for Lamport<E> {
    fn send(&mut self, (id, clock): (u64, C)) -> anyhow::Result<()> {
        anyhow::ensure!(id == self.1);
        self.0.send(lamport_mutex::events::UpdateOk(clock))
    }
}

pub struct Cops<E>(pub E);

impl<E: SendEvent<Update<C>>, C> SendEvent<cops::events::Update<C>> for Cops<E> {
    fn send(&mut self, update: cops::events::Update<C>) -> anyhow::Result<()> {
        self.0.send(Update(update.prev, update.deps, update.id))
    }
}

impl<E: SendEvent<cops::events::UpdateOk<C>>, C> SendEvent<UpdateOk<C>> for Cops<E> {
    fn send(&mut self, (id, clock): (u64, C)) -> anyhow::Result<()> {
        let update_ok = cops::events::UpdateOk {
            id,
            version_deps: clock,
        };
        self.0.send(update_ok)
    }
}

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

impl QuorumClock {
    pub fn verify(&self, num_faulty: usize, crypto: &Crypto) -> anyhow::Result<()> {
        if self.plain.is_genesis() {
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

impl<CW: Submit<Crypto, dyn quorum_client::SendCryptoEvent<A>>, U, N, A: Addr>
    OnEvent<Update<QuorumClock>> for QuorumClient<CW, U, N, A>
{
    fn on_event(
        &mut self,
        Update(prev, merged, id): Update<QuorumClock>,
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
        U: SendEvent<UpdateOk<QuorumClock>>,
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

#[derive(Debug, Serialize, Deserialize)]
#[derive_where(PartialOrd, PartialEq)]
pub struct NitroEnclavesClock {
    pub plain: DefaultVersion,
    #[derive_where(skip)]
    pub document: Payload,
}

impl TryFrom<DefaultVersion> for NitroEnclavesClock {
    type Error = anyhow::Error;

    fn try_from(value: DefaultVersion) -> Result<Self, Self::Error> {
        anyhow::ensure!(value.is_genesis());
        Ok(Self {
            plain: value,
            document: Default::default(),
        })
    }
}

impl DepOrd for NitroEnclavesClock {
    fn dep_cmp(&self, other: &Self, id: crate::cops::KeyId) -> std::cmp::Ordering {
        self.plain.dep_cmp(&other.plain, id)
    }

    fn deps(&self) -> impl Iterator<Item = crate::cops::KeyId> + '_ {
        self.plain.deps()
    }
}

#[cfg(feature = "nitro-enclaves")]
impl NitroEnclavesClock {
    pub fn verify(
        &self,
    ) -> anyhow::Result<Option<aws_nitro_enclaves_nsm_api::api::AttestationDoc>> {
        if self.plain.is_genesis() {
            return Ok(None);
        }
        use aws_nitro_enclaves_attestation::{AttestationProcess as _, AWS_ROOT_CERT};
        use aws_nitro_enclaves_nsm_api::api::AttestationDoc;
        let document = AttestationDoc::from_bytes(
            &self.document,
            AWS_ROOT_CERT,
            std::time::SystemTime::UNIX_EPOCH
                .elapsed()
                .unwrap()
                .as_secs(),
        )?;
        use crate::crypto::DigestHash as _;
        anyhow::ensure!(
            document.user_data.as_ref().map(|user_data| &***user_data)
                == Some(&self.plain.sha256().to_fixed_bytes()[..])
        );
        Ok(Some(document))
    }
}

#[derive(Debug)]
pub struct NitroSecureModule(pub i32);

#[cfg(feature = "nitro-enclaves")]
impl NitroSecureModule {
    fn new() -> anyhow::Result<Self> {
        let fd = aws_nitro_enclaves_nsm_api::driver::nsm_init();
        anyhow::ensure!(fd >= 0);
        Ok(Self(fd))
    }

    fn process_attestation(&self, user_data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        use aws_nitro_enclaves_nsm_api::api::Request::Attestation;
        // some silly code to avoid explicitly mention `serde_bytes::ByteBuf`
        let mut request = Attestation {
            user_data: Some(Default::default()),
            nonce: None,
            public_key: None,
        };
        let Attestation {
            user_data: Some(buf),
            ..
        } = &mut request
        else {
            unreachable!()
        };
        buf.extend(user_data);
        match aws_nitro_enclaves_nsm_api::driver::nsm_process_request(self.0, request) {
            aws_nitro_enclaves_nsm_api::api::Response::Attestation { document } => Ok(document),
            aws_nitro_enclaves_nsm_api::api::Response::Error(err) => anyhow::bail!("{err:?}"),
            _ => anyhow::bail!("unimplemented"),
        }
    }

    pub async fn run() -> anyhow::Result<()> {
        use std::os::fd::AsRawFd;

        use crate::crypto::DigestHash as _;
        use bincode::Options;
        use nix::sys::socket::{
            bind, listen, socket, AddressFamily, Backlog, SockFlag, SockType, VsockAddr,
        };
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let nsm = std::sync::Arc::new(Self::new()?);
        let socket_fd = socket(
            AddressFamily::Vsock,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )?;
        bind(socket_fd.as_raw_fd(), &VsockAddr::new(0xFFFFFFFF, 5005))?;
        // theoratically this is the earliest point to entering Tokio world, but i don't want to go
        // unsafe with `FromRawFd`, and Tokio don't have a `From<OwnedFd>` yet
        listen(&socket_fd, Backlog::new(64)?)?;
        let socket = std::os::unix::net::UnixListener::from(socket_fd);
        socket.set_nonblocking(true)?;
        let socket = tokio::net::UnixListener::from_std(socket)?;
        // let mut buf = Vec::new();
        let mut sessions = JoinSet::new();
        loop {
            enum Select {
                Accept((tokio::net::UnixStream, tokio::net::unix::SocketAddr)),
                JoinNext(Result<anyhow::Result<()>, tokio::task::JoinError>),
            }
            match tokio::select! {
                accept = socket.accept() => Select::Accept(accept?),
                Some(result) = sessions.join_next() => Select::JoinNext(result),
            } {
                Select::Accept((mut stream, _)) => {
                    let nsm = nsm.clone();
                    sessions.spawn(async move {
                        let len = stream.read_u64_le().await?;
                        let mut buf = vec![0; len as _];
                        stream.read_exact(&mut buf).await?;
                        let Update(prev, merged, id) =
                            bincode::options().deserialize::<Update<NitroEnclavesClock>>(&buf)?;
                        // TODO verify
                        let plain = prev
                            .plain
                            .update(merged.iter().map(|clock| &clock.plain), id);
                        // relies on the fact that different clocks always hash into different
                        // digests, hopefully true
                        let user_data = plain.sha256().to_fixed_bytes().to_vec();
                        let document = nsm.process_attestation(user_data)?;
                        let updated = NitroEnclavesClock {
                            plain,
                            document: Payload(document),
                        };
                        let buf = bincode::options().serialize(&(id, updated))?;
                        stream.write_u64_le(buf.len() as _).await?;
                        stream.write_all(&buf).await?;
                        Ok(())
                    });
                }
                Select::JoinNext(result) => {
                    if let Err(err) = result.map_err(Into::into).and_then(std::convert::identity) {
                        warn!("{err}")
                    }
                }
            }
        }
    }
}

#[cfg(feature = "nitro-enclaves")]
impl Drop for NitroSecureModule {
    fn drop(&mut self) {
        aws_nitro_enclaves_nsm_api::driver::nsm_exit(self.0)
    }
}

pub async fn nitro_enclaves_portal_session(
    cid: u32,
    mut events: UnboundedReceiver<Update<NitroEnclavesClock>>,
    sender: impl SendEvent<UpdateOk<NitroEnclavesClock>> + Clone + Send + 'static,
) -> anyhow::Result<()> {
    use std::os::fd::AsRawFd;

    use bincode::Options;
    use nix::sys::socket::{connect, socket, AddressFamily, SockFlag, SockType, VsockAddr};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut sessions = JoinSet::new();
    loop {
        enum Select {
            Recv(Option<Update<NitroEnclavesClock>>),
            JoinNext(anyhow::Result<()>),
        }
        match tokio::select! {
            recv = events.recv() => Select::Recv(recv),
            Some(result) = sessions.join_next() => Select::JoinNext(result?),
        } {
            Select::Recv(update) => {
                let Some(update) = update else {
                    anyhow::bail!("unreachable")
                };
                let mut sender = sender.clone();
                sessions.spawn(async move {
                    let fd = socket(
                        AddressFamily::Vsock,
                        SockType::Stream,
                        SockFlag::empty(),
                        None,
                    )?;
                    // this one is blocking, but should be instant, hopefully
                    connect(fd.as_raw_fd(), &VsockAddr::new(cid, 5005))?;
                    let socket = std::os::unix::net::UnixStream::from(fd);
                    socket.set_nonblocking(true)?;
                    let mut socket = tokio::net::UnixStream::from_std(socket)?;
                    let buf = bincode::options().serialize(&update)?;
                    socket.write_u64_le(buf.len() as _).await?;
                    socket.write_all(&buf).await?;
                    let len = socket.read_u64_le().await?;
                    let mut buf = vec![0; len as _];
                    socket.read_exact(&mut buf).await?;
                    sender.send(bincode::options().deserialize(&buf)?)
                });
            }
            Select::JoinNext(result) => result?,
        }
    }
}

// cSpell:words lamport upcall bincode vsock
// cSpell:ignore EINTR
