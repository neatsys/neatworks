// notes on this implementation of
// Don’t Settle for Eventual: Scalable Causal Consistency for Wide-Area Storage
// with COPS (SOSP'11)
// the implementation contains the COPS variant in the paper, COPS-GT and
// COPS-CD are not included
// the implementation is prepared for a security model that allows certain
// arbitrary faulty behaviors, mostly related to the partial ordering properties
// of the causal consistency (i.e. "causal+ consistency" in the original work).
// the examples of such behaviors are:
// * not passing down the complete causality information (i.e. `nearest` in the
//   original work) to remote replicas, inducing them to violate the "execution
//   thread" rule
// * ignoring the `nearest` passed by the remote replicas, directly violate the
//   "execution thread rule"
// * replying client with malformed version numbers, violate the "gets from"
//   rule
// * passing malformed `nearest` to remote replicas that violates partial
//   ordering properties e.g. the ver1 of key1 depends on the ver2 of key2, and
//   the ver2 of key2 also depends on the ver1 of key1, leading remote replicas
//   into deadlock
// in response to these faulty behaviors, the extensively used version number in
// the original work has been elaborated with necessary causality information,
// maps into the `V` type in this implementation. the generic version type
// allows the implementation to work with different versioning backends, each
// with unique performance and security (regarding causality properties)
// tradeoff, with a universal backbone protocol. the implementation requires
// the version type to not only keep track of the generation of the key (i.e.
// the original version number), but also the causal dependencies of the
// generation i.e. it happens after what other `V`s. for example, while a
// version number only says "ver2", a `V` value of key1 may say "ver2, which
// happens after and only after ver1 of key2 and ver3 of key42", and the `V`
// value of key42 may say "ver3, which happens after and only after ver1 of
// key1", etc
// (notice that we are implying a serial causality of versions of the same key.
// this is aligned with the linearizability in local clusters that is assumed
// by the original work, while the cross cluster conflict detection described
// in the original work is out of scope)
// in conclusion, `V` is assumed to be a general mechanism that can keep track
// of causality among version numbers (and allow reasoning about it), with
// additional feature that if ignoring the causality information it carries, it
// can still be viewed as a plain version number as in the original work. so we
// call it `version_deps` in this implementation
// because of this additional feature, the version type can be a drop in
// replacement of the original version number throughout the implementation,
// with all the "version bumping" replaced with the "causality merging"
// operation of the underlying `V` type (which corresponds to a
// `Update`/`UpdateOk` event round in this codebase). additionally, the
// `nearest` passed along in the asynchronous replication messages (i.e.
// `SyncKey` in this implementation), whose sole purpose is to inform the
// causality of the replicated key, can be omitted in the presence of `V`, as
// long as we are working with a somehow whitebox `V` type allow us to inspect
// the individual dependencies of keys in the `V` values (which corresponds to
// `impl AsRef<OrdinaryVersion>` in this codebase)
// (theoretically clients can do the same thing when issuing put requests, but
// we assume that in our system setup only replicas can produce `V` values so
// clients just echo back the `V` values previously replied by replicas.
// nevertheless the `nearest` compaction technique can still be performed on
// client side leveraging the partial ordering of `V`)
// that said, migrating from plain version number to `V` may fundamentally
// change performance features of the protocol still. after all the system keeps
// track of more information; in the original work each storage node maintains a
// key value store that is *assumed* to be causal consistent by the correctness
// of the protocol and the belief of all storage nodes are correct, while in
// this implementation at any time the state of a storage node always naturally
// forms a proof of the causal consistency based on the verifiability of the
// underlying `V`
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    mem::take,
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    app::ycsb,
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent, TimerId,
    },
    net::{deserialize, events::Recv, Addr, All, MessageNet, SendMessage},
    workload::events::{Invoke, InvokeOk},
};

// "key" under COPS context, "id" under Boson's logical clock context
pub type KeyId = u64;

pub trait Version: AsRef<OrdinaryVersion> + Clone {}
impl<V: AsRef<OrdinaryVersion> + Clone> Version for V {}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Put<V, A> {
    key: KeyId,
    value: String,
    pub deps: BTreeMap<KeyId, V>,
    client_addr: A,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct PutOk<V> {
    pub version_deps: V,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Get<A> {
    key: KeyId,
    client_addr: A,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct GetOk<V> {
    value: String,
    pub version_deps: V,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct SyncKey<V> {
    key: KeyId,
    value: String,
    pub version_deps: V,
}

pub trait ClientNet<A, V>: SendMessage<A, Put<V, A>> + SendMessage<A, Get<A>> {}
impl<T: SendMessage<A, Put<V, A>> + SendMessage<A, Get<A>>, A, V> ClientNet<A, V> for T {}

pub trait ToClientNet<A, V>: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>> {}
impl<T: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>>, A, V> ToClientNet<A, V> for T {}

pub trait ReplicaNet<A, V>: SendMessage<All, SyncKey<V>> {}
impl<T: SendMessage<All, SyncKey<V>>, A, V> ReplicaNet<A, V> for T {}

// events with version service
// version service expects at most one outstanding `Update<_>` per `id`
pub mod events {
    use super::KeyId;

    pub struct Update<V> {
        pub id: KeyId,
        pub prev: V, // `version_deps`
        pub deps: Vec<V>,
    }

    pub struct UpdateOk<V> {
        pub id: KeyId,
        pub version_deps: V,
    }
}
// client events are Invoke<ycsb::Op> and InvokeOk<ycsb::Result>

pub trait Upcall: SendEvent<InvokeOk<ycsb::Result>> {}
impl<T: SendEvent<InvokeOk<ycsb::Result>>> Upcall for T {}

pub struct Client<N, U, V, A> {
    addr: A,
    replica_addr: A, // local replica address, the one client always contacts
    deps: BTreeMap<KeyId, V>,
    working_key: Option<(KeyId, TimerId)>,

    net: N,
    upcall: U,
}

impl<N, U, V, A> Client<N, U, V, A> {
    pub fn new(addr: A, replica_addr: A, net: N, upcall: U) -> Self {
        Self {
            addr,
            replica_addr,
            net,
            upcall,
            deps: Default::default(),
            working_key: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InvokeTimeout;

impl InvokeTimeout {
    const AFTER: Duration = Duration::from_millis(2000);
}

impl<N: ClientNet<A, V>, U, V: Version, A: Addr> OnEvent<Invoke<ycsb::Op>> for Client<N, U, V, A> {
    fn on_event(
        &mut self,
        Invoke(op): Invoke<ycsb::Op>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // modelling client sessions with Poisson process that each of the events is whether the
        // previous session ends now. this would give us exponential distributed session duration
        // TODO adjustable end probability
        use rand::Rng as _;
        if rand::thread_rng().gen_bool(0.2) {
            self.deps.clear()
        }

        let key = match &op {
            ycsb::Op::Read(key) | ycsb::Op::Update(key, ..) => from_ycsb(key)?,
            _ => anyhow::bail!("unimplemented"),
        };
        let replaced = self
            .working_key
            .replace((key, timer.set(InvokeTimeout::AFTER, InvokeTimeout)?));
        // Put/Put concurrent is forbid by original work as well
        // Put/Get concurrent is allowed there, but may incur some difficulties when checking the
        // validity of PutOk/GetOk (not sure)
        // Get/Get concurrent could be supported, maybe in future
        // the client will be driven by a close loop without concurrent invocation after all
        anyhow::ensure!(replaced.is_none(), "concurrent op");

        match op {
            ycsb::Op::Update(_, index, value) => {
                anyhow::ensure!(index == 0, "unimplemented");
                let put = Put {
                    key,
                    value,
                    deps: self.deps.clone(),
                    client_addr: self.addr.clone(),
                };
                self.net.send(self.replica_addr.clone(), put)
            }
            ycsb::Op::Read(_) => {
                let get = Get {
                    key,
                    client_addr: self.addr.clone(),
                };
                self.net.send(self.replica_addr.clone(), get)
            }
            _ => unreachable!(),
        }
    }
}

// easy way to adapt current YCSB key format (easier than adapt on YCSB side)
fn from_ycsb(key: &str) -> anyhow::Result<KeyId> {
    Ok(key
        .strip_prefix("user")
        .ok_or(anyhow::format_err!("unimplemented"))?
        .parse()?)
}

impl<N, U: SendEvent<InvokeOk<ycsb::Result>>, V: Version, A> OnEvent<Recv<PutOk<V>>>
    for Client<N, U, V, A>
{
    fn on_event(
        &mut self,
        Recv(put_ok): Recv<PutOk<V>>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some((key, timer_id)) = self.working_key.take() else {
            anyhow::bail!("missing working key")
        };
        // if !self.deps.values().all(|dep| {
        //     matches!(
        //         put_ok.version_deps.partial_cmp(dep),
        //         Some(Ordering::Greater)
        //     )
        // }) {
        //     warn!("malformed PutOk");
        //     return Ok(());
        // }
        self.deps = [(key, put_ok.version_deps)].into();
        timer.unset(timer_id)?;
        self.upcall.send((Default::default(), ycsb::Result::Ok)) // careful
    }
}

impl<N, U: SendEvent<InvokeOk<ycsb::Result>>, V: Version, A> OnEvent<Recv<GetOk<V>>>
    for Client<N, U, V, A>
{
    fn on_event(
        &mut self,
        Recv(get_ok): Recv<GetOk<V>>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some((key, timer_id)) = self.working_key.take() else {
            anyhow::bail!("missing working key")
        };
        // if !self
        //     .deps
        //     .values()
        //     .all(|dep| get_ok.version_deps.dep_cmp(dep, key).is_ge())
        // {
        //     warn!("malformed GetOk");
        //     return Ok(());
        // }
        self.deps.insert(key, get_ok.version_deps);
        timer.unset(timer_id)?;
        self.upcall
            .send((Default::default(), ycsb::Result::ReadOk(vec![get_ok.value])))
    }
}

impl<N, U, V, A> OnEvent<InvokeTimeout> for Client<N, U, V, A> {
    fn on_event(
        &mut self,
        InvokeTimeout: InvokeTimeout,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("client timeout while working on {:?}", self.working_key)
        // warn!("client timeout while working on {:?}", self.working_key);
        // Ok(())
    }
}

pub struct Replica<N, CN, VS, V, A, _M = (N, CN, VS, V, A)> {
    store: HashMap<KeyId, KeyState<V, A>>,
    version_zero: V,
    pending_sync_keys: Vec<SyncKey<V>>,
    net: N,
    client_net: CN,
    version_service: VS,
    _m: std::marker::PhantomData<_M>,
}

#[derive(Clone)]
struct KeyState<V, A> {
    value: String,
    version_deps: V,
    pending_puts: VecDeque<Put<V, A>>,
}

impl<N, CN, VS, V: Clone, A: Clone> Replica<N, CN, VS, V, A> {
    pub fn new(version_zero: V, net: N, client_net: CN, version_service: VS) -> Self {
        Self {
            net,
            client_net,
            version_service,
            version_zero,
            store: Default::default(),
            pending_sync_keys: Default::default(),
            _m: Default::default(),
        }
    }

    pub fn startup_insert(&mut self, op: ycsb::Op) -> anyhow::Result<()> {
        let ycsb::Op::Insert(key, mut value) = op else {
            anyhow::bail!("unimplemented")
        };
        let key = from_ycsb(&key)?;
        anyhow::ensure!(value.len() == 1, "unimplemented");
        let value = value.remove(0);
        let state = KeyState {
            value,
            version_deps: self.version_zero.clone(),
            pending_puts: Default::default(),
        };
        let replaced = self.store.insert(key, state);
        anyhow::ensure!(replaced.is_none(), "duplicated startup insertion");
        Ok(())
    }
}

pub trait ReplicaCommon {
    type N: ReplicaNet<Self::A, Self::V>;
    type CN: ToClientNet<Self::A, Self::V>;
    type VS: SendEvent<events::Update<Self::V>>;
    type V: Version;
    type A: Addr;
}
impl<N, CN, VS, V, A> ReplicaCommon for (N, CN, VS, V, A)
where
    N: ReplicaNet<A, V>,
    CN: ToClientNet<A, V>,
    VS: SendEvent<events::Update<V>>,
    V: Version,
    A: Addr,
{
    type N = N;
    type CN = CN;
    type VS = VS;
    type V = V;
    type A = A;
}

impl<M: ReplicaCommon> OnEvent<Recv<Get<M::A>>> for Replica<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn on_event(
        &mut self,
        Recv(get): Recv<Get<M::A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.store.get(&get.key) else {
            anyhow::bail!("missing state for key {}", get.key)
        };
        let get_ok = GetOk {
            value: state.value.clone(),
            version_deps: state.version_deps.clone(),
        };
        self.client_net.send(get.client_addr, get_ok)
    }
}

impl<M: ReplicaCommon> OnEvent<Recv<Put<M::V, M::A>>>
    for Replica<M::N, M::CN, M::VS, M::V, M::A, M>
{
    fn on_event(
        &mut self,
        Recv(put): Recv<Put<M::V, M::A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        for (key, dep) in &put.deps {
            if !self
                .store
                .get(key)
                .map(|state| &state.version_deps)
                .unwrap_or(&self.version_zero)
                .as_ref()
                .dep_cmp(dep.as_ref(), *key)
                .is_ge()
            {
                warn!("malformed Put");
                return Ok(());
            }
        }
        let state = self.store.entry(put.key).or_insert_with(|| KeyState {
            version_deps: self.version_zero.clone(),
            value: Default::default(),
            pending_puts: Default::default(),
        });
        state.pending_puts.push_back(put.clone());
        if state.pending_puts.len() == 1 {
            let update = events::Update {
                id: put.key,
                prev: state.version_deps.clone(),
                deps: put.deps.into_values().collect(),
            };
            self.version_service.send(update)?
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> OnEvent<events::UpdateOk<M::V>>
    for Replica<M::N, M::CN, M::VS, M::V, M::A, M>
{
    fn on_event(
        &mut self,
        update_ok: events::UpdateOk<M::V>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.store.get_mut(&update_ok.id) else {
            anyhow::bail!("missing put key state")
        };
        let Some(put) = state.pending_puts.pop_front() else {
            anyhow::bail!("missing pending puts")
        };
        anyhow::ensure!(put.deps.values().all(|dep| matches!(
            update_ok.version_deps.as_ref().partial_cmp(dep.as_ref()),
            Some(Ordering::Greater)
        )));
        anyhow::ensure!(matches!(
            update_ok
                .version_deps
                .as_ref()
                .partial_cmp(state.version_deps.as_ref()),
            Some(Ordering::Greater)
        ));
        state.value = put.value.clone();
        state.version_deps = update_ok.version_deps.clone();
        let put_ok = PutOk {
            version_deps: update_ok.version_deps.clone(),
        };
        self.client_net.send(put.client_addr, put_ok)?;
        let sync_key = SyncKey {
            key: put.key,
            value: put.value,
            version_deps: update_ok.version_deps.clone(),
        };
        self.net.send(All, sync_key)?;
        if let Some(pending_put) = state.pending_puts.front() {
            let update = events::Update {
                id: update_ok.id,
                prev: update_ok.version_deps,
                deps: pending_put.deps.values().cloned().collect(),
            };
            self.version_service.send(update)?
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn can_sync(&self, sync_key: &SyncKey<M::V>) -> bool {
        for (key, remote_version) in &**sync_key.version_deps.as_ref() {
            // TODO more efficient dependency check so can remove this filter
            if *key != sync_key.key {
                continue;
            }
            let local_version = self
                .store
                .get(key)
                .and_then(|state| {
                    // the startup insertion
                    if state.version_deps.as_ref().is_genesis() {
                        None
                    } else {
                        Some(state.version_deps.as_ref()[key])
                    }
                })
                .unwrap_or_default();
            let version = if *key == sync_key.key {
                local_version + 1
            } else {
                local_version
            };
            if *remote_version > version {
                return false;
            }
        }
        true
    }

    fn apply_sync(&mut self, sync_key: SyncKey<M::V>) -> anyhow::Result<()>
    where
        M::V: std::fmt::Debug,
    {
        if let Some(state) = self.store.get_mut(&sync_key.key) {
            anyhow::ensure!(
                state.pending_puts.is_empty(),
                "conflicting Put across servers"
            );
            anyhow::ensure!(sync_key.version_deps.as_ref().contains_key(&sync_key.key));
            if !matches!(
                sync_key
                    .version_deps
                    .as_ref()
                    .partial_cmp(state.version_deps.as_ref()),
                Some(Ordering::Greater)
            ) {
                warn!(
                    "malformed SyncKey",
                    // "malformed SyncKey: {:?} -> {:?}",
                    // state.version_deps, sync_key.version_deps
                );
                return Ok(());
            }
            state.value = sync_key.value;
            state.version_deps = sync_key.version_deps
        } else {
            self.store.insert(
                sync_key.key,
                KeyState {
                    value: sync_key.value,
                    version_deps: sync_key.version_deps,
                    pending_puts: Default::default(),
                },
            );
        }
        debug!("synced key {}", sync_key.key);
        Ok(())
    }
}

impl<M: ReplicaCommon> OnEvent<Recv<SyncKey<M::V>>> for Replica<M::N, M::CN, M::VS, M::V, M::A, M>
where
    M::V: std::fmt::Debug,
{
    fn on_event(
        &mut self,
        Recv(sync_key): Recv<SyncKey<M::V>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if !self.can_sync(&sync_key) {
            self.pending_sync_keys.push(sync_key);
            debug!("pending SyncKey len {}", self.pending_sync_keys.len());
            return Ok(());
        }
        self.apply_sync(sync_key)?;
        for sync_key in take(&mut self.pending_sync_keys) {
            if !self.can_sync(&sync_key) {
                self.pending_sync_keys.push(sync_key);
                continue;
            }
            debug!("clear previous SyncKey");
            self.apply_sync(sync_key)?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum ToClientMessage<V> {
    PutOk(PutOk<V>),
    GetOk(GetOk<V>),
}

pub type ToClientMessageNet<N, V> = MessageNet<N, ToClientMessage<V>>;

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum ToReplicaMessage<V, A> {
    Put(Put<V, A>),
    Get(Get<A>),
    SyncKey(SyncKey<V>),
}

pub type ToReplicaMessageNet<N, V, A> = MessageNet<N, ToReplicaMessage<V, A>>;

pub trait SendClientRecvEvent<V>: SendEvent<Recv<PutOk<V>>> + SendEvent<Recv<GetOk<V>>> {}
impl<T: SendEvent<Recv<PutOk<V>>> + SendEvent<Recv<GetOk<V>>>, V> SendClientRecvEvent<V> for T {}

pub fn to_client_on_buf<V: DeserializeOwned>(
    buf: &[u8],
    sender: &mut impl SendClientRecvEvent<V>,
) -> anyhow::Result<()> {
    match deserialize(buf)? {
        ToClientMessage::PutOk(message) => sender.send(Recv(message)),
        ToClientMessage::GetOk(message) => sender.send(Recv(message)),
    }
}

pub trait SendReplicaRecvEvent<V, A>:
    SendEvent<Recv<Put<V, A>>> + SendEvent<Recv<Get<A>>> + SendEvent<Recv<SyncKey<V>>>
{
}
impl<
        T: SendEvent<Recv<Put<V, A>>> + SendEvent<Recv<Get<A>>> + SendEvent<Recv<SyncKey<V>>>,
        V,
        A,
    > SendReplicaRecvEvent<V, A> for T
{
}

pub fn to_replica_on_buf<V: DeserializeOwned, A: DeserializeOwned>(
    buf: &[u8],
    // applying difference security policy to messages received from clients and remote replicas
    sender: &mut impl SendReplicaRecvEvent<V, A>,
    sync_sender: &mut impl SendReplicaRecvEvent<V, A>,
) -> anyhow::Result<()> {
    match deserialize(buf)? {
        ToReplicaMessage::Put(message) => sender.send(Recv(message)),
        ToReplicaMessage::Get(message) => sender.send(Recv(message)),
        ToReplicaMessage::SyncKey(message) => sync_sender.send(Recv(message)),
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Default, derive_more::Deref, Serialize, Deserialize,
)]
pub struct OrdinaryVersion(pub BTreeMap<KeyId, u32>);

impl AsRef<OrdinaryVersion> for OrdinaryVersion {
    fn as_ref(&self) -> &OrdinaryVersion {
        self
    }
}

impl OrdinaryVersion {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_genesis(&self) -> bool {
        self.0.values().all(|n| *n == 0)
    }

    fn merge(&self, other: &Self) -> Self {
        let merged = self
            .0
            .keys()
            .chain(other.0.keys())
            .map(|id| {
                let n = match (self.0.get(id), other.0.get(id)) {
                    (Some(n), Some(other_n)) => (*n).max(*other_n),
                    (Some(n), None) | (None, Some(n)) => *n,
                    (None, None) => unreachable!(),
                };
                (*id, n)
            })
            .collect();
        Self(merged)
    }

    pub fn update<'a>(&'a self, others: impl Iterator<Item = &'a Self>, id: u64) -> Self {
        let mut updated = others.fold(self.clone(), |version, dep| version.merge(dep));
        *updated.0.entry(id).or_default() += 1;
        updated
    }
}

impl PartialOrd for OrdinaryVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // this is way more elegant, but probably also significant slower :(
        // let merged = self.merge(other);
        // match (merged == *self, merged == *other) {
        //     (true, true) => Some(Ordering::Equal),
        //     (true, false) => Some(Ordering::Greater),
        //     (false, true) => Some(Ordering::Less),
        //     (false, false) => None,
        // }
        fn ge(clock: &OrdinaryVersion, other_clock: &OrdinaryVersion) -> bool {
            for (other_id, other_n) in &other_clock.0 {
                if *other_n == 0 {
                    continue;
                }
                let Some(n) = clock.0.get(other_id) else {
                    return false;
                };
                if n < other_n {
                    return false;
                }
            }
            true
        }
        match (ge(self, other), ge(other, self)) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => None,
        }
    }
}

impl OrdinaryVersion {
    pub fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering {
        match (self.0.get(&id), other.0.get(&id)) {
            // handy sanity check
            // (Some(0), _) | (_, Some(0)) => {
            //     unimplemented!("invalid dependency compare: {self:?} vs {other:?} @ {id}")
            // }
            // disabling this check after the definition of genesis clock has been extended
            // haven't revealed any bug with this assertion before, hopefully disabling it will not
            // hide any bug in the future as well
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            // this can happen on the startup insertion
            (None, None) => Ordering::Equal,
            (Some(n), Some(m)) => n.cmp(m),
        }
    }
}

impl crate::lamport_mutex::Clock for OrdinaryVersion {
    fn reduce(&self) -> crate::lamport_mutex::LamportClock {
        self.0.values().copied().sum()
    }
}

#[derive(Debug)]
pub struct OrdinaryVersionService<E>(pub E);

impl<E: SendEvent<events::UpdateOk<OrdinaryVersion>>> SendEvent<events::Update<OrdinaryVersion>>
    for OrdinaryVersionService<E>
{
    fn send(&mut self, update: events::Update<OrdinaryVersion>) -> anyhow::Result<()> {
        let update_ok = events::UpdateOk {
            id: update.id,
            version_deps: update.prev.update(update.deps.iter(), update.id),
        };
        self.0.send(update_ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_genesis() -> anyhow::Result<()> {
        anyhow::ensure!(OrdinaryVersion::default().is_genesis());
        Ok(())
    }
}

// cSpell:words deque upcall ycsb sosp lamport
