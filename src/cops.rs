// notes on this implementation of
// Don’t Settle for Eventual: Scalable Causal Consistency for Wide-Area Storage
// with COPS (SOSP'11)
// the implementation contains the COPS variant in the paper, COPS-GT and
// COPS-CD are not included
// the implementation is prepared for an arbitrary fault security model.
// additional checks and proof passing take places
// the version type `V` in this implementation does not map to the version
// number in the original work. instead, it roughly maps to the "nearest"
// dependency set (when i say "roughly" i mean it's the superset of the nearest
// set). the `V` values returned by server contains the original version number
// as well: it's a combination of the assigned version number of a `Put` value
// and the nearest dependency set of that `Put`. that's why the `V` values sent
// by client and sent (and stored) by server are named `deps` and `version_deps`
// respectively
// the `version_deps` is something added to the original work. it enables client
// to learn about the nearest set of some version of a value in a verifiable way
// (assuming `V` is verifiable). thus client can check whether the version of
// values in the following replies consistent with this information, and reject
// malicious replies that violate causal consistency
// (at the same time, upon Put requests server also perform checks on submitted
// `deps`, ensure that it will not be fooled by malicious client and end up in
// an inconsistent state)
// besides of these additions, the only substantial difference to the original
// work is a deletion in the (geological) synchronization: the nearest set is
// omitted since all it's information is covered by the `V` value
// because of this deletion and also considering the additional storage and
// checking overhead is lightweight, i have the referenced implementation for
// trusted setup, i.e. the `DefaultVersion` below, also follows this design.
// so the protocol has unified logic under different setup (just all additional
// checks are expected to always pass under trusted setup), just plug in
// different `V` type for difference use case
// producing `V` typed value potentially takes long latency: it may require the
// computational expensive incrementally verifiable computation, or asynchronous
// network communication. so instead of inlined "version bumping" as in the
// original work, a version service is extracted to asynchronously produce `V`
// values. this incurs unnecessary overhead for the case that follows the
// original work's setup (i.e. the referenced `DefaultVersion` implementation),
// hopefully not critical (or even noticeable) since the work targets geological
// replication scenario
// the original work does not talk specifically about the causality policy of
// the same key across sessions. for the conflict-free scenario we assume an
// incremental causality of each key: the causal dependencies of the old version
// automatically carries to the new version. this policy is equivalent to the
// original work as long as it ensure to sequentially synchronize all versions
// of each key
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    mem::take,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::debug;

use crate::{
    app::ycsb,
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent,
    },
    net::{deserialize, events::Recv, Addr, All, MessageNet, SendMessage},
    workload::events::{Invoke, InvokeOk},
};

// "key" under COPS context, "id" under Boson's logical clock context
pub type KeyId = u64;

// `PartialOrd` for causality check: whether `self` happens after `other` in the
// sense of causality
// say Put(k2, _, {}) returns c2: {k2: 2}, then Put(k1, _, {k2: c2}) may return
// c1: {k1: 1, k2: 2} so that c1.partial_cmp(c2) == Some(Greater), but not just
// c1': {k1: 1}
pub trait Version: PartialOrd + DepOrd + Clone + Send + Sync + 'static {}
impl<T: PartialOrd + DepOrd + Clone + Send + Sync + 'static> Version for T {}

// `DepOrd` for dependency check: `self` may not happen after `other`, but must
// happen after (not earlier than) something involves `id` that happens before
// the `other`
// say Get(k1) returns c1: {k1: 1, k2: 2} from above, then Get(k2) may return
// c2: {k2: 2} from above, or c2': {k2: 3, k3: 1} that may happen after that c2
// (e.g. for a Put(k2, _, {k3: 1})), so that c2/c2'.dep_cmp(c1).is_ge() == true,
// but not c2'': {k2: 1}
pub trait DepOrd {
    fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering;

    // the `id`s that when calling `other.dep_cmp(self, id)`, Less may ever get returned
    // in another word, all `KeyId`s that `self` may have opinion regarding dependency check
    fn deps(&self) -> impl Iterator<Item = KeyId> + '_;
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Put<V, A> {
    key: KeyId,
    value: String,
    deps: BTreeMap<KeyId, V>,
    client_addr: A,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct PutOk<V> {
    version_deps: V,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Get<A> {
    key: KeyId,
    client_addr: A,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct GetOk<V> {
    value: String,
    version_deps: V,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct SyncKey<V> {
    key: KeyId,
    value: String,
    version_deps: V,
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
    working_key: Option<KeyId>,

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

impl<N: ClientNet<A, V>, U, V: Version, A: Addr> OnEvent<Invoke<ycsb::Op>> for Client<N, U, V, A> {
    fn on_event(
        &mut self,
        Invoke(op): Invoke<ycsb::Op>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let key = match &op {
            ycsb::Op::Read(key) | ycsb::Op::Update(key, ..) => key
                // easy way to adapt current YCSB key format (easier than adapt on YCSB side)
                .strip_prefix("user")
                .ok_or(anyhow::format_err!("malformed key name: {key}"))?
                .parse()?,
            _ => anyhow::bail!("unimplemented"),
        };
        let replaced = self.working_key.replace(key);
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

impl<N, U: SendEvent<InvokeOk<ycsb::Result>>, V: Version, A> OnEvent<Recv<PutOk<V>>>
    for Client<N, U, V, A>
{
    fn on_event(
        &mut self,
        Recv(put_ok): Recv<PutOk<V>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(key) = self.working_key.take() else {
            anyhow::bail!("missing working key")
        };
        if !self.deps.values().all(|dep| {
            matches!(
                put_ok.version_deps.partial_cmp(dep),
                Some(Ordering::Greater)
            )
        }) {
            return Ok(());
        }
        self.deps = [(key, put_ok.version_deps)].into();
        self.upcall.send((Default::default(), ycsb::Result::Ok)) // careful
    }
}

impl<N, U: SendEvent<InvokeOk<ycsb::Result>>, V: Version, A> OnEvent<Recv<GetOk<V>>>
    for Client<N, U, V, A>
{
    fn on_event(
        &mut self,
        Recv(get_ok): Recv<GetOk<V>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(key) = self.working_key.take() else {
            anyhow::bail!("missing working key")
        };
        if !self
            .deps
            .values()
            .all(|dep| get_ok.version_deps.dep_cmp(dep, key).is_ge())
        {
            return Ok(());
        }
        self.deps.insert(key, get_ok.version_deps);
        self.upcall
            .send((Default::default(), ycsb::Result::ReadOk(vec![get_ok.value])))
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
        let key = key
            .strip_prefix("user")
            .ok_or(anyhow::format_err!("unimplemented"))?
            .parse()?;
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
        let get_ok = if let Some(state) = self.store.get(&get.key) {
            GetOk {
                value: state.value.clone(),
                version_deps: state.version_deps.clone(),
            }
        } else {
            // reasonable default?
            // GetOk {
            //     value: Default::default(),
            //     version_deps: self.version_zero.clone(),
            // }
            anyhow::bail!("missing state for key {}", get.key)
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
        for (id, dep) in &put.deps {
            if !self
                .store
                .get(id)
                .map(|state| &state.version_deps)
                .unwrap_or(&self.version_zero)
                .dep_cmp(dep, *id)
                .is_ge()
            {
                return Ok(());
            }
        }
        let state = self.store.entry(put.key).or_insert_with(|| KeyState {
            value: Default::default(),
            version_deps: self.version_zero.clone(),
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

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn can_sync(&self, sync_key: &SyncKey<M::V>) -> bool {
        for id in sync_key.version_deps.deps() {
            if id == sync_key.key {
                continue;
            }
            if !self
                .store
                .get(&id)
                .map(|state| &state.version_deps)
                .unwrap_or(&self.version_zero)
                .dep_cmp(&sync_key.version_deps, id)
                .is_ge()
            {
                return false;
            }
        }
        true
    }

    fn apply_sync(&mut self, sync_key: SyncKey<M::V>) -> anyhow::Result<()> {
        if let Some(state) = self.store.get_mut(&sync_key.key) {
            anyhow::ensure!(
                state.pending_puts.is_empty(),
                "conflicting Put across servers"
            );
            if !matches!(
                sync_key.version_deps.partial_cmp(&state.version_deps),
                Some(Ordering::Greater)
            ) {
                //
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

impl<M: ReplicaCommon> OnEvent<Recv<SyncKey<M::V>>> for Replica<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn on_event(
        &mut self,
        Recv(sync_key): Recv<SyncKey<M::V>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if !self.can_sync(&sync_key) {
            self.pending_sync_keys.push(sync_key);
            return Ok(());
        }
        self.apply_sync(sync_key)?;
        for sync_key in take(&mut self.pending_sync_keys) {
            if !self.can_sync(&sync_key) {
                self.pending_sync_keys.push(sync_key);
                continue;
            }
            self.apply_sync(sync_key)?
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
            update_ok.version_deps.partial_cmp(dep),
            Some(Ordering::Greater)
        )));
        anyhow::ensure!(matches!(
            update_ok.version_deps.partial_cmp(&state.version_deps),
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
    sender: &mut impl SendReplicaRecvEvent<V, A>,
) -> anyhow::Result<()> {
    match deserialize(buf)? {
        ToReplicaMessage::Put(message) => sender.send(Recv(message)),
        ToReplicaMessage::Get(message) => sender.send(Recv(message)),
        ToReplicaMessage::SyncKey(message) => sender.send(Recv(message)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct DefaultVersion(BTreeMap<KeyId, u32>);

impl DefaultVersion {
    pub fn new() -> Self {
        Self::default()
    }

    fn merge(&self, other: &Self) -> Self {
        let mut merged = self.0.clone();
        for (id, version) in &other.0 {
            merged
                .entry(*id)
                .and_modify(|v| *v = (*v).max(*version))
                .or_insert(*version);
        }
        Self(merged)
    }
}

impl PartialOrd for DefaultVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let merged = self.merge(other);
        match (merged == *self, merged == *other) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => None,
        }
    }
}

impl DepOrd for DefaultVersion {
    fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering {
        match (self.0.get(&id), other.0.get(&id)) {
            // handy sanity check
            (Some(0), _) | (_, Some(0)) => {
                unimplemented!("invalid dependency compare: {self:?} vs {other:?} @ {id}")
            }
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            // this can happen on the startup insertion
            (None, None) => Ordering::Equal,
            (Some(n), Some(m)) => n.cmp(m),
        }
    }

    fn deps(&self) -> impl Iterator<Item = KeyId> + '_ {
        self.0.keys().copied()
    }
}

#[derive(Debug)]
pub struct DefaultVersionService<E>(pub E);

impl<E: SendEvent<events::UpdateOk<DefaultVersion>>> SendEvent<events::Update<DefaultVersion>>
    for DefaultVersionService<E>
{
    fn send(&mut self, update: events::Update<DefaultVersion>) -> anyhow::Result<()> {
        let mut version_deps = update
            .deps
            .into_iter()
            .fold(update.prev, |version, dep| version.merge(&dep));
        *version_deps.0.entry(update.id).or_default() += 1;
        let update_ok = events::UpdateOk {
            id: update.id,
            version_deps,
        };
        self.0.send(update_ok)
    }
}

// cSpell:words deque upcall ycsb sosp
