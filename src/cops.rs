// notes on this implementation of
// Don’t Settle for Eventual: Scalable Causal Consistency for Wide-Area Storage
// with COPS (SOSP'11)
// the implementation contains the COPS variant in the paper, COPS-GT and
// COPS-CD are not included
// the implementation is prepared for a Byzantine fault security model.
// additional checks and proof passing take places
// the version type `V` in this implementation does not map to the version
// number in the original work. instead, it roughly maps to the "nearest"
// dependency set (when i say "roughly" i mean it's the superset of the nearest
// set). the `V` values returned by server contains the original version number
// as well: it's a combination of the assigned version number of a `Put` value
// and the nearest set of that `Put`. that's why the `V` values sent by client
// and sent (and stored) by server are named `deps` and `version_deps`
// respectively
// the `version_deps` is something added to the original work. it enables client
// to learn about the nearest set of some version of a value in a verifiable way
// (assuming `V` is verifiable). thus client can check whether the version of
// values in the following replies consistent with this information, and reject
// malicious replies that violate causal consistency
// in a trusted setup, `version_deps` could just be the version number, and
// `dep_cmp` can always return Greater, as the case in the original work where
// every server/client is correctly behaving
// producing `V` typed value potentially takes long latency: it may require the
// computational expensive incrementally verifiable computation, or asynchronous
// network communication. so instead of inlined "version bumping" as in the
// original work, a version service is extracted to asynchronously produce `V`
// values. this incurs unnecessary overhead for the case that follows the
// original work's setup (i.e. the default version service included in this
// implementation), hopefully not critical (or even noticeable) since the work
// targets geological replication scenario
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
};

use serde::{Deserialize, Serialize};

use crate::{
    app::kvstore,
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent,
    },
    net::{events::Recv, Addr, All, SendMessage},
    util::Payload,
};

// "key" under COPS context, "id" under Boson's logical clock context
pub type KeyId = u32;

// precisely `deps` and `version_deps` should have different types
// get lazy on that

pub trait DepOrd {
    fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering;
}

// `self` must be `version_deps` and `other` must be `deps`
pub trait Version: PartialOrd + DepOrd + Clone + Send + Sync + 'static {}
impl<T: PartialOrd + DepOrd + Clone + Send + Sync + 'static> Version for T {}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Put<V, A> {
    key: KeyId,
    value: Payload,
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
    value: Payload,
    version_deps: V,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct SyncKey<V> {
    key: KeyId,
    value: Payload,
    version_deps: V,
}

pub trait ClientNet<A, V>: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>> {}
impl<T: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>>, A, V> ClientNet<A, V> for T {}

pub trait ServerNet<A, V>:
    SendMessage<u8, Put<V, A>> + SendMessage<u8, Get<A>> + SendMessage<All, SyncKey<V>>
{
}
impl<
        T: SendMessage<u8, Put<V, A>> + SendMessage<u8, Get<A>> + SendMessage<All, SyncKey<V>>,
        A,
        V,
    > ServerNet<A, V> for T
{
}

// events with version service
// version service expects at most one outstanding `Update<_>` per `id`

pub struct Update<V> {
    pub id: KeyId,
    pub previous: V, // `version_deps`
    pub deps: Vec<V>,
}

pub struct UpdateOk<V> {
    pub id: KeyId,
    pub version_deps: V,
}

pub struct Client<N, U, V, A> {
    addr: A,
    deps: BTreeMap<KeyId, V>,

    net: N,
    upcall: U,
}

impl<N: ServerNet<A, V>, U, V: Version, A: Addr> OnEvent<kvstore::Op> for Client<N, U, V, A> {
    fn on_event(&mut self, event: kvstore::Op, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        match event {
            kvstore::Op::Append(..) => anyhow::bail!("unimplemented"),
            kvstore::Op::Put(key, value) => {
                let put = Put {
                    key: key.parse()?,
                    value: Payload(value.into_bytes()),
                    deps: self.deps.clone(),
                    client_addr: self.addr.clone(),
                };
                self.net.send(0, put)
            }
            kvstore::Op::Get(key) => {
                let get = Get {
                    key: key.parse()?,
                    client_addr: self.addr.clone(),
                };
                self.net.send(0, get)
            }
        }
    }
}

impl<N, U: SendEvent<kvstore::Result>, V: Version, A> OnEvent<Recv<PutOk<V>>>
    for Client<N, U, V, A>
{
    fn on_event(
        &mut self,
        Recv(put_ok): Recv<PutOk<V>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if !self.deps.values().all(|dep| {
            matches!(
                put_ok.version_deps.partial_cmp(dep),
                Some(Ordering::Greater)
            )
        }) {
            return Ok(());
        }
        self.deps = [(0, put_ok.version_deps)].into(); // TODO
        self.upcall.send(kvstore::Result::PutOk)
    }
}

impl<N, U: SendEvent<kvstore::Result>, V: Version, A> OnEvent<Recv<GetOk<V>>>
    for Client<N, U, V, A>
{
    fn on_event(
        &mut self,
        Recv(get_ok): Recv<GetOk<V>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if !self
            .deps
            .iter()
            .all(|(id, dep)| get_ok.version_deps.dep_cmp(dep, *id).is_ge())
        {
            return Ok(());
        }
        self.deps.insert(0, get_ok.version_deps);
        self.upcall
            .send(kvstore::Result::GetResult(String::from_utf8(
                get_ok.value.0,
            )?))
    }
}

pub struct Server<N, CN, VS, V, A, _M = (N, CN, VS, V, A)> {
    store: BTreeMap<KeyId, KeyState<V, A>>,
    version_zero: V,
    net: N,
    client_net: CN,
    version_service: VS,
    _m: std::marker::PhantomData<_M>,
}

#[derive(Clone)]
struct KeyState<V, A> {
    value: Payload,
    version_deps: V,
    pending_puts: VecDeque<Put<V, A>>,
}

impl<N, CN, VS, V: Clone, A: Clone> Server<N, CN, VS, V, A> {
    pub fn new(version_zero: V, net: N, client_net: CN, version_service: VS) -> Self {
        Self {
            net,
            client_net,
            version_service,
            version_zero,
            store: Default::default(),
            _m: Default::default(),
        }
    }
}

pub trait ServerCommon {
    type N: ServerNet<Self::A, Self::V>;
    type CN: ClientNet<Self::A, Self::V>;
    type VS: SendEvent<Update<Self::V>>;
    type V: Version;
    type A: Addr;
}
impl<N, CN, VS, V, A> ServerCommon for (N, CN, VS, V, A)
where
    N: ServerNet<A, V>,
    CN: ClientNet<A, V>,
    VS: SendEvent<Update<V>>,
    V: Version,
    A: Addr,
{
    type N = N;
    type CN = CN;
    type VS = VS;
    type V = V;
    type A = A;
}

impl<M: ServerCommon> OnEvent<Recv<Get<M::A>>> for Server<M::N, M::CN, M::VS, M::V, M::A, M> {
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
            GetOk {
                value: Default::default(),
                version_deps: self.version_zero.clone(),
            }
        };
        self.client_net.send(get.client_addr, get_ok)
    }
}

impl<M: ServerCommon> OnEvent<Recv<Put<M::V, M::A>>> for Server<M::N, M::CN, M::VS, M::V, M::A, M> {
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
            let merge = Update {
                id: put.key,
                previous: state.version_deps.clone(),
                deps: put.deps.into_values().collect(),
            };
            self.version_service.send(merge)?
        }
        Ok(())
    }
}

impl<M: ServerCommon> OnEvent<Recv<SyncKey<M::V>>> for Server<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn on_event(
        &mut self,
        Recv(_): Recv<SyncKey<M::V>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }
}

impl<M: ServerCommon> OnEvent<UpdateOk<M::V>> for Server<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn on_event(
        &mut self,
        update_ok: UpdateOk<M::V>,
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
            let merge = Update {
                id: update_ok.id,
                previous: update_ok.version_deps,
                deps: pending_put.deps.values().cloned().collect(),
            };
            self.version_service.send(merge)?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultVersion(KeyId, u32);

impl DefaultVersion {
    pub fn new(id: KeyId) -> Self {
        Self(id, 0)
    }
}

impl PartialOrd for DefaultVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.0 == other.0 {
            Some(self.1.cmp(&other.1))
        } else {
            Some(Ordering::Greater)
        }
    }
}

impl DepOrd for DefaultVersion {
    fn dep_cmp(&self, _: &Self, id: KeyId) -> Ordering {
        assert_eq!(id, self.0);
        Ordering::Greater
    }
}

pub struct DefaultVersionService<E>(E);

impl<E: SendEvent<UpdateOk<DefaultVersion>>> OnEvent<Update<DefaultVersion>>
    for DefaultVersionService<E>
{
    fn on_event(
        &mut self,
        update: Update<DefaultVersion>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let DefaultVersion(id, version) = update.previous;
        anyhow::ensure!(id == update.id);
        let update_ok = UpdateOk {
            id: update.id,
            version_deps: DefaultVersion(update.id, version + 1),
        };
        self.0.send(update_ok)
    }
}

// cSpell:words deque upcall kvstore sosp
