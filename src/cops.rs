use std::{cmp::Ordering, collections::VecDeque};

use serde::{Deserialize, Serialize};

use crate::{
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent,
    },
    net::{events::Recv, Addr, All, SendMessage},
    util::Payload,
};

// "key" under COPS context, "id" under Boson's logical clock context
pub type KeyId = u32;

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Put<V, A> {
    key: KeyId,
    value: Payload,
    deps: V,
    client_addr: A,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct PutOk<V> {
    version_deps: V,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Get<V, A> {
    key: KeyId,
    deps: V,
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
    SendMessage<u8, Put<V, A>> + SendMessage<u8, Get<V, A>> + SendMessage<All, SyncKey<V>>
{
}
impl<
        T: SendMessage<u8, Put<V, A>> + SendMessage<u8, Get<V, A>> + SendMessage<All, SyncKey<V>>,
        A,
        V,
    > ServerNet<A, V> for T
{
}

pub trait DepOrd {
    fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering;
}

pub trait Version: PartialOrd + DepOrd + Clone + Send + Sync + 'static {}
impl<T: PartialOrd + DepOrd + Clone + Send + Sync + 'static> Version for T {}

pub struct IncompleteMerge<V>(pub V, pub V);

pub struct CompleteMerge<V> {
    pub id: KeyId,
    pub previous: V, // must be complete, consider encode this into type
    pub deps: V,     // either complete or incomplete
}

pub struct IncompleteMerged<V>(pub V);

pub struct CompleteMerged<V> {
    pub id: KeyId,
    pub version_deps: V,
}

// pub struct Client<V> {
// }

pub struct Server<N, CN, VS, V, A, _M = (N, CN, VS, V, A)> {
    store: Vec<KeyState<V, A>>,
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
    pub fn new(
        num_key: usize,
        version_zero: V,
        net: N,
        client_net: CN,
        version_service: VS,
    ) -> Self {
        Self {
            store: vec![
                KeyState {
                    value: Default::default(),
                    version_deps: version_zero,
                    pending_puts: Default::default()
                };
                num_key
            ],
            net,
            client_net,
            version_service,
            _m: Default::default(),
        }
    }
}

pub trait ServerCommon {
    type N: ServerNet<Self::A, Self::V>;
    type CN: ClientNet<Self::A, Self::V>;
    type VS: SendEvent<CompleteMerge<Self::V>>;
    type V: Version;
    type A: Addr;
}
impl<N, CN, VS, V, A> ServerCommon for (N, CN, VS, V, A)
where
    N: ServerNet<A, V>,
    CN: ClientNet<A, V>,
    VS: SendEvent<CompleteMerge<V>>,
    V: Version,
    A: Addr,
{
    type N = N;
    type CN = CN;
    type VS = VS;
    type V = V;
    type A = A;
}

impl<M: ServerCommon> OnEvent<Recv<Get<M::V, M::A>>> for Server<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn on_event(
        &mut self,
        Recv(get): Recv<Get<M::V, M::A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        for (id, state) in self.store.iter().enumerate() {
            if get.deps.dep_cmp(&state.version_deps, id as _).is_gt() {
                //
                return Ok(());
            }
        }
        let state = &self.store[get.key as usize];
        let get_ok = GetOk {
            value: state.value.clone(),
            version_deps: state.version_deps.clone(),
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
        for (id, state) in self.store.iter().enumerate() {
            if put.deps.dep_cmp(&state.version_deps, id as _).is_gt() {
                //
                return Ok(());
            }
        }
        let state = &mut self.store[put.key as usize];
        state.pending_puts.push_back(put.clone());
        if state.pending_puts.len() == 1 {
            let merge = CompleteMerge {
                id: put.key,
                previous: state.version_deps.clone(),
                deps: put.deps,
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

impl<M: ServerCommon> OnEvent<CompleteMerged<M::V>> for Server<M::N, M::CN, M::VS, M::V, M::A, M> {
    fn on_event(
        &mut self,
        merged: CompleteMerged<M::V>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let state = &mut self.store[merged.id as usize];
        let Some(put) = state.pending_puts.pop_front() else {
            anyhow::bail!("missing pending puts")
        };
        anyhow::ensure!(matches!(
            merged.version_deps.partial_cmp(&put.deps),
            Some(Ordering::Greater)
        ));
        anyhow::ensure!(matches!(
            merged.version_deps.partial_cmp(&state.version_deps),
            Some(Ordering::Greater)
        ));
        state.value = put.value.clone();
        state.version_deps = merged.version_deps.clone();
        let put_ok = PutOk {
            version_deps: merged.version_deps.clone(),
        };
        self.client_net.send(put.client_addr, put_ok)?;
        let sync_key = SyncKey {
            key: put.key,
            value: put.value,
            version_deps: merged.version_deps.clone(),
        };
        self.net.send(All, sync_key)?;
        if let Some(pending_put) = state.pending_puts.front() {
            let merge = CompleteMerge {
                id: merged.id,
                previous: merged.version_deps,
                deps: pending_put.deps.clone(),
            };
            self.version_service.send(merge)?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorClock(Vec<u32>);

impl VectorClock {
    pub fn new(num_key: usize) -> Self {
        Self(vec![0; num_key])
    }

    fn merge(&self, other: &Self) -> Self {
        Self(
            self.0
                .iter()
                .zip(&other.0)
                .map(|(a, b)| a.max(b))
                .copied()
                .collect(),
        )
    }

    fn update(&self, id: KeyId, other: &Self) -> Self {
        let mut merged = self.merge(other);
        merged.0[id as usize] += 1;
        merged
    }
}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let merged = self.merge(other);
        match (*self == merged, *other == merged) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => None,
        }
    }
}

impl DepOrd for VectorClock {
    fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering {
        self.0[id as usize].cmp(&other.0[id as usize])
    }
}

pub struct VectorClockService<E>(E);

impl<E: SendEvent<IncompleteMerged<VectorClock>>> OnEvent<IncompleteMerge<VectorClock>>
    for VectorClockService<E>
{
    fn on_event(
        &mut self,
        IncompleteMerge(clock, another_clock): IncompleteMerge<VectorClock>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        self.0.send(IncompleteMerged(clock.merge(&another_clock)))
    }
}

impl<E: SendEvent<CompleteMerged<VectorClock>>> OnEvent<CompleteMerge<VectorClock>>
    for VectorClockService<E>
{
    fn on_event(
        &mut self,
        event: CompleteMerge<VectorClock>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let merged = CompleteMerged {
            id: event.id,
            version_deps: event.previous.update(event.id, &event.deps),
        };
        self.0.send(merged)
    }
}

// cSpell:words deque
