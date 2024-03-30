use std::{
    cmp::Ordering::{Equal, Greater},
    collections::{BTreeMap, HashSet},
};

use crate::{
    event::{erased::OnEvent, SendEvent, Timer},
    message::Payload,
    net::{events::Recv, SendMessage},
};

// "key" under COPS context, "id" under Boson's logical clock context
pub type KeyId = u32;

pub struct Put<V, A> {
    key: KeyId,
    value: Payload,
    deps: BTreeMap<KeyId, V>,
    client_addr: A,
}

pub struct PutOk<V> {
    version: V,
}

pub struct Get<A> {
    key: KeyId,
    client_addr: A,
}

pub struct GetOk<V> {
    value: Payload,
    version: V,
}

pub struct Sync<V, A> {
    key: KeyId,
    version: V,
    server_addr: A,
}

pub struct SyncOk<V> {
    key: KeyId,
    value: Payload,
    version: V,
}

pub trait ClientNet<A, V>: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>> {}
impl<T: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>>, A, V> ClientNet<A, V> for T {}

pub trait ServerNet<A, V>:
    SendMessage<A, Put<V, A>>
    + SendMessage<A, Get<A>>
    + SendMessage<A, Sync<V, A>>
    + SendMessage<A, SyncOk<V>>
{
}
impl<
        T: SendMessage<A, Put<V, A>>
            + SendMessage<A, Get<A>>
            + SendMessage<A, Sync<V, A>>
            + SendMessage<A, SyncOk<V>>,
        A,
        V,
    > ServerNet<A, V> for T
{
}

pub struct CreateVersion<V> {
    pub id: KeyId,
    pub previous: Option<V>,
    pub deps: Vec<V>,
}

pub struct CreateVersionOk<V>(pub KeyId, pub V);

// pub struct Client<V> {
// }

pub trait VersionService<V>: SendEvent<CreateVersion<V>> {}
impl<T: SendEvent<CreateVersion<V>>, V> VersionService<V> for T {}

pub struct Server<N, CN, S, V, A> {
    store: BTreeMap<KeyId, (Payload, V)>,
    version_zero: V,
    pending_puts: Vec<Put<V, A>>,
    pending_gets: Vec<Get<A>>,
    net: N,
    client_net: CN,
    version_service: S,
}

impl<N, CN: ClientNet<A, V>, A, V: Clone, S> OnEvent<Recv<Get<A>>> for Server<N, CN, S, V, A> {
    fn on_event(&mut self, Recv(get): Recv<Get<A>>, _: &mut impl Timer) -> anyhow::Result<()> {
        if let Some((value, version)) = self.store.get(&get.key) {
            let get_ok = GetOk {
                value: value.clone(),
                version: version.clone(),
            };
            return self.client_net.send(get.client_addr, get_ok);
        }
        // TODO figure out who to sync with and send Sync
        self.pending_gets.push(get);
        Ok(())
    }
}

impl<N: ServerNet<A, V>, CN: ClientNet<A, V>, A, V: PartialOrd + Clone, S: VersionService<V>>
    OnEvent<Recv<Put<V, A>>> for Server<N, CN, S, V, A>
{
    fn on_event(&mut self, Recv(put): Recv<Put<V, A>>, _: &mut impl Timer) -> anyhow::Result<()> {
        if let Some((_, version)) = self.store.get(&put.key) {
            if put
                .deps
                .iter()
                .all(|(_, v)| matches!(version.partial_cmp(v), Some(Greater | Equal)))
            {
                let put_ok = PutOk {
                    version: version.clone(),
                };
                return self.client_net.send(put.client_addr, put_ok);
            }
        }
        let pending_sync_keys = put
            .deps
            .iter()
            .filter_map(|(dep_key, dep_version)| {
                if let Some((_, version)) = self.store.get(dep_key) {
                    if matches!(version.partial_cmp(dep_version), Some(Greater | Equal)) {
                        return None;
                    }
                }
                Some(dep_key)
            })
            .collect::<HashSet<_>>();
        if pending_sync_keys.is_empty() {
            let deps = put
                .deps
                .keys()
                .map(|dep_key| self.store.get(dep_key).unwrap().1.clone())
                .collect::<Vec<_>>();
            let previous = self.store.get(&put.key).map(|(_, version)| version.clone());
            // shortcut only when both condition holds
            // if there are already dependencies on the first version, the version should not start
            // with `version_zero` anymore
            // if there is no dependency but it's not the first version, the version still need to
            // be incremented
            if deps.is_empty() && previous.is_none() {
                self.store
                    .insert(put.key, (put.value, self.version_zero.clone()));
                let put_ok = PutOk {
                    version: self.version_zero.clone(),
                };
                return self.client_net.send(put.client_addr, put_ok);
            }
            self.version_service.send(CreateVersion {
                id: put.key,
                previous,
                deps,
            })?;
        }
        // TODO figure out who to sync with and send Sync
        self.pending_puts.push(put);
        Ok(())
    }
}

impl<N: ServerNet<A, V>, CN, A, V: PartialOrd + Clone, S> OnEvent<Recv<Sync<V, A>>>
    for Server<N, CN, S, V, A>
{
    fn on_event(&mut self, Recv(sync): Recv<Sync<V, A>>, _: &mut impl Timer) -> anyhow::Result<()> {
        if let Some((value, version)) = self.store.get(&sync.key) {
            if matches!(version.partial_cmp(&sync.version), Some(Greater | Equal)) {
                let sync_ok = SyncOk {
                    key: sync.key,
                    value: value.clone(),
                    version: version.clone(),
                };
                self.net.send(sync.server_addr, sync_ok)?
            }
        }
        Ok(())
    }
}

impl<N, CN: ClientNet<A, V>, A, V: PartialOrd + Clone, S: VersionService<V>>
    OnEvent<Recv<SyncOk<V>>> for Server<N, CN, S, V, A>
{
    fn on_event(
        &mut self,
        Recv(sync_ok): Recv<SyncOk<V>>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some((_, version)) = self.store.get(&sync_ok.key) {
            if !matches!(sync_ok.version.partial_cmp(version), Some(Greater)) {
                return Ok(());
            }
        }
        self.store.insert(
            sync_ok.key,
            (sync_ok.value.clone(), sync_ok.version.clone()),
        );

        let mut pending_gets = Vec::new();
        for get in self.pending_gets.drain(..) {
            if get.key != sync_ok.key {
                pending_gets.push(get)
            } else {
                let get_ok = GetOk {
                    value: sync_ok.value.clone(),
                    version: sync_ok.version.clone(),
                };
                self.client_net.send(get.client_addr, get_ok)?
            }
        }
        self.pending_gets = pending_gets;

        for put in &self.pending_puts {
            if put.deps.iter().all(|(dep_key, dep_version)| {
                if let Some((_, version)) = self.store.get(dep_key) {
                    matches!(version.partial_cmp(dep_version), Some(Greater | Equal))
                } else {
                    false
                }
            }) {
                self.version_service.send(CreateVersion {
                    id: put.key,
                    previous: self.store.get(&put.key).map(|(_, version)| version.clone()),
                    deps: put.deps.values().cloned().collect(),
                })?
            }
        }

        Ok(())
    }
}

impl<N, CN: ClientNet<A, V>, A, V: PartialOrd + Clone, S: VersionService<V>>
    OnEvent<CreateVersionOk<V>> for Server<N, CN, S, V, A>
{
    fn on_event(
        &mut self,
        CreateVersionOk(key, version): CreateVersionOk<V>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        //
        Ok(())
    }
}
