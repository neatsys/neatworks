use std::{
    cmp::Ordering::{Equal, Greater},
    collections::{BTreeMap, HashSet},
};

use crate::{
    event::{erased::OnEvent, SendEvent, Timer},
    message::Payload,
    net::{events::Recv, SendMessage},
    worker::erased::Worker,
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

pub struct SyncKey<V, A> {
    key: KeyId,
    version: V,
    server_addr: A,
}

pub struct SyncKeyOk<V> {
    key: KeyId,
    value: Payload,
    version: V,
}

pub trait ClientNet<A, V>: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>> {}
impl<T: SendMessage<A, GetOk<V>> + SendMessage<A, PutOk<V>>, A, V> ClientNet<A, V> for T {}

pub trait ServerNet<A, V>:
    SendMessage<A, Put<V, A>>
    + SendMessage<A, Get<A>>
    + SendMessage<A, SyncKey<V, A>>
    + SendMessage<A, SyncKeyOk<V>>
{
}
impl<
        T: SendMessage<A, Put<V, A>>
            + SendMessage<A, Get<A>>
            + SendMessage<A, SyncKey<V, A>>
            + SendMessage<A, SyncKeyOk<V>>,
        A,
        V,
    > ServerNet<A, V> for T
{
}

pub trait VersionService {
    type Version;
    fn merge_and_increment_once(
        &self,
        id: KeyId,
        previous: Option<Self::Version>,
        deps: Vec<Self::Version>,
    ) -> anyhow::Result<Self::Version>;
}

pub trait Version: PartialOrd + Clone + Send + Sync + 'static {}
impl<T: PartialOrd + Clone + Send + Sync + 'static> Version for T {}

pub struct VersionOk<V>(pub KeyId, pub V);

// pub struct Client<V> {
// }

pub struct Server<N, CN, VS, E, V, A> {
    store: BTreeMap<KeyId, (Payload, V)>,
    version_zero: V,
    pending_puts: Vec<Put<V, A>>,
    pending_gets: Vec<Get<A>>,
    net: N,
    client_net: CN,
    version_worker: Worker<VS, E>,
}

impl<N, CN: ClientNet<A, V>, A, V: Clone, VS, E> OnEvent<Recv<Get<A>>>
    for Server<N, CN, VS, E, V, A>
{
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

impl<
        N: ServerNet<A, V>,
        CN: ClientNet<A, V>,
        A,
        V: Version,
        VS: VersionService<Version = V>,
        E: SendEvent<VersionOk<V>>,
    > OnEvent<Recv<Put<V, A>>> for Server<N, CN, VS, E, V, A>
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
            self.version_worker
                .submit(Box::new(move |service, sender| {
                    let version = service.merge_and_increment_once(put.key, previous, deps)?;
                    sender.send(VersionOk(put.key, version))
                }))?;
        }
        // TODO figure out who to sync with and send Sync
        self.pending_puts.push(put);
        Ok(())
    }
}

impl<N: ServerNet<A, V>, CN, A, V: PartialOrd + Clone, VS, E> OnEvent<Recv<SyncKey<V, A>>>
    for Server<N, CN, VS, E, V, A>
{
    fn on_event(
        &mut self,
        Recv(sync): Recv<SyncKey<V, A>>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some((value, version)) = self.store.get(&sync.key) {
            if matches!(version.partial_cmp(&sync.version), Some(Greater | Equal)) {
                let sync_ok = SyncKeyOk {
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

impl<
        N,
        CN: ClientNet<A, V>,
        A,
        V: Version,
        VS: VersionService<Version = V>,
        E: SendEvent<VersionOk<V>>,
    > OnEvent<Recv<SyncKeyOk<V>>> for Server<N, CN, VS, E, V, A>
{
    fn on_event(
        &mut self,
        Recv(sync_ok): Recv<SyncKeyOk<V>>,
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
                let id = put.key;
                let previous = self.store.get(&put.key).map(|(_, version)| version.clone());
                let deps = put.deps.values().cloned().collect();
                self.version_worker
                    .submit(Box::new(move |service, sender| {
                        let version = service.merge_and_increment_once(id, previous, deps)?;
                        sender.send(VersionOk(id, version))
                    }))?
            }
        }

        Ok(())
    }
}

impl<
        N,
        CN: ClientNet<A, V>,
        A,
        V: PartialOrd + Clone,
        VS: VersionService<Version = V>,
        E: SendEvent<VersionOk<V>>,
    > OnEvent<VersionOk<V>> for Server<N, CN, VS, E, V, A>
{
    fn on_event(
        &mut self,
        VersionOk(key, version): VersionOk<V>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        //
        Ok(())
    }
}
