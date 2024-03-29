use std::{
    cmp::Ordering::{Equal, Greater},
    collections::{BTreeMap, HashSet},
};

use crate::{
    event::{erased::OnEvent, Timer},
    message::Payload,
    net::{events::Recv, SendMessage},
};

pub struct Put<V, A> {
    key: Payload,
    value: Payload,
    deps: BTreeMap<Payload, V>,
    client_addr: A,
}

pub struct PutOk<V> {
    version: V,
}

pub struct Get<A> {
    key: Payload,
    client_addr: A,
}

pub struct GetOk<V> {
    value: Payload,
    version: V,
}

pub struct Sync<V, A> {
    key: Payload,
    version: V,
    server_addr: A,
}

pub struct SyncOk<V> {
    key: Payload,
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
    pub previous: V,
    pub deps: Vec<V>,
}

pub struct CreateVersionOk<V>(pub V);

// pub struct Client<V> {
// }

pub struct Server<N, CN, V, A> {
    store: BTreeMap<Payload, (Payload, V)>,
    net: N,
    client_net: CN,
    pending_puts: Vec<PendingPut<V, A>>,
}

struct PendingPut<V, A> {
    message: Put<V, A>,
    keys: HashSet<Payload>,
}

impl<N, CN: ClientNet<A, V>, A, V: Clone> OnEvent<Recv<Get<A>>> for Server<N, CN, V, A> {
    fn on_event(&mut self, Recv(get): Recv<Get<A>>, _: &mut impl Timer) -> anyhow::Result<()> {
        if let Some((value, version)) = self.store.get(&get.key) {
            let get_ok = GetOk {
                value: value.clone(),
                version: version.clone(),
            };
            return self.client_net.send(get.client_addr, get_ok);
        }
        // TODO
        Ok(())
    }
}

impl<N: ServerNet<A, V>, CN: ClientNet<A, V>, A, V: PartialOrd + Clone> OnEvent<Recv<Put<V, A>>>
    for Server<N, CN, V, A>
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
                Some(dep_key.clone())
            })
            .collect::<HashSet<_>>();
        if pending_sync_keys.is_empty() {
            // TODO
        }
        self.pending_puts.push(PendingPut {
            message: put,
            keys: pending_sync_keys,
        });
        // TODO
        Ok(())
    }
}

impl<N: ServerNet<A, V>, CN, A, V: PartialOrd + Clone> OnEvent<Recv<Sync<V, A>>>
    for Server<N, CN, V, A>
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

impl<N, CN: ClientNet<A, V>, A, V: PartialOrd + Clone> OnEvent<Recv<SyncOk<V>>>
    for Server<N, CN, V, A>
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
        self.store
            .insert(sync_ok.key, (sync_ok.value, sync_ok.version));
        // TODO
        Ok(())
    }
}
