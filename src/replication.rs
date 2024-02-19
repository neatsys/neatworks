use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{
    event::{
        erased::{OnEvent, Timer},
        SendEvent,
    },
    net::{Addr, SendMessage, SendMessageToEach, SendMessageToEachExt as _},
};

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Vec<u8>,
}

pub struct CloseLoop<E> {
    client_senders: HashMap<u32, E>,
    pub latencies: Vec<Duration>,
    invoke_instants: HashMap<u32, Instant>,
    max_count: Option<(usize, ConcurrentStop)>,
}

type ConcurrentStop = Box<dyn FnOnce() -> anyhow::Result<()> + Send + Sync>;

impl<E> Debug for CloseLoop<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Concurrent")
            .field("<count>", &self.latencies.len())
            .finish_non_exhaustive()
    }
}

impl<E> CloseLoop<E> {
    pub fn new() -> Self {
        Self {
            client_senders: Default::default(),
            latencies: Default::default(),
            invoke_instants: Default::default(),
            max_count: None,
        }
    }
}

impl<E> Default for CloseLoop<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> CloseLoop<E> {
    pub fn insert_client_sender(&mut self, client_id: u32, sender: E) -> anyhow::Result<()> {
        let replaced = self.client_senders.insert(client_id, sender);
        if replaced.is_none() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("duplicated client id"))
        }
    }

    pub fn insert_max_count(&mut self, count: NonZeroUsize, stop: ConcurrentStop) {
        self.max_count = Some((count.into(), stop));
    }
}

#[derive(Debug, Clone)]
pub struct Invoke(pub Vec<u8>);

pub type InvokeOk = (u32, Vec<u8>);

impl<E> CloseLoop<E>
where
    E: SendEvent<Invoke>,
{
    pub fn launch(&mut self) -> anyhow::Result<()> {
        for (client_id, sender) in &mut self.client_senders {
            sender.send(Invoke(Vec::new()))?; // TODO
            self.invoke_instants.insert(*client_id, Instant::now());
        }
        Ok(())
    }
}

impl<E> OnEvent<InvokeOk> for CloseLoop<E>
where
    E: SendEvent<Invoke>,
{
    fn on_event(
        &mut self,
        (client_id, _result): InvokeOk,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(sender) = self.client_senders.get_mut(&client_id) else {
            anyhow::bail!("unknown client id {client_id}")
        };
        sender.send(Invoke(Vec::new()))?; // TODO
        let replaced_instant = self
            .invoke_instants
            .insert(client_id, Instant::now())
            .ok_or(anyhow::anyhow!(
                "missing invocation instant of client id {client_id}"
            ))?;
        self.latencies.push(replaced_instant.elapsed());
        if let Some((count, _)) = &self.max_count {
            if self.latencies.len() == *count {
                (self.max_count.take().unwrap().1)()?
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaNet<N, A> {
    inner_net: N,
    replica_addrs: Vec<A>,
    all_except: Option<usize>,
}

impl<N, A> ReplicaNet<N, A> {
    pub fn new(inner_net: N, replica_addrs: Vec<A>, replica_id: impl Into<Option<u8>>) -> Self {
        Self {
            inner_net,
            replica_addrs,
            all_except: replica_id.into().map(|id| id as usize),
        }
    }
}

impl<N: SendMessage<A, M>, A: Addr, M> SendMessage<u8, M> for ReplicaNet<N, A> {
    fn send(&mut self, dest: u8, message: M) -> anyhow::Result<()> {
        let dest = self
            .replica_addrs
            .get(dest as usize)
            .ok_or(anyhow::anyhow!("unknown replica id {dest}"))?;
        self.inner_net.send(dest.clone(), message)
    }
}

#[derive(Debug)]
pub struct AllReplica;

impl<N: for<'a> SendMessageToEach<A, M>, A: Addr, M> SendMessage<AllReplica, M>
    for ReplicaNet<N, A>
{
    fn send(&mut self, AllReplica: AllReplica, message: M) -> anyhow::Result<()> {
        let addrs = self
            .replica_addrs
            .iter()
            .enumerate()
            .filter_map(|(id, addr)| {
                if self.all_except == Some(id) {
                    None
                } else {
                    Some(addr.clone())
                }
            });
        self.inner_net.send_to_each(addrs, message)
    }
}
