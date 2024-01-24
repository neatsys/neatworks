use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{
    event::{OnEvent, SendEvent, Timer},
    net::{Addr, Buf, SendMessage},
};

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Vec<u8>,
}

pub struct Concurrent<E> {
    client_senders: HashMap<u32, E>,
    pub latencies: Vec<Duration>,
    invoke_instants: HashMap<u32, Instant>,
    max_count: Option<(usize, ConcurrentStop)>,
}

type ConcurrentStop = Box<dyn FnOnce() -> anyhow::Result<()> + Send + Sync>;

impl<E> Debug for Concurrent<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Concurrent")
            .field("<count>", &self.latencies.len())
            .finish_non_exhaustive()
    }
}

impl<E> Concurrent<E> {
    pub fn new() -> Self {
        Self {
            client_senders: Default::default(),
            latencies: Default::default(),
            invoke_instants: Default::default(),
            max_count: None,
        }
    }
}

impl<E> Default for Concurrent<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Concurrent<E> {
    pub fn insert_client_sender(&mut self, client_id: u32, sender: E) -> anyhow::Result<()> {
        let replaced = self.client_senders.insert(client_id, sender);
        if replaced.is_none() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("duplicated client id"))
        }
    }

    pub fn insert_max_count(&mut self, count: NonZeroUsize, stop: ConcurrentStop) {
        let _ = self.max_count.insert((count.into(), stop));
    }
}

pub type ConcurrentEvent = (u32, Vec<u8>);

impl<E> Concurrent<E>
where
    E: SendEvent<Vec<u8>>,
{
    pub fn launch(&mut self) -> anyhow::Result<()> {
        for (client_id, sender) in &self.client_senders {
            sender.send(Vec::new())?; // TODO
            self.invoke_instants.insert(*client_id, Instant::now());
        }
        Ok(())
    }
}

impl<E> OnEvent<ConcurrentEvent> for Concurrent<E>
where
    E: SendEvent<Vec<u8>>,
{
    fn on_event(
        &mut self,
        event: ConcurrentEvent,
        _: &mut dyn Timer<ConcurrentEvent>,
    ) -> anyhow::Result<()> {
        let (client_id, _result) = event;
        let Some(sender) = self.client_senders.get(&client_id) else {
            anyhow::bail!("unknown client id {client_id}")
        };
        sender.send(Vec::new())?; // TODO
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
    socket_net: N,
    replica_addrs: Vec<A>,
    all_except: Option<usize>,
}

impl<N, A> ReplicaNet<N, A> {
    pub fn new(socket_net: N, replica_addrs: Vec<A>, replica_id: impl Into<Option<u8>>) -> Self {
        Self {
            socket_net,
            replica_addrs,
            all_except: replica_id.into().map(|id| id as usize),
        }
    }
}

impl<N: SendMessage<A, M>, A: Addr, M> SendMessage<u8, M> for ReplicaNet<N, A> {
    fn send(&self, dest: u8, message: M) -> anyhow::Result<()> {
        let dest = self
            .replica_addrs
            .get(dest as usize)
            .ok_or(anyhow::anyhow!("unknown replica id {dest}"))?;
        self.socket_net.send(dest.clone(), message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AllReplica;

// intentionally (seems unnecessarily) restrict message type to `Buf` instead
// of just `Clone`
// broadcast should be performed after serialization to avoid duplicated
// serialization pitfall
// in another word, always wrap a raw net with `ReplicaNet` first, then desired
// message net
impl<N: SendMessage<A, B>, A: Addr, B: Buf> SendMessage<AllReplica, B> for ReplicaNet<N, A> {
    fn send(&self, AllReplica: AllReplica, message: B) -> anyhow::Result<()> {
        for (id, dest) in self.replica_addrs.iter().enumerate() {
            if self.all_except == Some(id) {
                continue;
            }
            self.socket_net.send(dest.clone(), message.clone())?
        }
        Ok(())
    }
}
