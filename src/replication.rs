use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{
    event::{OnEvent, SendEvent, Timer},
    net::{Buf, SendBuf},
};

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Vec<u8>,
}

#[derive(Debug)]
pub struct Concurrent<E> {
    client_senders: HashMap<u32, E>,
    pub latencies: Vec<Duration>,
    invoke_instants: HashMap<u32, Instant>,
}

impl<E> Concurrent<E> {
    pub fn new() -> Self {
        Self {
            client_senders: Default::default(),
            latencies: Default::default(),
            invoke_instants: Default::default(),
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
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaNet<N> {
    socket_net: N,
    replica_addrs: Vec<SocketAddr>,
    all_except: Option<usize>,
}

impl<N> ReplicaNet<N> {
    pub fn new(
        socket_net: N,
        replica_addrs: Vec<SocketAddr>,
        replica_id: impl Into<Option<u8>>,
    ) -> Self {
        Self {
            socket_net,
            replica_addrs,
            all_except: replica_id.into().map(|id| id as usize),
        }
    }
}

// impl<N: SendMessage<M, Addr = SocketAddr>, M> SendMessage<M> for ReplicaNet<N> {
//     type Addr = u8;

//     fn send(&self, dest: Self::Addr, message: &M) -> anyhow::Result<()> {
//         let dest = self
//             .replica_addrs
//             .get(dest as usize)
//             .ok_or(anyhow::anyhow!("unknown replica id {dest}"))?;
//         self.socket_net.send(*dest, message)
//     }

//     fn send_to_all(&self, message: &M) -> anyhow::Result<()> {
//         todo!()
//     }
// }

impl<N: SendBuf<Addr = SocketAddr>> SendBuf for ReplicaNet<N> {
    type Addr = u8;

    fn send(&self, dest: Self::Addr, buf: impl Buf) -> anyhow::Result<()> {
        let dest = self
            .replica_addrs
            .get(dest as usize)
            .ok_or(anyhow::anyhow!("unknown replica id {dest}"))?;
        self.socket_net.send(*dest, buf)
    }

    fn send_to_all(&self, buf: impl Buf) -> anyhow::Result<()> {
        for (id, dest) in self.replica_addrs.iter().enumerate() {
            if self.all_except == Some(id) {
                continue;
            }
            self.socket_net.send(*dest, buf.clone())?
        }
        Ok(())
    }
}
