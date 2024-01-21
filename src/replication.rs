use std::{collections::HashMap, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::{
    event::{OnEvent, SendEvent, SessionSender, TimerEngine},
    net::{SendBuf, SendMessage},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Vec<u8>,
}

#[derive(Debug)]
pub struct Concurrent<M> {
    client_senders: HashMap<u32, SessionSender<M>>,
}

impl<M> Concurrent<M> {
    pub fn new() -> Self {
        Self {
            client_senders: Default::default(),
        }
    }
}

impl<M> Default for Concurrent<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Concurrent<M> {
    pub fn insert_client_sender(
        &mut self,
        client_id: u32,
        sender: SessionSender<M>,
    ) -> anyhow::Result<()> {
        let replaced = self.client_senders.insert(client_id, sender);
        if replaced.is_none() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("duplicated client id"))
        }
    }
}

pub type ConcurrentEvent = (u32, Vec<u8>);

impl<M> Concurrent<M>
where
    Vec<u8>: Into<M>,
{
    pub fn launch(&self) -> anyhow::Result<()> {
        for sender in self.client_senders.values() {
            sender.send(Vec::new())? // TODO
        }
        Ok(())
    }
}

impl<M> OnEvent<ConcurrentEvent> for Concurrent<M>
where
    Vec<u8>: Into<M>,
{
    fn on_event(
        &mut self,
        event: ConcurrentEvent,
        _: TimerEngine<'_, ConcurrentEvent>,
    ) -> anyhow::Result<()> {
        let (client_id, _result) = event;
        let Some(sender) = self.client_senders.get(&client_id) else {
            anyhow::bail!("unknown client id {client_id}")
        };
        sender.send(Vec::new()) // TODO
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaNet<N> {
    socket_net: N,
    replica_addrs: Vec<SocketAddr>,
}

impl<N> ReplicaNet<N> {
    pub fn new(socket_net: N, replica_addrs: Vec<SocketAddr>) -> Self {
        Self {
            socket_net,
            replica_addrs,
        }
    }
}

impl<N: SendMessage<M, Addr = SocketAddr>, M> SendMessage<M> for ReplicaNet<N> {
    type Addr = u8;

    fn send(&self, dest: Self::Addr, message: &M) -> anyhow::Result<()> {
        let dest = self
            .replica_addrs
            .get(dest as usize)
            .ok_or(anyhow::anyhow!("unknown replica id {dest}"))?;
        self.socket_net.send(*dest, message)
    }
}

impl<N: SendBuf<Addr = SocketAddr>> SendBuf for ReplicaNet<N> {
    type Addr = u8;

    fn send(&self, dest: Self::Addr, buf: Vec<u8>) -> anyhow::Result<()> {
        let dest = self
            .replica_addrs
            .get(dest as usize)
            .ok_or(anyhow::anyhow!("unknown replica id {dest}"))?;
        self.socket_net.send(*dest, buf)
    }
}
