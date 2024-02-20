use std::{
    collections::HashMap,
    fmt::Debug,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Invoke(pub Vec<u8>);

// newtype namespace may be desired after the type erasure migration
pub type InvokeOk = (u32, Vec<u8>);

pub struct CloseLoop<I> {
    clients: HashMap<u32, Box<dyn SendEvent<Invoke> + Send + Sync>>,
    op_iter: I,
    pub latencies: Option<Vec<Duration>>,
    invoke_instants: HashMap<u32, Instant>,
    pub invocations: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    invoke_ops: HashMap<u32, Vec<u8>>,
    pub stop: Option<CloseLoopStop>,
    pub done: bool,
}

type CloseLoopStop = Box<dyn FnOnce() -> anyhow::Result<()> + Send + Sync>;

impl<I> Debug for CloseLoop<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloseLoop").finish_non_exhaustive()
    }
}

impl<I> CloseLoop<I> {
    pub fn new(op_iter: I) -> Self {
        Self {
            op_iter,
            clients: Default::default(),
            latencies: Default::default(),
            invoke_instants: Default::default(),
            invocations: Default::default(),
            invoke_ops: Default::default(),
            stop: None,
            done: false,
        }
    }

    pub fn insert_client(
        &mut self,
        client_id: u32,
        sender: impl SendEvent<Invoke> + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        let replaced = self.clients.insert(client_id, Box::new(sender));
        if replaced.is_none() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("duplicated client id"))
        }
    }
}

impl<I: Clone> CloseLoop<I> {
    pub fn duplicate<E: SendEvent<Invoke> + Send + Sync + 'static>(
        &self,
        client: impl Fn() -> E,
    ) -> anyhow::Result<Self> {
        if self.latencies.is_some() || self.stop.is_some() {
            anyhow::bail!("real time close loop is not for model checking")
        }
        Ok(Self {
            op_iter: self.op_iter.clone(),
            clients: self
                .clients
                .keys()
                .copied()
                .map(|id| (id, Box::new(client()) as _))
                .collect(),
            latencies: None,
            invoke_instants: Default::default(),
            invocations: self.invocations.clone(),
            invoke_ops: self.invoke_ops.clone(),
            stop: None,
            done: self.done,
        })
    }
}

impl<I: Iterator<Item = Vec<u8>>> CloseLoop<I> {
    pub fn launch(&mut self) -> anyhow::Result<()> {
        for (client_id, sender) in &mut self.clients {
            let op = self
                .op_iter
                .next()
                .ok_or(anyhow::anyhow!("not enough op"))?;
            if self.invocations.is_some() {
                self.invoke_ops.insert(*client_id, op.clone());
            }
            if self.latencies.is_some() {
                self.invoke_instants.insert(*client_id, Instant::now());
            }
            sender.send(Invoke(op))?
        }
        Ok(())
    }
}

impl<I: Iterator<Item = Vec<u8>>> OnEvent<InvokeOk> for CloseLoop<I> {
    fn on_event(
        &mut self,
        (client_id, result): InvokeOk,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(sender) = self.clients.get_mut(&client_id) else {
            anyhow::bail!("unknown client id {client_id}")
        };
        if let Some(latencies) = &mut self.latencies {
            let replaced_instant = self
                .invoke_instants
                .insert(client_id, Instant::now())
                .ok_or(anyhow::anyhow!(
                    "missing invocation instant of client id {client_id}"
                ))?;
            latencies.push(replaced_instant.elapsed())
        }
        let op = self.op_iter.next();
        if let Some(invocations) = &mut self.invocations {
            let replaced_op = if let Some(op) = &op {
                self.invoke_ops.insert(client_id, op.clone())
            } else {
                self.invoke_ops.remove(&client_id)
            }
            .ok_or(anyhow::anyhow!(
                "missing invocation op of client id {client_id}"
            ))?;
            invocations.push((replaced_op, result))
        }
        if let Some(op) = op {
            sender.send(Invoke(op))
        } else {
            self.done = true;
            if let Some(stop) = self.stop.take() {
                stop()?
            }
            Ok(())
        }
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

pub mod check {
    use super::CloseLoop;

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct DryCloseLoop {
        // seems necessary to include `op_iter` as well
        // but clearly there's technical issue for doing that, and hopefully the workload will have
        // the same intent when all other states are the same
        invocations: Option<Vec<(Vec<u8>, Vec<u8>)>>,
        // necessary to include `invoke_ops`?
    }

    impl<I> From<CloseLoop<I>> for DryCloseLoop {
        fn from(value: CloseLoop<I>) -> Self {
            Self {
                invocations: value.invocations,
            }
        }
    }
}
