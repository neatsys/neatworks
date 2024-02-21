use std::{
    collections::HashMap,
    fmt::{Debug, Display},
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
    workload_iter: I,
    pub latencies: Option<Vec<Duration>>,
    invoke_instants: HashMap<u32, Instant>,
    pub invocations: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    invoke_workloads: HashMap<u32, (Vec<u8>, Option<Vec<u8>>)>,
    pub stop: Option<CloseLoopStop>,
    pub done: bool,
}

type CloseLoopStop = Box<dyn FnOnce() -> anyhow::Result<()> + Send + Sync>;

impl<I> Debug for CloseLoop<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloseLoop").finish_non_exhaustive()
    }
}

pub type Workload = (Vec<u8>, Option<Vec<u8>>);

impl<I> CloseLoop<I> {
    pub fn new(workload_iter: I) -> Self {
        Self {
            workload_iter,
            clients: Default::default(),
            latencies: Default::default(),
            invoke_instants: Default::default(),
            invocations: Default::default(),
            invoke_workloads: Default::default(),
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
            workload_iter: self.workload_iter.clone(),
            clients: self
                .clients
                .keys()
                .copied()
                .map(|id| (id, Box::new(client()) as _))
                .collect(),
            latencies: None,
            invoke_instants: Default::default(),
            invocations: self.invocations.clone(),
            invoke_workloads: self.invoke_workloads.clone(),
            stop: None,
            done: self.done,
        })
    }
}

impl<I: Iterator<Item = Workload>> CloseLoop<I> {
    pub fn launch(&mut self) -> anyhow::Result<()> {
        for (client_id, sender) in &mut self.clients {
            let (op, expected_result) = self
                .workload_iter
                .next()
                .ok_or(anyhow::anyhow!("not enough op"))?;
            self.invoke_workloads
                .insert(*client_id, (op.clone(), expected_result));
            if self.latencies.is_some() {
                self.invoke_instants.insert(*client_id, Instant::now());
            }
            sender.send(Invoke(op))?
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct UnexpectedResult {
    pub expect: Vec<u8>,
    pub actual: Vec<u8>,
}

impl Display for UnexpectedResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for UnexpectedResult {}

impl<I: Iterator<Item = Workload>> OnEvent<InvokeOk> for CloseLoop<I> {
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
        let workload = self.workload_iter.next();
        let (replaced_op, replaced_expected_result) = if let Some(workload) = &workload {
            self.invoke_workloads.insert(client_id, workload.clone())
        } else {
            self.invoke_workloads.remove(&client_id)
        }
        .ok_or(anyhow::anyhow!(
            "missing invocation record of client id {client_id}"
        ))?;
        if let Some(expected_result) = replaced_expected_result {
            if result != expected_result {
                Err(UnexpectedResult {
                    expect: expected_result,
                    actual: result.clone(),
                })?
            }
        }
        if let Some(invocations) = &mut self.invocations {
            invocations.push((replaced_op, result))
        }
        if let Some((op, _)) = workload {
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
