use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use crate::{
    event::{SendEvent, TimerId},
    net::events::Send,
};

pub trait State {
    type Event;

    fn events(&self) -> Vec<Self::Event>;

    fn step(&mut self, event: Self::Event) -> anyhow::Result<()>;

    fn fix(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub struct Network<A, M, T> {
    pub messages: BTreeMap<A, Vec<M>>,
    pub timers: BTreeMap<A, Vec<(u32, Duration, T)>>, // TODO
    pub timer_count: u32,
    pub now: Duration,
}

#[derive(Debug)]
pub struct NetworkContext<'a, A, M, T> {
    state: &'a mut Network<A, M, T>,
    pub addr: A,
}

impl<A, M, T> Network<A, M, T> {
    pub fn context(&mut self, addr: A) -> NetworkContext<'_, A, M, T> {
        NetworkContext { state: self, addr }
    }
}

impl<A: Ord + Debug, M: Into<N>, N, T> SendEvent<Send<A, M>> for NetworkContext<'_, A, N, T> {
    fn send(&mut self, Send(remote, message): Send<A, M>) -> anyhow::Result<()> {
        let Some(inbox) = self.state.messages.get_mut(&remote) else {
            anyhow::bail!("missing inbox for addr {remote:?}")
        };
        inbox.push(message.into());
        Ok(())
    }
}

impl<A: Ord + Debug, M, T> NetworkContext<'_, A, M, T> {
    pub fn set(&mut self, period: Duration, event: T) -> anyhow::Result<TimerId> {
        let Some(inbox) = self.state.timers.get_mut(&self.addr) else {
            anyhow::bail!("missing inbox for addr {:?}", self.addr)
        };
        self.state.timer_count += 1;
        let id = self.state.timer_count;
        inbox.push((id, self.state.now + period, event));
        Ok(TimerId(id))
    }

    pub fn unset(&mut self, TimerId(id): TimerId) -> anyhow::Result<()> {
        let Some(inbox) = self.state.timers.get_mut(&self.addr) else {
            anyhow::bail!("missing inbox for addr {:?}", self.addr)
        };
        let Some(pos) = inbox.iter().position(|(other_id, _, _)| *other_id == id) else {
            anyhow::bail!("missing timer {:?}", TimerId(id))
        };
        inbox.remove(pos);
        Ok(())
    }
}
