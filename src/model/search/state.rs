use std::{collections::BTreeSet, fmt::Debug, time::Duration};

use derive_where::derive_where;

use crate::{
    event::{ActiveTimer, ScheduleEvent, SendEvent},
    net::events::Cast,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive_where(Default)]
pub struct Schedule<M> {
    envelops: Vec<TimerEnvelop<M>>,
    count: TimerId,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TimerEnvelop<M> {
    id: TimerId,
    period: Duration,
    event: M,
}

pub type TimerId = u32;

impl<M> Schedule<M> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<M: Into<N>, N> ScheduleEvent<M> for Schedule<N> {
    fn set(&mut self, period: Duration, event: M) -> anyhow::Result<ActiveTimer> {
        self.count += 1;
        let id = self.count;
        let envelop = TimerEnvelop {
            id,
            event: event.into(),
            period,
        };
        self.envelops.push(envelop);
        Ok(ActiveTimer(id))
    }

    fn unset(&mut self, ActiveTimer(id): ActiveTimer) -> anyhow::Result<()> {
        self.remove(id)?;
        Ok(())
    }
}

impl<M> Schedule<M> {
    fn remove(&mut self, id: u32) -> anyhow::Result<TimerEnvelop<M>> {
        let Some(pos) = self.envelops.iter().position(|envelop| envelop.id == id) else {
            anyhow::bail!("missing timer of {:?}", ActiveTimer(id))
        };
        Ok(self.envelops.remove(pos))
    }

    pub fn tick(&mut self, id: TimerId) -> anyhow::Result<()> {
        let ticked = self.remove(id)?;
        self.envelops.push(ticked);
        Ok(())
    }
}

impl<M: Clone> Schedule<M> {
    pub fn events(&self) -> impl Iterator<Item = (TimerId, M)> + '_ {
        let mut limit = Duration::MAX;
        self.envelops.iter().map_while(move |envelop| {
            if envelop.period >= limit {
                return None;
            }
            limit = envelop.period;
            Some((envelop.id, envelop.event.clone()))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive_where(Default)]
pub struct Network<A, M> {
    messages: BTreeSet<(A, M)>,
}

impl<A, M> Network<A, M> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<A: Ord + Debug, M: Into<N>, N: Ord> SendEvent<Cast<A, M>> for Network<A, N> {
    fn send(&mut self, Cast(remote, message): Cast<A, M>) -> anyhow::Result<()> {
        self.messages.insert((remote, message.into()));
        Ok(())
    }
}

impl<A: Clone, M: Clone> Network<A, M> {
    pub fn events(&self) -> impl Iterator<Item = (A, M)> + '_ {
        self.messages.iter().cloned()
    }
}
