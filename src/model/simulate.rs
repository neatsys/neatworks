use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use derive_more::{Display, Error};
use derive_where::derive_where;

use crate::{
    event::{ActiveTimer, ScheduleEvent, SendEvent},
    net::events::Cast,
};

#[derive(Debug, Display, Error)]
pub struct ProgressExhausted;

pub trait State {
    type Event;

    fn step(&mut self, temporal: &mut Temporal<Self::Event>) -> anyhow::Result<()>;

    fn progress(&mut self, temporal: &mut Temporal<Self::Event>) -> anyhow::Result<()> {
        loop {
            match self.step(temporal) {
                Err(err) if err.is::<ProgressExhausted>() => return Ok(()),
                result => result?,
            }
        }
    }
}

pub type TimerId = u32;

#[derive_where(Default)]
pub struct Temporal<M> {
    now: Duration,
    count: TimerId,
    timeline: BTreeSet<(Duration, TimerId)>,
    timers: HashMap<TimerId, TimerEnvelop<M>>,
}

struct TimerEnvelop<M> {
    event: M,
    period: Duration,
    at: Duration,
}

impl<M> ScheduleEvent<M> for Temporal<M> {
    fn set(&mut self, period: Duration, event: M) -> anyhow::Result<ActiveTimer>
    where
        M: Send + Clone + 'static,
    {
        self.count += 1;
        let id = self.count;
        let at = self.now + period;
        let envelop = TimerEnvelop { event, period, at };
        let replaced = self.timers.insert(id, envelop);
        assert!(replaced.is_none());
        let inserted = self.timeline.insert((at, id));
        assert!(inserted);
        Ok(ActiveTimer(id))
    }

    fn set_internal(
        &mut self,
        _: Duration,
        _: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<ActiveTimer> {
        anyhow::bail!("unimplemented")
    }

    fn unset(&mut self, ActiveTimer(id): ActiveTimer) -> anyhow::Result<()> {
        let Some(envelop) = self.timers.remove(&id) else {
            anyhow::bail!("missing timer envelop")
        };
        let removed = self.timeline.remove(&(envelop.at, id));
        assert!(removed);
        Ok(())
    }
}

impl<M> Temporal<M> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pop(&mut self) -> anyhow::Result<M>
    where
        M: Clone,
    {
        let Some((at, id)) = self.timeline.pop_first() else {
            anyhow::bail!(ProgressExhausted)
        };
        self.now = at;
        let Some(envelop) = self.timers.get_mut(&id) else {
            unreachable!()
        };
        envelop.at = self.now + envelop.period;
        let event = envelop.event.clone();
        let inserted = self.timeline.insert((envelop.at, id));
        assert!(inserted);
        Ok(event)
    }
}

#[derive(Debug)]
#[derive_where(Default)]
pub struct NetworkState<A, M> {
    messages: Vec<(A, M)>,
}

impl<A, M> NetworkState<A, M> {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn choose(&mut self, u: &mut arbtest::arbitrary::Unstructured) -> anyhow::Result<(A, M)> {
        anyhow::ensure!(!self.messages.is_empty(), ProgressExhausted);
        let i = u.choose_index(self.messages.len()).unwrap();
        Ok(self.messages.swap_remove(i))
    }
}

impl<A, M: Into<N>, N> SendEvent<Cast<A, M>> for NetworkState<A, N> {
    fn send(&mut self, Cast(remote, message): Cast<A, M>) -> anyhow::Result<()> {
        self.messages.push((remote, message.into()));
        Ok(())
    }
}
