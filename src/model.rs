use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    iter::repeat,
    time::Duration,
};

use derive_where::derive_where;

use crate::{
    event::{ScheduleEvent, SendEvent, TimerId},
    net::events::Cast,
};

pub mod search;

pub trait State: SendEvent<Self::Event> {
    type Event;

    fn events(&self) -> impl Iterator<Item = Self::Event> + '_;
}

// the alternative `State` interface
//   trait State = OnEvent<C> where C: Context<Self::Event>
pub trait Context<M> {
    fn register(&mut self, event: M) -> anyhow::Result<()>;
}
// the custom `events` method can be removed then, results in more compact
// interface
//
// the downside of this alternation
// * bootstrapping. the every first event(s) that applied to the initial state
//   is hard to be provided
// * it doesn't fit the current searching workflows. it may be possible to
//   adjust the workflows to maintain a buffer of not yet applied events, but
//   in my opinion that complicates things

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive_where(Default)]
pub struct ScheduleState<M> {
    envelops: Vec<TimerEnvelop<M>>,
    count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TimerEnvelop<M> {
    id: u32,
    period: Duration,
    event: M,
}

impl<M> ScheduleState<M> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<M: Into<N>, N> ScheduleEvent<M> for ScheduleState<N> {
    fn set(&mut self, period: Duration, event: M) -> anyhow::Result<TimerId> {
        self.count += 1;
        let id = self.count;
        let envelop = TimerEnvelop {
            id,
            event: event.into(),
            period,
        };
        self.envelops.push(envelop);
        Ok(TimerId(id))
    }

    fn set_internal(
        &mut self,
        _: Duration,
        _: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<TimerId> {
        anyhow::bail!("unimplemented")
    }

    fn unset(&mut self, TimerId(id): TimerId) -> anyhow::Result<()> {
        self.remove(id)?;
        Ok(())
    }
}

impl<M> ScheduleState<M> {
    fn remove(&mut self, id: u32) -> anyhow::Result<TimerEnvelop<M>> {
        let Some(pos) = self.envelops.iter().position(|envelop| envelop.id == id) else {
            anyhow::bail!("missing timer of {:?}", TimerId(id))
        };
        Ok(self.envelops.remove(pos))
    }

    pub fn tick(&mut self, TimerId(id): TimerId) -> anyhow::Result<()> {
        let ticked = self.remove(id)?;
        self.envelops.push(ticked);
        Ok(())
    }
}

impl<M: Clone> ScheduleState<M> {
    pub fn events(&self) -> impl Iterator<Item = (TimerId, M)> + '_ {
        let mut limit = Duration::MAX;
        self.envelops.iter().map_while(move |envelop| {
            if envelop.period >= limit {
                return None;
            }
            limit = envelop.period;
            Some((TimerId(envelop.id), envelop.event.clone()))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive_where(Default)]
pub struct NetworkState<A, M> {
    messages: BTreeMap<A, BTreeSet<M>>,
}

impl<A, M> NetworkState<A, M> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<A: Ord + Debug, M: Into<N>, N: Ord> SendEvent<Cast<A, M>> for NetworkState<A, N> {
    fn send(&mut self, Cast(remote, message): Cast<A, M>) -> anyhow::Result<()> {
        let Some(inbox) = self.messages.get_mut(&remote) else {
            anyhow::bail!("missing inbox for addr {remote:?}")
        };
        inbox.insert(message.into());
        Ok(())
    }
}

impl<A: Ord, M> NetworkState<A, M> {
    pub fn register(&mut self, addr: A) -> anyhow::Result<()> {
        let replaced = self.messages.insert(addr, Default::default());
        anyhow::ensure!(replaced.is_none());
        Ok(())
    }
}

impl<A: Clone, M: Clone> NetworkState<A, M> {
    pub fn events(&self) -> impl Iterator<Item = (A, M)> + '_ {
        self.messages
            .iter()
            .flat_map(|(addr, inbox)| repeat(addr.clone()).zip(inbox.iter().cloned()))
    }
}
