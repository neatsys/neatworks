use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use derive_where::derive_where;

use crate::{
    event::{ScheduleEvent, SendEvent, TimerId},
    net::events::Cast,
};

pub mod search;

pub trait State: SendEvent<Self::Event> {
    type Event;

    fn events(&self) -> Vec<Self::Event>;

    fn fix(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

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
        let Some(pos) = self.envelops.iter().position(|envelop| envelop.id == id) else {
            anyhow::bail!("missing timer of {:?}", TimerId(id))
        };
        self.envelops.remove(pos);
        Ok(())
    }
}

impl<M: Clone> ScheduleState<M> {
    pub fn generate_events(&self, mut on_event: impl FnMut(M)) {
        let mut limit = Duration::MAX;
        for envelop in &self.envelops {
            if envelop.period >= limit {
                break;
            }
            on_event(envelop.event.clone());
            limit = envelop.period;
        }
    }

    pub fn tick(&mut self, event: M) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive_where(Default)]
pub struct NetworkState<A, M> {
    messages: BTreeMap<A, Vec<M>>,
}

impl<A, M> NetworkState<A, M> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<A: Ord + Debug, M: Into<N>, N> SendEvent<Cast<A, M>> for NetworkState<A, N> {
    fn send(&mut self, Cast(remote, message): Cast<A, M>) -> anyhow::Result<()> {
        let Some(inbox) = self.messages.get_mut(&remote) else {
            anyhow::bail!("missing inbox for addr {remote:?}")
        };
        inbox.push(message.into());
        Ok(())
    }
}

impl<A: Clone, M: Clone> NetworkState<A, M> {
    pub fn generate_events(&self, mut on_event: impl FnMut(A, M)) {
        for (addr, inbox) in &self.messages {
            for message in inbox {
                on_event(addr.clone(), message.clone())
            }
        }
    }
}
