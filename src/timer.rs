use std::{marker::PhantomData, time::Duration};

use derive_where::derive_where;

use crate::event::{OnErasedEvent, ScheduleEventFor, TimerId};

#[derive_where(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Timer<M> {
    id: Option<TimerId>,
    period: Duration,
    _m: PhantomData<M>,
}

impl<M> Timer<M> {
    pub fn new(period: Duration) -> Self {
        Self {
            period,
            id: None,
            _m: PhantomData,
        }
    }
}

// TODO support `ScheduleEvent`
impl<M: Send + 'static> Timer<M> {
    pub fn set<S: OnErasedEvent<M, C>, C>(
        &mut self,
        event: impl FnMut() -> M + Send + 'static,
        context: &mut impl ScheduleEventFor<S, C>,
    ) -> anyhow::Result<()> {
        let replaced = self.id.replace(context.set(self.period, event)?);
        anyhow::ensure!(replaced.is_none());
        Ok(())
    }

    pub fn unset<S, C>(&mut self, context: &mut impl ScheduleEventFor<S, C>) -> anyhow::Result<()> {
        context.unset(
            self.id
                .take()
                .ok_or(anyhow::format_err!("missing timer id"))?,
        )
    }

    pub fn ensure_set<S: OnErasedEvent<M, C>, C>(
        &mut self,
        event: impl FnMut() -> M + Send + 'static,
        context: &mut impl ScheduleEventFor<S, C>,
    ) -> anyhow::Result<()> {
        if self.id.is_none() {
            self.set(event, context)?
        }
        Ok(())
    }

    pub fn ensure_unset<S, C>(
        &mut self,
        context: &mut impl ScheduleEventFor<S, C>,
    ) -> anyhow::Result<()> {
        if self.id.is_some() {
            self.unset(context)?
        }
        Ok(())
    }
}
