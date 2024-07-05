use std::{marker::PhantomData, time::Duration};

use derive_where::derive_where;

use crate::event::{ScheduleEvent, TimerId};

#[derive_where(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Timer<T> {
    id: Option<TimerId>,
    period: Duration,
    _m: PhantomData<T>,
}

impl<T> Timer<T> {
    pub fn new(period: Duration) -> Self {
        Self {
            period,
            id: None,
            _m: PhantomData,
        }
    }

    pub fn set(
        &mut self,
        event: impl FnMut() -> T + Send + 'static,
        context: &mut impl ScheduleEvent<T>,
    ) -> anyhow::Result<()> {
        let replaced = self.id.replace(context.set(self.period, event)?);
        anyhow::ensure!(replaced.is_none());
        Ok(())
    }

    pub fn unset(&mut self, context: &mut impl ScheduleEvent<T>) -> anyhow::Result<()> {
        context.unset(
            self.id
                .take()
                .ok_or(anyhow::format_err!("missing timer id"))?,
        )
    }

    pub fn ensure_set(
        &mut self,
        event: impl FnMut() -> T + Send + 'static,
        context: &mut impl ScheduleEvent<T>,
    ) -> anyhow::Result<()> {
        if self.id.is_none() {
            self.set(event, context)?
        }
        Ok(())
    }

    pub fn ensure_unset(&mut self, context: &mut impl ScheduleEvent<T>) -> anyhow::Result<()> {
        if self.id.is_some() {
            self.unset(context)?
        }
        Ok(())
    }
}
