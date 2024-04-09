use std::collections::BTreeMap;

use derive_where::derive_where;

use super::TimerId;

#[derive(derive_more::Deref, derive_more::DerefMut, Debug, Clone, PartialEq, Eq, Hash)]
#[derive_where(Default; T)]
pub struct Timer<T, D> {
    #[deref]
    #[deref_mut]
    inner: T,
    attach: BTreeMap<u32, D>,
}

pub trait DowncastEvent {
    fn try_from<M: 'static>(event: M) -> anyhow::Result<Self>
    where
        Self: Sized;
}

impl<T: super::Timer, D: DowncastEvent, S> super::erased::RichTimer<S> for Timer<T, D> {
    fn set<M: Clone + Send + Sync + 'static>(
        &mut self,
        period: std::time::Duration,
        event: M,
    ) -> anyhow::Result<super::TimerId>
    where
        S: super::erased::OnEventRichTimer<M>,
    {
        let data = D::try_from(event)?;
        let TimerId(timer_id) = self.inner.set(period)?;
        self.attach.insert(timer_id, data);
        Ok(TimerId(timer_id))
    }

    fn unset(&mut self, TimerId(timer_id): TimerId) -> anyhow::Result<()> {
        self.attach.remove(&timer_id);
        self.inner.unset(TimerId(timer_id))
    }
}

impl<T, D> Timer<T, D> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            attach: Default::default(),
        }
    }

    pub fn get_timer_data(&self, TimerId(timer_id): &TimerId) -> anyhow::Result<&D> {
        self.attach
            .get(timer_id)
            .ok_or(anyhow::anyhow!("missing timer attachment"))
    }
}
