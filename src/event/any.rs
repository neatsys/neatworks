use std::collections::BTreeMap;

use derive_where::derive_where;

use super::TimerId;

#[derive(derive_more::Deref, derive_more::DerefMut)]
#[derive_where(Debug, Clone, PartialEq, Eq, Hash; T, D)]
#[derive_where(Default; T)]
pub struct Timer<T, F, D> {
    #[deref]
    #[deref_mut]
    inner: T,
    attach: BTreeMap<u32, D>,
    #[derive_where(skip)]
    _m: std::marker::PhantomData<F>,
}

pub trait TryIntoTimerData<D> {
    fn try_into<M: 'static>(event: M) -> anyhow::Result<D>;
}

impl<T: super::Timer, F: TryIntoTimerData<D>, D, S> super::erased::RichTimer<S> for Timer<T, F, D> {
    fn set<M: Clone + Send + Sync + 'static>(
        &mut self,
        period: std::time::Duration,
        event: M,
    ) -> anyhow::Result<super::TimerId>
    where
        S: super::erased::OnEventRichTimer<M>,
    {
        let data = F::try_into(event)?;
        let TimerId(timer_id) = self.inner.set(period)?;
        self.attach.insert(timer_id, data);
        Ok(TimerId(timer_id))
    }

    fn unset(&mut self, TimerId(timer_id): TimerId) -> anyhow::Result<()> {
        self.attach.remove(&timer_id);
        self.inner.unset(TimerId(timer_id))
    }
}

impl<T, F, D> Timer<T, F, D> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            attach: Default::default(),
            _m: Default::default(),
        }
    }

    pub fn get_timer_data(&self, TimerId(timer_id): &TimerId) -> anyhow::Result<&D> {
        self.attach
            .get(timer_id)
            .ok_or(anyhow::anyhow!("missing timer attachment"))
    }
}
