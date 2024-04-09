use std::{collections::BTreeSet, hash::Hash, time::Duration};

use super::TimerId;

#[derive(Debug, Clone, Default)]
pub struct Timer {
    events: Vec<(u32, Duration)>,
    timer_id: u32,
    pub elapsed: Duration,
}

// Eq and Hash implements are delegated to `Timer::events`, that is, two timers
// are considered equal as long as same set of events are *observed* from
// outside
// not sure how sound this definition is. not very good for performance for sure

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.events().collect::<BTreeSet<_>>() == other.events().collect()
    }
}
impl Eq for Timer {}

impl Hash for Timer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.events().collect::<BTreeSet<_>>().hash(state)
    }
}

impl Timer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> impl Iterator<Item = TimerId> + '_ {
        let mut prev_period = None;
        self.events
            .iter()
            .take_while(move |(_, period)| {
                if let Some(prev_period) = prev_period.replace(*period) {
                    *period < prev_period
                } else {
                    true
                }
            })
            .map(|(id, _)| TimerId(*id))
    }

    fn unset(&mut self, TimerId(id): &TimerId) -> anyhow::Result<(u32, Duration)> {
        let i = self
            .events
            .iter()
            .position(|(timer_id, _)| timer_id == id)
            .ok_or(anyhow::anyhow!("timer not found"))?;
        Ok(self.events.remove(i))
    }

    pub fn step_timer(&mut self, timer_id: &TimerId) -> anyhow::Result<()> {
        let (timer_id, period) = self.unset(timer_id)?;
        self.elapsed += period;
        self.events.push((timer_id, period));
        Ok(())
    }
}

impl crate::event::Timer for Timer {
    fn set(&mut self, period: Duration) -> anyhow::Result<TimerId> {
        self.timer_id += 1;
        let timer_id = self.timer_id;
        self.events.push((timer_id, period));
        Ok(TimerId(timer_id))
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        Timer::unset(self, &timer_id)?;
        Ok(())
    }
}
