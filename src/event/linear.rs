use std::time::Duration;

use super::TimerId;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Timer {
    events: Vec<TimerEvent>,
    timer_id: u32,
    now: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerEvent {
    id: u32,
    pub period: Duration,
    // the time of *last* triggering, the incoming triggering is `at()`
    // always zero during model checking
    when: Duration, // `deadline` sounds like a one time thing
}

impl TimerEvent {
    pub fn id(&self) -> TimerId {
        TimerId(self.id)
    }

    pub fn at(&self) -> Duration {
        self.when + self.period
    }
}

impl Timer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> impl Iterator<Item = TimerEvent> + '_ {
        let mut prev_period = None;
        self.events.iter().map_while(move |event| {
            if let Some(prev_period) = prev_period.replace(event.period) {
                if event.period >= prev_period {
                    return None;
                }
            }
            Some(*event)
        })
    }

    pub fn advance_to(&mut self, now: Duration) -> anyhow::Result<()> {
        anyhow::ensure!(now >= self.now);
        self.now = now;
        anyhow::ensure!(self.events.iter().all(|event| event.at() >= self.now));
        Ok(())
    }

    fn unset(&mut self, TimerId(id): &TimerId) -> anyhow::Result<TimerEvent> {
        let i = self
            .events
            .iter()
            .position(|event| event.id == *id)
            .ok_or(anyhow::anyhow!("timer not found"))?;
        Ok(self.events.remove(i))
    }

    pub fn step_timer(&mut self, timer_id: &TimerId) -> anyhow::Result<()> {
        let mut event = self.unset(timer_id)?;
        event.when = self.now;
        self.events.push(event);
        Ok(())
    }
}

impl crate::event::Timer for Timer {
    fn set(&mut self, period: Duration) -> anyhow::Result<TimerId> {
        self.timer_id += 1;
        let timer_id = self.timer_id;
        let event = TimerEvent {
            id: timer_id,
            period,
            when: self.now,
        };
        self.events.push(event);
        Ok(TimerId(timer_id))
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        Timer::unset(self, &timer_id)?;
        Ok(())
    }
}

// cSpell:words Hasher
