use std::time::Duration;

use super::TimerId;

#[derive(Clone, Default)]
pub struct Timer {
    events: Vec<(u32, Duration)>,
    timer_id: u32,
}

impl Timer {
    pub fn events(&self) -> Vec<TimerId> {
        let mut events = Vec::new();
        let mut prev_period = None;
        for &(id, period) in &self.events {
            if let Some(prev_period) = prev_period {
                if period >= prev_period {
                    break;
                }
            }
            events.push(TimerId(id));
            prev_period = Some(period)
        }
        events
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
        let event = self.unset(timer_id)?;
        self.events.push(event);
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
