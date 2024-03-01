use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};

use super::TimerId;

#[derive(Debug, Default)]
pub struct Timer {
    id: u32,
    deadlines: BTreeSet<(Instant, u32)>,
    states: HashMap<u32, TimerState>,
}

#[derive(Debug)]
struct TimerState {
    period: Duration,
    deadline: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl crate::event::Timer for Timer {
    fn set(&mut self, period: Duration) -> anyhow::Result<TimerId> {
        self.id += 1;
        let id = self.id;
        let deadline = Instant::now() + period;
        self.states.insert(id, TimerState { period, deadline });
        self.deadlines.insert((deadline, id));
        Ok(TimerId(id))
    }

    fn unset(&mut self, TimerId(id): TimerId) -> anyhow::Result<()> {
        let state = self
            .states
            .remove(&id)
            .ok_or(anyhow::anyhow!("missing timer state"))?;
        let removed = self.deadlines.remove(&(state.deadline, id));
        if removed {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "inconsistency between states and deadlines"
            ))
        }
    }
}

impl Timer {
    pub fn deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }

    pub fn on_deadline(&mut self) -> anyhow::Result<TimerId> {
        let (_, id) = self
            .deadlines
            .pop_first()
            .ok_or(anyhow::anyhow!("empty deadlines"))?;
        let state = self.states.get_mut(&id).ok_or(anyhow::anyhow!(
            "inconsistency between states and deadlines"
        ))?;
        state.deadline += state.period;
        self.deadlines.insert((state.deadline, id));
        Ok(TimerId(id))
    }
}
