use std::{
    sync::mpsc::{Receiver, RecvTimeoutError},
    time::Instant,
};

use super::{ordered::Timer, OnEventUniversal, OnTimerUniversal, SendEvent};

pub type Sender<M> = std::sync::mpsc::Sender<M>;

impl<N: Into<M>, M> SendEvent<N> for Sender<M> {
    fn send(&mut self, event: N) -> anyhow::Result<()> {
        Sender::send(self, event.into()).map_err(|err| anyhow::anyhow!(err.to_string()))
    }
}

// there's no way to cancel a blocking run, so it should be fine to create timer in the function
// scope. not absolutely sure though
pub fn run<M>(
    receiver: Receiver<M>,
    state: &mut (impl OnEventUniversal<Timer, Event = M> + OnTimerUniversal<Timer>),
) -> anyhow::Result<()> {
    let mut timer = Timer::new();
    loop {
        let event = if let Some(deadline) = timer.deadline() {
            match receiver.recv_timeout(deadline.duration_since(Instant::now())) {
                Ok(event) => Some(event),
                Err(RecvTimeoutError::Timeout) => None,
                Err(err) => anyhow::bail!(err),
            }
        } else {
            Some(receiver.recv().map_err(|err| anyhow::anyhow!(err))?)
        };
        if let Some(event) = event {
            state.on_event(event, &mut timer)?
        } else {
            state.on_timer(timer.advance()?, &mut timer)?
        }
    }
}
