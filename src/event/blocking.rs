use std::{
    sync::mpsc::{Receiver, RecvTimeoutError},
    time::Instant,
};

use super::{ordered::Timer, OnEvent, OnTimer, SendEvent, TimerId};

pub type Sender<M> = std::sync::mpsc::Sender<M>;

impl<N: Into<M>, M> SendEvent<N> for Sender<M> {
    fn send(&mut self, event: N) -> anyhow::Result<()> {
        Sender::send(self, event.into()).map_err(|err| anyhow::anyhow!(err.to_string()))
    }
}

pub fn run<M>(
    receiver: Receiver<M>,
    state: &mut (impl OnEvent<M> + OnTimer),
) -> anyhow::Result<()> {
    run_internal(receiver, state, OnEvent::on_event, OnTimer::on_timer)
}

// there's no way to cancel a blocking run, so it should be fine to create timer in the function
// scope. not absolutely sure though
pub fn run_internal<S, M>(
    receiver: Receiver<M>,
    state: &mut S,
    mut on_event: impl FnMut(&mut S, M, &mut Timer) -> anyhow::Result<()>,
    mut on_timer: impl FnMut(&mut S, TimerId, &mut Timer) -> anyhow::Result<()>,
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
            on_event(state, event, &mut timer)?
        } else {
            on_timer(state, timer.advance()?, &mut timer)?
        }
    }
}
