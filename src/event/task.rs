use std::collections::HashMap;

use tokio::{
    select, spawn,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::AbortHandle,
    time::interval,
};

use super::{OnEvent, ScheduleEvent, SendEvent, TimerId};

impl<M: Into<N>, N> SendEvent<M> for UnboundedSender<N> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        UnboundedSender::send(self, event.into()).map_err(|_| anyhow::format_err!("send error"))
    }
}

async fn must_recv<M>(receiver: &mut UnboundedReceiver<M>) -> anyhow::Result<M> {
    receiver
        .recv()
        .await
        .ok_or(anyhow::format_err!("unexpected receive channel closed"))
}

pub async fn run<M, C>(
    receiver: &mut UnboundedReceiver<M>,
    state: &mut impl OnEvent<C, Event = M>,
    context: &mut C,
) -> anyhow::Result<()> {
    loop {
        let event = must_recv(receiver).await?;
        state.on_event(event, context)?
    }
}

pub struct ScheduleState<M> {
    count: u32,
    events: HashMap<u32, (AbortHandle, M)>,
    sender: UnboundedSender<u32>,
}

impl<M> ScheduleEvent<M> for ScheduleState<M> {
    fn set(&mut self, period: std::time::Duration, event: M) -> anyhow::Result<super::TimerId> {
        self.count += 1;
        let id = self.count;
        let sender = self.sender.clone();
        let handle = spawn(async move {
            let mut delay = interval(period);
            delay.tick().await;
            loop {
                delay.tick().await;
                if sender.send(id).is_err() {
                    // log
                    return;
                }
            }
        })
        .abort_handle();
        self.events.insert(id, (handle, event));
        Ok(TimerId(id))
    }

    fn unset(&mut self, TimerId(id): TimerId) -> anyhow::Result<()> {
        let Some((handle, _)) = self.events.remove(&id) else {
            anyhow::bail!("missing event for {:?}", TimerId(id))
        };
        handle.abort();
        Ok(())
    }
}

pub async fn run_schedule<M: Clone, C: AsMut<ScheduleState<M>>>(
    receiver: &mut UnboundedReceiver<M>,
    schedule_receiver: &mut UnboundedReceiver<u32>,
    state: &mut impl OnEvent<C, Event = M>,
    context: &mut C,
) -> anyhow::Result<()> {
    loop {
        enum Select<M> {
            Recv(M),
            ScheduleRecv(u32),
        }
        match select! {
            recv = must_recv(receiver) => Select::Recv(recv?),
            recv = must_recv(schedule_receiver) => Select::ScheduleRecv(recv?),
        } {
            Select::Recv(event) => state.on_event(event, context)?,
            Select::ScheduleRecv(id) => {
                let Some((_, event)) = context.as_mut().events.get(&id) else {
                    continue;
                };
                state.on_event(event.clone(), context)?
            }
        }
    }
}
