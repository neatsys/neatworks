use std::collections::HashMap;

use derive_where::derive_where;
use tokio::{
    select, spawn,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::AbortHandle,
    time::interval,
};

use super::{OnEvent, ScheduleEvent, SendEvent, TimerId};

impl<M: Into<N>, N> SendEvent<M> for UnboundedSender<N> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        UnboundedSender::send(self, event.into())
            .map_err(|_| anyhow::format_err!("unexpected send channel closed"))
    }
}

async fn must_recv<M>(receiver: &mut UnboundedReceiver<M>) -> anyhow::Result<M> {
    receiver
        .recv()
        .await
        .ok_or(anyhow::format_err!("unexpected receive channel closed"))
}

pub async fn run<M, C>(
    state: impl OnEvent<C, Event = M>,
    context: &mut C,
    receiver: &mut UnboundedReceiver<M>,
) -> anyhow::Result<()> {
    let (_sender, mut schedule_receiver) = unbounded_channel();
    run_with_schedule(state, context, receiver, &mut schedule_receiver, |_| unreachable!()).await
}

#[derive_where(Debug)]
pub struct ScheduleState<M> {
    count: u32,
    #[derive_where(skip)]
    events: HashMap<u32, (AbortHandle, Box<dyn FnMut() -> M + Send>)>,
    sender: UnboundedSender<u32>,
}

impl<M> ScheduleState<M> {
    pub fn new(sender: UnboundedSender<u32>) -> Self {
        Self {
            sender,
            count: 0,
            events: Default::default(),
        }
    }
}

impl<M> ScheduleEvent<M> for ScheduleState<M> {
    fn set(
        &mut self,
        period: std::time::Duration,
        event: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<super::TimerId> {
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
        self.events.insert(id, (handle, Box::new(event)));
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

pub async fn run_with_schedule<M, C>(
    mut state: impl OnEvent<C, Event = M>,
    context: &mut C,
    receiver: &mut UnboundedReceiver<M>,
    schedule_receiver: &mut UnboundedReceiver<u32>,
    schedule_mut: impl Fn(&mut C) -> &mut ScheduleState<M>,
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
                let Some((_, event)) = schedule_mut(context).events.get_mut(&id) else {
                    continue;
                };
                state.on_event(event(), context)?
            }
        }
    }
}
