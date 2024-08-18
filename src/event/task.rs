use std::collections::HashMap;

use derive_where::derive_where;
use tokio::{
    select, spawn,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::{AbortHandle, JoinSet},
    time::interval,
};

use super::{ActiveTimer, OnEvent, ScheduleEvent, SendEvent, UntypedEvent};

pub mod erase {
    use crate::event::{Erase, UntypedEvent};

    pub type Sender<S, C> = Erase<S, C, super::UnboundedSender<UntypedEvent<S, C>>>;

    pub type ScheduleState<S, C> = Erase<S, C, super::ScheduleState<UntypedEvent<S, C>>>;
}

impl<M: Into<N>, N> SendEvent<M> for UnboundedSender<N> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        UnboundedSender::send(self, event.into())
            .map_err(|_| anyhow::format_err!("unexpected send channel closed"))
    }
}

pub mod work {
    use crate::event::{SendEvent, Submit, UntypedEvent, Work};

    pub type Sender<S, C> = super::UnboundedSender<UntypedEvent<S, C>>;

    impl<S, C> Submit<S, C> for Sender<S, C> {
        fn submit(&mut self, work: Work<S, C>) -> anyhow::Result<()> {
            SendEvent::send(self, UntypedEvent(work))
        }
    }
}

async fn must_recv<M>(receiver: &mut UnboundedReceiver<M>) -> anyhow::Result<M> {
    receiver
        .recv()
        .await
        .ok_or(anyhow::format_err!("unexpected receive channel closed"))
}

#[derive_where(Debug)]
pub struct ScheduleState<M> {
    count: u32,
    #[derive_where(skip)]
    events: HashMap<u32, ScheduleEventState<M>>,
    sender: UnboundedSender<u32>,
}

type ScheduleEventState<M> = (AbortHandle, Box<dyn FnMut() -> M + Send>);

impl<M> ScheduleState<M> {
    pub fn new(sender: UnboundedSender<u32>) -> Self {
        Self {
            sender,
            count: 0,
            events: Default::default(),
        }
    }
}

impl<M: Into<N> + Send + 'static, N> ScheduleEvent<M> for ScheduleState<N> {
    fn set_internal(
        &mut self,
        period: std::time::Duration,
        mut event: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<ActiveTimer> {
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
        self.events
            .insert(id, (handle, Box::new(move || event().into())));
        Ok(ActiveTimer(id))
    }

    fn unset(&mut self, ActiveTimer(id): ActiveTimer) -> anyhow::Result<()> {
        let Some((handle, _)) = self.events.remove(&id) else {
            anyhow::bail!("missing event for {:?}", ActiveTimer(id))
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

pub async fn run<M, C>(
    state: impl OnEvent<C, Event = M>,
    context: &mut C,
    receiver: &mut UnboundedReceiver<M>,
) -> anyhow::Result<()> {
    let (_sender, mut schedule_receiver) = unbounded_channel();
    run_with_schedule(
        state,
        context,
        receiver,
        &mut schedule_receiver,
        |_| unreachable!(),
    )
    .await
}

pub async fn run_worker<S: Clone + Send + 'static, C: Clone + Send + 'static>(
    state: S,
    context: C,
    receiver: &mut UnboundedReceiver<UntypedEvent<S, C>>,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    loop {
        enum Select<M> {
            Recv(M),
            JoinNext(()),
        }
        match select! {
            recv = must_recv(receiver) => Select::Recv(recv?),
            Some(result) = tasks.join_next() => Select::JoinNext(result??)
        } {
            Select::Recv(UntypedEvent(event)) => {
                let mut state = state.clone();
                let mut context = context.clone();
                tasks.spawn(async move { event(&mut state, &mut context) });
            }
            Select::JoinNext(()) => {}
        }
    }
}
