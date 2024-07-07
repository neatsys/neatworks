use std::{collections::HashMap, marker::PhantomData};

use derive_where::derive_where;
use tokio::{
    select, spawn,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::{AbortHandle, JoinSet},
    time::interval,
};

use super::{OnEvent, ScheduleEvent, SendEvent, TimerId, Work};

pub mod erase {
    use crate::event::{Erase, UntypedEvent};

    pub type Sender<S, C> = Erase<S, C, super::UnboundedSender<UntypedEvent<S, C>>>;

    pub type ScheduleState<S, C> = Erase<S, C, super::ScheduleState<UntypedEvent<S, C>>>;
}

// a "stub", or marker type that indicates the task based context is being used
// in a context type that refers to itself (on type level, not memory reference)
//
// the private PhantomData<()> prevents it from being constructed anywhere
// outside. and it indeed should not be ever constructed; it only shows up in
// type annotations, as a "placeholder" to take place for the actual generics
// that would refer the context's own type and cannot be written out directly
// anywhere outside the context definition
//
// the marker type seems to have no implementation. ideally it should have
// several implementation "blanket over" a generic state e.g. for schedule
//
// struct ContextOf<S>(PhantomData<S>) // the desired form of `Context`
//
// trait On<Context> {
//     type Out;
// }
//
// impl<State, Context> On<Context> for ContextOf<State>
// where
//     /* whatever bounds the state and context */
// {
//     type Out = erase::ScheduleState<State, Context>
// }
//
// this would not work (at least for now) because of the limitations by how
// compiler deals with `where` clauses involving types refer to each other (the
// details do not fit here but in short it will probably result in
// "error[E0275]: overflow evaluating the requirement"). the current workaround
// comes with three parts
// * move bounds in `where` clauses to associated types
// * `impl` on specialization instead of blanket to avoid explicitly writing out
//   the `where` clauses, but relying on the requirements trivially hold for the
//   specialized states and contexts
// * because the `On<_>::Out` has context-specified bounds (for the first
//   point), and one context type can have at most one `On<Self>` bound (or the
//   `impl`s of those bounds will need to `where` on each other again), invent
//   dedicated `On<_>` trait for every context that produces all types that
//   refer back to the context type, instead of one universal trait for
//   schedule, one for worker upcall, etc
//
// as the result, the `impl`s of `ContextOf<_>` all lives in the use sites and
// are for some specialization. but in sprite those `impl`s are together
// recovering the necessary part of the blanket above
//
// since the `impl`s are becoming specialized, it is unnecessary for this marker
// to be generic over state (or anything). the (becoming unnecessary)
// PhantomData<_> is saved to continue preventing `Context` value being
// constructed, which is still desired despiting the workaround
pub struct Context(PhantomData<()>);

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
    receiver: &mut UnboundedReceiver<Work<S, C>>,
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
            Select::Recv(event) => {
                let mut state = state.clone();
                let mut context = context.clone();
                tasks.spawn(async move { event(&mut state, &mut context) });
            }
            Select::JoinNext(()) => {}
        }
    }
}
