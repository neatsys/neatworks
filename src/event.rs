use std::{marker::PhantomData, time::Duration};

pub mod semantic;
pub mod task;

pub trait SendEvent<M> {
    fn send(&mut self, event: M) -> anyhow::Result<()>;
}

impl<E: SendEvent<M>, M> SendEvent<M> for &mut E {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        E::send(self, event)
    }
}

pub trait OnEvent<C> {
    type Event;

    fn on_event(&mut self, event: Self::Event, context: &mut C) -> anyhow::Result<()>;
}

impl<S: OnEvent<C>, C> OnEvent<C> for &mut S {
    type Event = S::Event;

    fn on_event(&mut self, event: Self::Event, context: &mut C) -> anyhow::Result<()> {
        S::on_event(self, event, context)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId(u32);

pub trait ScheduleEvent<M> {
    fn set(&mut self, period: Duration, event: M) -> anyhow::Result<TimerId>;

    fn unset(&mut self, id: TimerId) -> anyhow::Result<()>;
}

pub struct Erased<C, S>(S, PhantomData<C>);

type ErasedEvent<S, C> = Box<dyn FnOnce(&mut S, &mut C) -> anyhow::Result<()> + Send>;

impl<S, C> OnEvent<C> for Erased<C, S> {
    type Event = ErasedEvent<S, C>;

    fn on_event(&mut self, event: Self::Event, context: &mut C) -> anyhow::Result<()> {
        event(&mut self.0, context)
    }
}

pub trait OnErasedEvent<M, C> {
    fn on_event(&mut self, event: M, context: &mut C) -> anyhow::Result<()>;
}

pub struct ErasedSender<S, C, E>(E, PhantomData<(S, C)>);

impl<E: SendEvent<ErasedEvent<S, C>>, S: OnErasedEvent<M, C>, C, M: Send + 'static> SendEvent<M>
    for ErasedSender<S, C, E>
{
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        self.0.send(Box::new(move |state, context| {
            state.on_event(event, context)
        }))
    }
}
