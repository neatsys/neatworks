pub mod session;

use std::{fmt::Debug, time::Duration};

pub trait SendEvent<M> {
    fn send(&mut self, event: M) -> anyhow::Result<()>;
}

impl<T: ?Sized + SendEvent<M>, M> SendEvent<M> for Box<T> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        T::send(self, event)
    }
}

pub trait OnEvent<M> {
    fn on_event(&mut self, event: M, timer: &mut impl Timer<M>) -> anyhow::Result<()>;
}

// SendEvent -> OnEvent
// is this a generally reasonable blanket impl?
// anyway, this is not a To iff From scenario: there's semantic difference
// of implementing the two traits
// should always prefer to implement OnEvent for event consumers even if they
// don't make use of timers
impl<T: SendEvent<M>, M> OnEvent<M> for T {
    fn on_event(&mut self, event: M, _: &mut impl Timer<M>) -> anyhow::Result<()> {
        self.send(event)
    }
}

// OnEvent -> SendEvent cannot be trivially converted because a timer service is involved

#[derive(Debug)]
pub struct Void; // for testing

impl<M> SendEvent<M> for Void {
    fn send(&mut self, _: M) -> anyhow::Result<()> {
        Ok(())
    }
}

// pub type TimerId = u32;
// intentionally switch to a non-Clone (nor even Copy) newtype to mitigate use
// after free, that is, calling `unset` consume the TimerId so the timer cannot
// be referred anymore
// this does not solve leak though
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct TimerId(u32);

pub trait Timer<M> {
    fn set_dyn(
        &mut self,
        period: Duration,
        event: Box<dyn FnMut() -> M + Send>,
    ) -> anyhow::Result<TimerId>;

    fn set(
        &mut self,
        period: Duration,
        event: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<TimerId>
    where
        Self: Sized,
    {
        self.set_dyn(period, Box::new(event))
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
}

impl<M> dyn Timer<M> + '_ {
    pub fn set<N: Into<M>>(
        &mut self,
        duration: Duration,
        mut event: impl FnMut() -> N + Send + 'static,
    ) -> anyhow::Result<TimerId> {
        self.set_dyn(duration, Box::new(move || event().into()))
    }
}

pub use session::Session;
pub type Sender<M> = session::SessionSender<M>;

// alternative design: type-erasure event
// the goal here is to only wrap other code in this module, so e.g. erased session is inside this
// instead of in event::session:erased
pub mod erased {
    use std::{fmt::Debug, time::Duration};

    use super::{SendEvent, TimerId};

    // the "universal event type" that every event can turn into
    // this is slightly weaker than it suppose to be which is
    //   for<T: Timer<S>> FnOnce(&mut S, &mut T) -> ...
    // this cannot be easily done, since we need the FnOnce to be object safe
    // so instead the restriction is propagated backward from receiver side to sender side, i.e.
    // the timer type `T` is fixed at the time when the universal event get sent
    // this is controled by the `T` parameter on `Erasure` below
    pub type Event<S, T> = Box<dyn FnOnce(&mut S, &mut T) -> anyhow::Result<()> + Send + Sync>;

    pub trait Timer<S> {
        fn set<M: Clone + Send + Sync + 'static>(
            &mut self,
            period: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEvent<M>;

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
    }

    // a impl<T: super::Timer<Event<S, T>>, S> Timer<S> for T may be desired
    // but currently it's not used, the session does another wrapping before working
    // guess that will be case for every future event scheduler, hope not too ugly boilerplates

    pub trait OnEvent<M> {
        fn on_event(&mut self, event: M, timer: &mut impl Timer<Self>) -> anyhow::Result<()>
        where
            Self: Sized;
    }

    impl<S: SendEvent<M>, M> OnEvent<M> for S {
        fn on_event(&mut self, event: M, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
            self.send(event)
        }
    }

    #[derive(Debug)]
    pub struct Erasure<E, S, T>(E, std::marker::PhantomData<(S, T)>);

    impl<E, S, T> From<E> for Erasure<E, S, T> {
        fn from(value: E) -> Self {
            Self(value, Default::default())
        }
    }

    impl<E: Clone, S, T> Clone for Erasure<E, S, T> {
        fn clone(&self) -> Self {
            Self::from(self.0.clone())
        }
    }

    impl<E: SendEvent<Event<S, T>>, S: OnEvent<M>, T: Timer<S>, M: Send + Sync + 'static>
        SendEvent<M> for Erasure<E, S, T>
    {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            let event = move |state: &mut S, timer: &mut T| state.on_event(event, timer);
            self.0.send(Box::new(event))
        }
    }

    // TODO convert to enum when there's second implementation
    pub type Sender<S> = SessionSender<S>;

    // Session-specific code onward
    // a must-have newtype to allow us talk about Self type in super::Session's event position
    #[derive(derive_more::From)]
    pub struct SessionEvent<S>(Event<S, super::Session<Self>>);

    impl<S> Debug for SessionEvent<S> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SessionEvent").finish_non_exhaustive()
        }
    }

    // this implementation for super::Session<SessionEvent<S>> has been generalized into a blanket
    // impl on T, but yet to be generalized over SessionEvent<S> (into
    // `F where Event<S, T>: Into<F>`)
    // that would require a newtype to tag the impl, and i don't bother
    impl<T: super::Timer<SessionEvent<S>>, S: 'static> Timer<S> for T {
        fn set<M: Clone + Send + Sync + 'static>(
            &mut self,
            period: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEvent<M>,
        {
            super::Timer::set(self, period, move || {
                let event = event.clone();
                let event = move |state: &mut S, timer: &mut _| state.on_event(event, timer);
                SessionEvent(Box::new(event))
            })
        }

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
            super::Timer::unset(self, timer_id)
        }
    }

    pub type Session<S> = super::Session<SessionEvent<S>>;
    pub type SessionSender<S> = Erasure<super::Sender<SessionEvent<S>>, S, Session<S>>;

    impl<S> Session<S> {
        pub fn erased_sender(&self) -> Sender<S> {
            Erasure(self.sender(), Default::default())
        }
    }

    impl<S: 'static> Session<S> {
        pub async fn erased_run(&mut self, state: &mut S) -> anyhow::Result<()> {
            self.run_internal(state, |state, SessionEvent(event), timer| {
                event(state, timer)
            })
            .await
        }
    }
}
