pub mod session;

use std::{collections::HashMap, time::Duration};

pub trait SendEvent<M> {
    fn send(&mut self, event: M) -> anyhow::Result<()>;
}

impl<T: ?Sized + SendEvent<M>, M> SendEvent<M> for Box<T> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        T::send(self, event)
    }
}

// pub type TimerId = u32;
// intentionally switch to a non-Copy newtype to mitigate use after free, that
// is, calling `unset` consume the TimerId so the timer cannot be referred
// anymore
// this does not solve leak though so Clone is permitted
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TimerId(pub u32);

pub trait Timer {
    fn set(&mut self, period: Duration) -> anyhow::Result<TimerId>;

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
}

pub struct UnreachableTimer;

impl Timer for UnreachableTimer {
    fn set(&mut self, _: Duration) -> anyhow::Result<TimerId> {
        unreachable!()
    }

    fn unset(&mut self, _: TimerId) -> anyhow::Result<()> {
        unreachable!()
    }
}

pub trait OnEvent<M> {
    fn on_event(&mut self, event: M, timer: &mut impl Timer) -> anyhow::Result<()>;
}

pub trait OnTimer {
    fn on_timer(&mut self, timer_id: TimerId, timer: &mut impl Timer) -> anyhow::Result<()>;
}

pub struct Void; // for testing

impl<M> SendEvent<M> for Void {
    fn send(&mut self, _: M) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait RichTimer<M> {
    fn set(
        &mut self,
        period: Duration,
        event: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<TimerId>;

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
}

pub struct Buffered<S, M> {
    pub inner: S,
    attched: HashMap<TimerId, Box<dyn FnMut() -> M + Send>>,
}

struct BufferedTimer<'a, T, M> {
    inner: &'a mut T,
    attched: &'a mut HashMap<TimerId, Box<dyn FnMut() -> M + Send>>,
}

impl<T: Timer, M> RichTimer<M> for BufferedTimer<'_, T, M> {
    fn set(
        &mut self,
        period: Duration,
        event: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<TimerId> {
        let TimerId(timer_id) = self.inner.set(period)?;
        let replaced = self.attched.insert(TimerId(timer_id), Box::new(event));
        if replaced.is_some() {
            anyhow::bail!("duplicated timer id")
        }
        Ok(TimerId(timer_id))
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        let removed = self.attched.remove(&timer_id);
        if removed.is_some() {
            anyhow::bail!("missing timer attachment")
        }
        self.inner.unset(timer_id)
    }
}

pub trait OnEventRichTimer<M> {
    fn on_event(&mut self, event: M, timer: &mut impl RichTimer<M>) -> anyhow::Result<()>;
}

impl<S: OnEventRichTimer<M>, M> OnEvent<M> for Buffered<S, M> {
    fn on_event(&mut self, event: M, timer: &mut impl Timer) -> anyhow::Result<()> {
        let mut timer = BufferedTimer {
            inner: timer,
            attched: &mut self.attched,
        };
        self.inner.on_event(event, &mut timer)
    }
}

impl<S: OnEventRichTimer<M>, M> OnTimer for Buffered<S, M> {
    fn on_timer(&mut self, timer_id: TimerId, timer: &mut impl Timer) -> anyhow::Result<()> {
        let event = (self
            .attched
            .get_mut(&timer_id)
            .ok_or(anyhow::anyhow!("missing timer attachment"))?)();
        self.on_event(event, timer)
    }
}

pub use session::Session;

pub mod erased {
    use std::{collections::HashMap, time::Duration};

    use super::{SendEvent, Timer, TimerId};

    pub type Event<S, T> = Box<dyn FnOnce(&mut S, &mut T) -> anyhow::Result<()> + Send>;

    trait OnEvent<M, T> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()>;
    }

    pub trait OnTimer<T> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()>;
    }

    pub trait RichTimer<S> {
        fn set<M: Clone + Send + 'static>(
            &mut self,
            period: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEventRichTimer<M>;

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
    }

    // a impl<T: super::Timer<Event<S, T>>, S> Timer<S> for T may be desired
    // but currently it's not used, the session does another wrapping before working
    // guess that will be case for every future event scheduler, hope not too ugly boilerplates

    pub trait OnEventRichTimer<M> {
        fn on_event(&mut self, event: M, timer: &mut impl RichTimer<Self>) -> anyhow::Result<()>
        where
            Self: Sized;
    }

    pub struct Buffered<S, T> {
        inner: S,
        attached: HashMap<
            TimerId,
            Box<dyn FnMut() -> Box<dyn FnOnce(&mut Self, &mut T) -> anyhow::Result<()> + Send>>,
        >,
    }

    struct BufferedTimer<'a, T, S> {
        inner: &'a mut T,
        attached: &'a mut HashMap<
            TimerId,
            Box<
                dyn FnMut() -> Box<
                    dyn FnOnce(&mut Buffered<S, T>, &mut T) -> anyhow::Result<()> + Send,
                >,
            >,
        >,
    }

    impl<T: Timer, S> RichTimer<S> for BufferedTimer<'_, T, S> {
        fn set<M: Clone + Send + 'static>(
            &mut self,
            period: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEventRichTimer<M>,
        {
            let TimerId(timer_id) = self.inner.set(period)?;
            let action = move || {
                let event = event.clone();
                Box::new(move |buffered: &mut Buffered<_, _>, timer: &mut _| {
                    buffered.on_event(event, timer)
                }) as _
            };
            let replaced = self.attached.insert(TimerId(timer_id), Box::new(action));
            if replaced.is_some() {
                anyhow::bail!("duplicated timer id")
            }
            Ok(TimerId(timer_id))
        }

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
            self.inner.unset(timer_id)
        }
    }

    impl<S: OnEventRichTimer<M>, M, T: Timer> OnEvent<M, T> for Buffered<S, T> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()> {
            let mut timer = BufferedTimer {
                inner: timer,
                attached: &mut self.attached,
            };
            self.inner.on_event(event, &mut timer)
        }
    }

    impl<S, T: Timer> OnTimer<T> for Buffered<S, T> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()> {
            (self
                .attached
                .get_mut(&timer_id)
                .ok_or(anyhow::anyhow!("missing timer attachment"))?)()(self, timer)
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

    impl<E: SendEvent<Event<S, T>>, S: OnEvent<M, T>, T: Timer, M: Send + Sync + 'static>
        SendEvent<M> for Erasure<E, S, T>
    {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            let event = move |state: &mut S, timer: &mut T| state.on_event(event, timer);
            self.0.send(Box::new(event))
        }
    }

    // Session-specific code onward
    // a must-have newtype to allow us talk about Self type in super::Session's event position
    #[derive(derive_more::From)]
    pub struct SessionEvent<S>(Event<S, super::Session<Self>>);

    impl<S> std::fmt::Debug for SessionEvent<S> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SessionEvent").finish_non_exhaustive()
        }
    }

    pub type Session<S> = super::Session<SessionEvent<S>>;
    // could do a `pub mod seesion { type Sender = ... }` but that's too silly
    pub type SessionSender<S> = Erasure<super::session::Sender<SessionEvent<S>>, S, Session<S>>;

    impl<S> Session<S> {
        pub fn erased_sender(&self) -> SessionSender<S> {
            Erasure(self.sender(), Default::default())
        }
    }

    impl<S: OnTimer<Self> + 'static> Session<S> {
        pub async fn erased_run(&mut self, state: &mut S) -> anyhow::Result<()> {
            self.run_internal(
                state,
                |state, SessionEvent(event), timer| event(state, timer),
                OnTimer::on_timer,
            )
            .await
        }
    }

    // if brings phantom T and blanket impl for T: Timer, the impl does not get picked up
    // because the exact T will become a infinite recursive type
    // the error reporting is really confusing in this case
    #[derive(derive_more::From, derive_more::Deref, derive_more::DerefMut)]
    pub struct FixTimer<S>(pub S);

    impl<S: super::OnEvent<M> + 'static, M> OnEvent<M, Session<Self>> for FixTimer<S> {
        fn on_event(&mut self, event: M, timer: &mut Session<Self>) -> anyhow::Result<()> {
            self.0.on_event(event, timer)
        }
    }

    impl<S: super::OnTimer + 'static> OnTimer<Session<Self>> for FixTimer<S> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut Session<Self>) -> anyhow::Result<()> {
            self.0.on_timer(timer_id, timer)
        }
    }
}
