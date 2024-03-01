pub mod ordered;
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
        event: impl FnMut() -> M + Send + Sync + 'static,
    ) -> anyhow::Result<TimerId>;

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
}

pub struct Buffered<S, M> {
    pub inner: S,
    attched: HashMap<TimerId, Box<dyn FnMut() -> M + Send + Sync>>,
}

struct BufferedTimer<'a, T, M> {
    inner: &'a mut T,
    attched: &'a mut HashMap<TimerId, Box<dyn FnMut() -> M + Send + Sync>>,
}

impl<T: Timer, M> RichTimer<M> for BufferedTimer<'_, T, M> {
    fn set(
        &mut self,
        period: Duration,
        event: impl FnMut() -> M + Send + Sync + 'static,
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

// alternative interface that performs type erasure on event types
pub mod erased {
    use std::{collections::HashMap, fmt::Debug, time::Duration};

    use super::{SendEvent, Timer, TimerId};

    // the universal event type: every `M` that state `impl OnEvent<M>` (and also `Send`) can be
    // turned into this form
    // this is weaker than it would be, which can be illustrated as
    //   FnOnce(&mut S, &mut impl Timer), or for<T: Timer> FnOnce(&mut S, &mut T)
    // the "real" higher rank trait bound (that works on general types instead of lifetimes) will
    // take really long to enter the language (if ever happens), and even if we have that in hand we
    // probably cannot use it here due to object safety, so i instead take this form that fixes the
    // timer type ahead of the time i.e. on producing instead of on consuming the event
    // this fact causes a few headaches, like the Buffered below has to fix the `impl Timer` type
    // as well, duplicated `OnEvent` and `OnTimer` trait to the ones in the super module below, and
    // the boilerplates dealing with infinite recusive types
    // it also prevent us to write down obivous facts like
    //   impl<S> OnEvent<Event<S, ???>> { ... }
    // which results in e.g. leaking internal details of `Session::run`
    pub type Event<S, T> = Box<dyn FnOnce(&mut S, &mut T) -> anyhow::Result<()> + Send>;

    pub trait OnEvent<M, T> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()>;
    }

    pub trait OnTimer<T> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()>;
    }

    // ideally the following impl should directly apply to S
    // however that will be conflicted with the following impl on Buffered
    // TODO try to apply this wrapper when necessary internally, instead of asking user to wrap
    // the states on their side
    #[derive(derive_more::Deref, derive_more::DerefMut)]
    pub struct Blanket<S>(pub S);

    impl<S: super::OnEvent<M>, M, T: Timer> OnEvent<M, T> for Blanket<S> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()> {
            self.0.on_event(event, timer)
        }
    }

    impl<S: super::OnTimer, T: Timer> OnTimer<T> for Blanket<S> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()> {
            self.0.on_timer(timer_id, timer)
        }
    }

    pub trait RichTimer<S> {
        fn set<M: Clone + Send + Sync + 'static>(
            &mut self,
            period: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEventRichTimer<M>;

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
    }

    pub trait OnEventRichTimer<M> {
        fn on_event(&mut self, event: M, timer: &mut impl RichTimer<Self>) -> anyhow::Result<()>
        where
            Self: Sized;
    }

    #[derive(derive_more::Deref, derive_more::DerefMut)]
    pub struct Buffered<S, T> {
        #[deref]
        #[deref_mut]
        inner: S,
        attached: Attached<S, T>,
    }

    impl<S: Debug, T> Debug for Buffered<S, T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Buffered")
                .field("inner", &self.inner)
                .finish_non_exhaustive()
        }
    }

    impl<S, T> From<S> for Buffered<S, T> {
        fn from(value: S) -> Self {
            Self {
                inner: value,
                attached: Default::default(),
            }
        }
    }

    type Attached<S, T> = HashMap<
        TimerId,
        // not FnMut() -> Event<S, BufferedTimer<'???, T, S>> because cannot write out the lifetime
        Box<
            dyn FnMut() -> Box<dyn FnOnce(&mut Buffered<S, T>, &mut T) -> anyhow::Result<()>>
                + Send
                + Sync,
        >,
    >;

    struct BufferedTimer<'a, T, S> {
        inner: &'a mut T,
        attached: &'a mut Attached<S, T>,
    }

    impl<T: Timer, S> RichTimer<S> for BufferedTimer<'_, T, S> {
        fn set<M: Clone + Send + Sync + 'static>(
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
                Box::new(move |buffered: &mut _, timer: &mut _| {
                    Buffered::on_event(buffered, event, timer)
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

    // boilerplates for type check
    // types like Event<S, T> and Buffered<S, T> mentions the fixed timer type `T`
    // the `impl Timer` type that fills this `T` e.g. `super::Session<_>` probably mentions the
    // event type which is `Event<S, T>` itself, causes a inifite recursion
    //   super::Session<Event<S, super::Session<...
    // on the `T` position
    // (the error reporting is really confusing in this case)
    // so unfortunately they have to be adapted for every `impl Timer` to be usable, each adaption
    // comes with a distinct newtype that mentions `Self` in T position, and I hardly see any option
    // to reduce this boilerplate except using macros

    pub use session::Session;

    pub mod session {
        use crate::event::TimerId;

        use super::{Erasure, OnEvent, OnEventRichTimer, OnTimer};

        #[derive(derive_more::From)]
        pub struct Event<S>(super::Event<S, crate::event::Session<Self>>);

        impl<S> std::fmt::Debug for Event<S> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("SessionEvent").finish_non_exhaustive()
            }
        }

        pub type Session<S> = crate::event::Session<Event<S>>;
        pub type Sender<S> = Erasure<crate::event::session::Sender<Event<S>>, S, Session<S>>;

        impl<S> Session<S> {
            pub fn erased_sender(&self) -> Sender<S> {
                Erasure(self.sender(), Default::default())
            }
        }

        impl<S: OnTimer<Self> + 'static> Session<S> {
            pub async fn erased_run(&mut self, state: &mut S) -> anyhow::Result<()> {
                self.run_internal(
                    state,
                    |state, Event(event), timer| event(state, timer),
                    OnTimer::on_timer,
                )
                .await
            }
        }

        #[derive(Debug, derive_more::Deref, derive_more::DerefMut)]
        pub struct Buffered<S>(super::Buffered<S, Session<Self>>);

        impl<S> From<S> for Buffered<S> {
            fn from(value: S) -> Self {
                Self(super::Buffered::from(value))
            }
        }

        impl<S: OnEventRichTimer<M> + 'static, M> OnEvent<M, Session<Buffered<S>>> for Buffered<S> {
            fn on_event(
                &mut self,
                event: M,
                timer: &mut Session<Buffered<S>>,
            ) -> anyhow::Result<()> {
                self.0.on_event(event, timer)
            }
        }

        impl<S: 'static> OnTimer<Session<Buffered<S>>> for Buffered<S> {
            fn on_timer(
                &mut self,
                timer_id: TimerId,
                timer: &mut Session<Buffered<S>>,
            ) -> anyhow::Result<()> {
                self.0.on_timer(timer_id, timer)
            }
        }
    }
}
