pub mod blocking;
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

pub struct Void; // for testing

impl<M> SendEvent<M> for Void {
    fn send(&mut self, _: M) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, derive_more::Deref, derive_more::DerefMut)]
pub struct Transient<M>(Vec<M>);

impl<M> Default for Transient<M> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<N: Into<M>, M> SendEvent<N> for Transient<M> {
    fn send(&mut self, event: N) -> anyhow::Result<()> {
        self.0.push(event.into());
        Ok(())
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

#[derive(Clone, Default)]
pub struct LinearTimer {
    events: Vec<(u32, Duration)>,
    timer_id: u32,
}

impl LinearTimer {
    pub fn events(&self) -> Vec<TimerId> {
        let mut events = Vec::new();
        let mut prev_period = None;
        for &(id, period) in &self.events {
            if let Some(prev_period) = prev_period {
                if period >= prev_period {
                    break;
                }
            }
            events.push(TimerId(id));
            prev_period = Some(period)
        }
        events
    }

    fn unset(&mut self, TimerId(id): TimerId) -> anyhow::Result<(u32, Duration)> {
        let i = self
            .events
            .iter()
            .position(|(timer_id, _)| *timer_id == id)
            .ok_or(anyhow::anyhow!("timer not found"))?;
        Ok(self.events.remove(i))
    }

    pub fn step_timer(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        let event = self.unset(timer_id)?;
        self.events.push(event);
        Ok(())
    }
}

impl Timer for LinearTimer {
    fn set(&mut self, period: Duration) -> anyhow::Result<TimerId> {
        self.timer_id += 1;
        let timer_id = self.timer_id;
        self.events.push((timer_id, period));
        Ok(TimerId(timer_id))
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        LinearTimer::unset(self, timer_id)?;
        Ok(())
    }
}

pub trait OnEvent {
    type Event;

    fn on_event(&mut self, event: Self::Event, timer: &mut impl Timer) -> anyhow::Result<()>;
}

pub trait OnTimer {
    fn on_timer(&mut self, timer_id: TimerId, timer: &mut impl Timer) -> anyhow::Result<()>;
}

pub struct Inline<'a, S, T>(pub &'a mut S, pub &'a mut T);

impl<S: OnEvent, T: Timer> SendEvent<S::Event> for Inline<'_, S, T> {
    fn send(&mut self, event: S::Event) -> anyhow::Result<()> {
        self.0.on_event(event, self.1)
    }
}

pub trait RichTimer {
    type Event;

    fn set(
        &mut self,
        period: Duration,
        event: impl FnMut() -> Self::Event + Send + Sync + 'static,
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

impl<T: Timer, M> RichTimer for BufferedTimer<'_, T, M> {
    type Event = M;

    fn set(
        &mut self,
        period: Duration,
        event: impl FnMut() -> Self::Event + Send + Sync + 'static,
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

pub trait OnEventRichTimer {
    type Event;

    fn on_event(
        &mut self,
        event: Self::Event,
        timer: &mut impl RichTimer<Event = Self::Event>,
    ) -> anyhow::Result<()>;
}

impl<S: OnEventRichTimer> OnEvent for Buffered<S, S::Event> {
    type Event = S::Event;

    fn on_event(&mut self, event: Self::Event, timer: &mut impl Timer) -> anyhow::Result<()> {
        let mut timer = BufferedTimer {
            inner: timer,
            attched: &mut self.attched,
        };
        self.inner.on_event(event, &mut timer)
    }
}

impl<S: OnEventRichTimer> OnTimer for Buffered<S, S::Event> {
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
    // as well, and workaround traits `OnEventFixTimer` and `OnTimerFixTimer` (which unfortunately
    // must be public and even get referenced by replication artifact)
    // it also prevent us to write down obivous facts like
    //   impl<S> OnEvent<Event<S, ???>> { ... }
    // which results in e.g. leaking internal details of `Session::run`
    pub type Event<S, T> = Box<dyn FnOnce(&mut S, &mut T) -> anyhow::Result<()> + Send>;

    pub trait OnEvent<M> {
        fn on_event(&mut self, event: M, timer: &mut impl Timer) -> anyhow::Result<()>;
    }

    pub trait OnEventFixTimer<M, T> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()>;
    }

    pub trait OnTimerFixTimer<T> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()>;
    }

    // impl<S, T> OnEventFixTimer<Event<S, T>, T> for S {
    //     fn on_event(&mut self, event: Event<S, T>, timer: &mut T) -> anyhow::Result<()> {
    //         event(self, timer)
    //     }
    // }

    // ideally the following impl should directly apply to S
    // however that will be conflicted with the following impl on Buffered
    // TODO try to apply this wrapper when necessary internally, instead of asking user to wrap
    // the states on their side
    // anyway for now, the "injudicious" rule is that wrap `impl erased::OnEvent<_>` with `Blanket`
    // and wrap `impl erased::OnEventRichTimer<_>` with one of the `Buffered`, then it proabably
    // works
    #[derive(derive_more::Deref, derive_more::DerefMut)]
    pub struct Blanket<S>(pub S);

    impl<S: OnEvent<M>, M, T: Timer> OnEventFixTimer<M, T> for Blanket<S> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()> {
            self.0.on_event(event, timer)
        }
    }

    impl<S: super::OnTimer, T: Timer> OnTimerFixTimer<T> for Blanket<S> {
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

    impl<S: OnEventRichTimer<M>, M, T: Timer> OnEventFixTimer<M, T> for Buffered<S, T> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()> {
            let mut timer = BufferedTimer {
                inner: timer,
                attached: &mut self.attached,
            };
            self.inner.on_event(event, &mut timer)
        }
    }

    impl<S, T: Timer> OnTimerFixTimer<T> for Buffered<S, T> {
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

    impl<
            E: SendEvent<Event<S, T>>,
            S: OnEventFixTimer<M, T>,
            T: Timer,
            M: Send + Sync + 'static,
        > SendEvent<M> for Erasure<E, S, T>
    {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            let event = move |state: &mut S, timer: &mut T| state.on_event(event, timer);
            self.0.send(Box::new(event))
        }
    }

    pub use session::Session;

    pub mod session {
        use crate::event::session::SessionTimer;

        use super::{Erasure, OnTimerFixTimer};

        // some historical snippet when `Timer` was still implemented by `Session<_>` itself
        // #[derive(derive_more::From)]
        // pub struct Event<S>(super::Event<S, crate::event::Session<Self>>);

        pub type Event<S> = super::Event<S, SessionTimer>;
        pub type Session<S> = crate::event::Session<Event<S>>;
        pub type Sender<S> = Erasure<crate::event::session::Sender<Event<S>>, S, SessionTimer>;

        impl<S> Session<S> {
            pub fn erased_sender(&self) -> Sender<S> {
                Erasure(self.sender(), Default::default())
            }
        }

        impl<S: OnTimerFixTimer<SessionTimer> + 'static> Session<S> {
            pub async fn erased_run(&mut self, state: &mut S) -> anyhow::Result<()> {
                self.run_internal(
                    state,
                    |state, event, timer| event(state, timer),
                    OnTimerFixTimer::on_timer,
                )
                .await
            }
        }

        pub type Buffered<S> = super::Buffered<S, SessionTimer>;
    }

    pub mod blocking {
        use std::sync::mpsc::Receiver;

        use crate::event::{blocking::run_internal, ordered::Timer};

        use super::{Erasure, OnTimerFixTimer};

        pub type Event<S> = super::Event<S, Timer>;
        pub type Sender<S> = Erasure<crate::event::blocking::Sender<Event<S>>, S, Timer>;

        pub fn channel<S>() -> (Sender<S>, Receiver<Event<S>>) {
            let (sender, receiver) = std::sync::mpsc::channel();
            (Erasure::from(sender), receiver)
        }

        pub fn run<S: OnTimerFixTimer<Timer>>(
            receiver: Receiver<Event<S>>,
            state: &mut S,
        ) -> anyhow::Result<()> {
            run_internal(
                receiver,
                state,
                |state, event, timer| event(state, timer),
                OnTimerFixTimer::on_timer,
            )
        }

        pub type Buffered<S> = super::Buffered<S, Timer>;
    }
}
