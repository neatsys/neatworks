// TL;DR
// wrap `OnEvent + OnTimer` with `Unify(_)`
// wrap `OnEventRichTimer` with `Unify(Buffered(_))`
// wrap `erased::OnEvent<_> + OnTimer` with `erased::Blanket(erased::Unify(_))`
// wrap `erased::OnEventRichTimer<_>` with
//   `erased::Blanket(erased::Buffered(_))`
// TODO convert these newtypes into internal details, instead of exposed as part
// of the interface
// even myself already get tired of dealing with them (and related error
// messages = =)
// https://zhuanlan.zhihu.com/p/685275310

pub mod blocking;
pub mod downcast;
pub mod linear;
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlackHole; // for testing

impl<M> SendEvent<M> for BlackHole {
    fn send(&mut self, _: M) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait SendEventOnce<M> {
    fn send_once(self, event: M) -> anyhow::Result<()>;
}

impl<M> SendEventOnce<M> for BlackHole {
    fn send_once(self, _: M) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<T: SendEventOnce<M>, M> SendEvent<M> for Option<T> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        if let Some(emit) = self.take() {
            emit.send_once(event)
        } else {
            Err(anyhow::anyhow!("can only send once"))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Once<E>(pub E);

impl<T: SendEvent<M>, M> SendEventOnce<M> for Once<T> {
    fn send_once(mut self, event: M) -> anyhow::Result<()> {
        self.0.send(event)
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
// is, calling `unset` will consume the TimerId so one timer cannot be unset
// multiple times (although runtime check will still catch it)
// `Clone` remains here because it has to: during model checking the network
// state, which probably contains a lot of `TimerId`s on application side, must
// be cloned (a lot). we surely can invent our own cloning mechanism that
// dedicated for model checking, but that would incur too much boilerplate and
// does not seem to pay off
// in conclusion the runtime check mentioned above is still necessary because of
// the existence of this `Clone` "backdoor", and the ownership is only for
// preventing silly mistakes
// besides `#[derive(Clone)]` scenario, it's always recommended to unpack the
// `TimerId` (as long as you have the access), clone (actually copy) the inner
// integer, then pack it back. the more involved procedure more clearly shows
// you know what you are doing
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimerId(u32);

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

pub trait OnEvent {
    type Event;

    fn on_event(&mut self, event: Self::Event, timer: &mut impl Timer) -> anyhow::Result<()>;
}

pub trait OnTimer {
    fn on_timer(&mut self, timer_id: TimerId, timer: &mut impl Timer) -> anyhow::Result<()>;
}

pub struct Inline<'a, S, T>(pub &'a mut S, pub &'a mut T);

impl<S: OnEventUniversal<T>, T: Timer> SendEvent<S::Event> for Inline<'_, S, T> {
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

pub trait OnEventRichTimer {
    type Event;

    fn on_event(
        &mut self,
        event: Self::Event,
        timer: &mut impl RichTimer<Event = Self::Event>,
    ) -> anyhow::Result<()>;
}

pub struct Buffered<S, M> {
    pub inner: S,
    attached: HashMap<TimerId, Box<dyn FnMut() -> M + Send + Sync>>,
}

struct BufferedTimer<'a, T, M> {
    inner: &'a mut T,
    attached: &'a mut HashMap<TimerId, Box<dyn FnMut() -> M + Send + Sync>>,
}

impl<T: Timer, M> RichTimer for BufferedTimer<'_, T, M> {
    type Event = M;

    fn set(
        &mut self,
        period: Duration,
        event: impl FnMut() -> Self::Event + Send + Sync + 'static,
    ) -> anyhow::Result<TimerId> {
        let TimerId(timer_id) = self.inner.set(period)?;
        let replaced = self.attached.insert(TimerId(timer_id), Box::new(event));
        anyhow::ensure!(replaced.is_none(), "duplicated timer id");
        Ok(TimerId(timer_id))
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        let removed = self.attached.remove(&timer_id);
        anyhow::ensure!(removed.is_some(), "missing timer attachment");
        self.inner.unset(timer_id)
    }
}

impl<S: OnEventRichTimer> OnEvent for Buffered<S, S::Event> {
    type Event = S::Event;

    fn on_event(&mut self, event: Self::Event, timer: &mut impl Timer) -> anyhow::Result<()> {
        let mut timer = BufferedTimer {
            inner: timer,
            attached: &mut self.attached,
        };
        self.inner.on_event(event, &mut timer)
    }
}

impl<S: OnEventRichTimer> OnTimer for Buffered<S, S::Event> {
    fn on_timer(&mut self, timer_id: TimerId, timer: &mut impl Timer) -> anyhow::Result<()> {
        let event = (self
            .attached
            .get_mut(&timer_id)
            .ok_or(anyhow::anyhow!("missing timer attachment"))?)();
        self.on_event(event, timer)
    }
}

pub trait OnEventUniversal<T> {
    type Event;

    fn on_event(&mut self, event: Self::Event, timer: &mut T) -> anyhow::Result<()>;
}

pub trait OnTimerUniversal<T> {
    fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()>;
}

#[derive(derive_more::Deref, derive_more::DerefMut)]
pub struct Unify<S>(pub S);

impl<S: OnEvent, T: Timer> OnEventUniversal<T> for Unify<S> {
    type Event = S::Event;

    fn on_event(&mut self, event: Self::Event, timer: &mut T) -> anyhow::Result<()> {
        self.0.on_event(event, timer)
    }
}

impl<S: OnTimer, T: Timer> OnTimerUniversal<T> for Unify<S> {
    fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()> {
        self.0.on_timer(timer_id, timer)
    }
}

pub use session::Session;

// alternative interface that performs type erasure on event types
pub mod erased {
    use std::{collections::HashMap, fmt::Debug, time::Duration};

    use derive_where::derive_where;

    use super::{OnEventUniversal, OnTimer, OnTimerUniversal, SendEvent, Timer, TimerId};

    // the universal event type: every `M` that state `impl OnEvent<M>` (and also `Send`) can be
    // turned into this form
    // this is weaker than it would be, which should be illustrated as
    //   FnOnce(&mut S, &mut impl Timer), or for<T: Timer> FnOnce(&mut S, &mut T)
    // the "real" higher rank trait bound (that works on general types instead of lifetimes) will
    // take really long to enter the language (if ever happens), and even if we have that in hand we
    // probably cannot use it here due to object safety, so i instead take this form that fixes the
    // timer type ahead of the time i.e. on producing instead of on consuming the event
    // this fact causes a few headaches, like the Buffered below has to stick on some fixed
    // `impl Timer` type (which prevents us to reuse the `Buffered` above), workaround trait
    // `OnEventFixTimer` must be introduced, etc
    pub type Event<S, T> = Box<dyn FnOnce(&mut S, &mut T) -> anyhow::Result<()> + Send>;

    pub trait OnEvent<M> {
        fn on_event(&mut self, event: M, timer: &mut impl Timer) -> anyhow::Result<()>;
    }

    pub struct Inline<'a, S, T>(pub &'a mut S, pub &'a mut T);

    impl<S: OnEvent<M>, M, T: Timer> SendEvent<M> for Inline<'_, S, T> {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            self.0.on_event(event, self.1)
        }
    }

    #[derive(derive_more::Deref, derive_more::DerefMut)]
    pub struct Blanket<S>(pub S);

    impl<S, T> OnEventUniversal<T> for Blanket<S> {
        type Event = Event<Blanket<S>, T>;

        fn on_event(&mut self, event: Self::Event, timer: &mut T) -> anyhow::Result<()> {
            event(self, timer)
        }
    }

    impl<S: OnTimerUniversal<T>, T> OnTimerUniversal<T> for Blanket<S> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()> {
            self.0.on_timer(timer_id, timer)
        }
    }

    trait OnEventFixTimer<M, T> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()>;
    }

    #[derive(derive_more::Deref, derive_more::DerefMut)]
    pub struct Unify<S>(pub S);

    impl<S: OnEvent<M>, M, T: Timer> OnEventFixTimer<M, T> for Unify<S> {
        fn on_event(&mut self, event: M, timer: &mut T) -> anyhow::Result<()> {
            self.0.on_event(event, timer)
        }
    }

    impl<S: OnTimer, T: Timer> OnTimerUniversal<T> for Unify<S> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()> {
            self.0.on_timer(timer_id, timer)
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
            E: SendEvent<Event<Blanket<S>, T>>,
            S: OnEventFixTimer<M, T>,
            T: Timer,
            M: Send + 'static,
        > SendEvent<M> for Erasure<E, Blanket<S>, T>
    {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            let event = move |state: &mut Blanket<S>, timer: &mut T| {
                OnEventFixTimer::on_event(&mut **state, event, timer)
            };
            self.0.send(Box::new(event))
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
    #[derive_where(Debug; S)]
    pub struct Buffered<S, T> {
        #[deref]
        #[deref_mut]
        inner: S,
        #[derive_where(skip)]
        attached: Attached<S, T>,
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

    impl<S, T> From<S> for Buffered<S, T> {
        fn from(value: S) -> Self {
            Self {
                inner: value,
                attached: Default::default(),
            }
        }
    }

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
            anyhow::ensure!(replaced.is_none(), "duplicated timer id");
            Ok(TimerId(timer_id))
        }

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
            let removed = self.attached.remove(&timer_id);
            anyhow::ensure!(removed.is_some(), "missing timer attachment");
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

    impl<S, T: Timer> OnTimerUniversal<T> for Buffered<S, T> {
        fn on_timer(&mut self, timer_id: TimerId, timer: &mut T) -> anyhow::Result<()> {
            (self
                .attached
                .get_mut(&timer_id)
                .ok_or(anyhow::anyhow!("missing timer attachment"))?)()(self, timer)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct TimerState<M>(Option<(M, TimerId)>, Duration);

    impl<M> TimerState<M> {
        pub fn new(period: Duration) -> Self {
            Self(Default::default(), period)
        }

        pub fn set<S: OnEventRichTimer<M>>(
            &mut self,
            event: M,
            timer: &mut impl RichTimer<S>,
        ) -> anyhow::Result<()>
        where
            M: Clone + Send + Sync + 'static,
        {
            anyhow::ensure!(
                self.ensure_unset(timer)?.is_none(),
                "timer has already been set"
            );
            self.ensure_set(event, timer)
        }

        pub fn unset<S>(&mut self, timer: &mut impl RichTimer<S>) -> anyhow::Result<M> {
            let Some(event) = self.ensure_unset(timer)? else {
                anyhow::bail!("timer has not been set")
            };
            Ok(event)
        }

        pub fn reset<S: OnEventRichTimer<M>>(
            &mut self,
            timer: &mut impl RichTimer<S>,
        ) -> anyhow::Result<()>
        where
            M: Clone + Send + Sync + 'static,
        {
            let event = self.unset(timer)?;
            self.set(event, timer)
        }

        pub fn ensure_set<S: OnEventRichTimer<M>>(
            &mut self,
            event: M,
            timer: &mut impl RichTimer<S>,
        ) -> anyhow::Result<()>
        where
            M: Clone + Send + Sync + 'static,
        {
            if self.0.is_none() {
                self.0 = Some((event.clone(), timer.set(self.1, event)?))
            }
            Ok(())
        }

        pub fn ensure_unset<S>(
            &mut self,
            timer: &mut impl RichTimer<S>,
        ) -> anyhow::Result<Option<M>> {
            if let Some((event, timer_id)) = self.0.take() {
                timer.unset(timer_id)?;
                Ok(Some(event))
            } else {
                Ok(None)
            }
        }
    }

    pub use session::Session;

    pub mod events {
        #[derive(Debug)]
        pub struct Init;
    }

    pub mod session {
        use crate::event::session::SessionTimer;

        use super::Erasure;

        // some historical snippet when `Timer` was still implemented by `Session<_>` itself
        // #[derive(derive_more::From)]
        // pub struct Event<S>(super::Event<S, crate::event::Session<Self>>);

        pub type Event<S> = super::Event<S, SessionTimer>;
        pub type Session<S> = crate::event::Session<Event<S>>;
        pub type Sender<S> = Erasure<crate::event::session::Sender<Event<S>>, S, SessionTimer>;

        pub type Buffered<S> = super::Buffered<S, SessionTimer>;
    }

    pub mod blocking {
        use crate::event::ordered::Timer;

        use super::Erasure;

        pub type Event<S> = super::Event<S, Timer>;
        pub type Sender<S> = Erasure<crate::event::blocking::Sender<Event<S>>, S, Timer>;

        pub type Buffered<S> = super::Buffered<S, Timer>;
    }
}

// cSpell:words newtype
