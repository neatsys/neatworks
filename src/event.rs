use std::{collections::HashMap, fmt::Debug, time::Duration};

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};

pub trait SendEvent<M> {
    fn send(&mut self, event: M) -> anyhow::Result<()>;
}

pub trait OnEvent<M> {
    fn on_event(&mut self, event: M, timer: &mut dyn Timer<M>) -> anyhow::Result<()>;
}

// SendEvent -> OnEvent
// is this a generally reasonable blanket impl?
// anyway, this is not a To iff From scenario: there's semantic difference
// of implementing the two traits
// should always prefer to implement OnEvent for event consumers even if they
// don't make use of timers
impl<T: SendEvent<M>, M> OnEvent<M> for T {
    fn on_event(&mut self, event: M, _: &mut dyn Timer<M>) -> anyhow::Result<()> {
        self.send(event)
    }
}

// OnEvent -> SendEvent
pub struct Inline<'a, S, M>(pub &'a mut S, pub &'a mut dyn Timer<M>);

impl<S: Debug, M> Debug for Inline<'_, S, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inline")
            .field("state", &self.0)
            .finish_non_exhaustive()
    }
}

impl<S: OnEvent<M>, N: Into<M>, M> SendEvent<N> for Inline<'_, S, M> {
    fn send(&mut self, event: N) -> anyhow::Result<()> {
        self.0.on_event(event.into(), self.1)
    }
}

#[derive(Debug)]
pub struct Void; // for testing

impl<M> SendEvent<M> for Void {
    fn send(&mut self, _: M) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<N: Into<M>, M> SendEvent<N> for UnboundedSender<M> {
    fn send(&mut self, event: N) -> anyhow::Result<()> {
        UnboundedSender::send(self, event.into()).map_err(|_| anyhow::anyhow!("channel closed"))
    }
}

pub type TimerId = u32;

pub trait Timer<M> {
    fn set_internal(&mut self, duration: Duration, event: M) -> anyhow::Result<TimerId>;

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
}

impl<M> dyn Timer<M> + '_ {
    pub fn set(&mut self, duration: Duration, event: impl Into<M>) -> anyhow::Result<u32> {
        self.set_internal(duration, event.into())
    }
}

#[derive(Debug)]
enum SessionEvent<M> {
    Timer(TimerId, M),
    Other(M),
}

#[derive(Debug)]
pub struct SessionSender<M>(UnboundedSender<SessionEvent<M>>);

impl<M> Clone for SessionSender<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M> PartialEq for SessionSender<M> {
    fn eq(&self, other: &Self) -> bool {
        self.0.same_channel(&other.0)
    }
}

impl<M> Eq for SessionSender<M> {}

impl<M: Into<N>, N> SendEvent<M> for SessionSender<N> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        SendEvent::send(&mut self.0, SessionEvent::Other(event.into()))
    }
}

pub struct Session<M> {
    sender: UnboundedSender<SessionEvent<M>>,
    receiver: UnboundedReceiver<SessionEvent<M>>,
    timer_id: TimerId,
    timers: HashMap<TimerId, JoinHandle<()>>,
}

impl<M> Debug for Session<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("timer_id", &self.timer_id)
            .field("timers", &self.timers)
            .finish_non_exhaustive()
    }
}

impl<M> Session<M> {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self {
            sender,
            receiver,
            timer_id: 0,
            timers: Default::default(),
        }
    }
}

impl<M> Default for Session<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Session<M> {
    pub fn sender(&self) -> SessionSender<M> {
        SessionSender(self.sender.clone())
    }

    pub async fn run(&mut self, state: &mut impl OnEvent<M>) -> anyhow::Result<()>
    where
        M: Send + 'static,
    {
        loop {
            let event = match self
                .receiver
                .recv()
                .await
                .ok_or(anyhow::anyhow!("channel closed"))?
            {
                SessionEvent::Timer(timer_id, event) => {
                    if self.timers.remove(&timer_id).is_some() {
                        event
                    } else {
                        // unset/timeout contention, force to skip timer as long as it has been
                        // unset
                        // this could happen because of stalled timers in event waiting list
                        // another approach has been taken previously, by passing the timer events
                        // with a shared mutex state `timeouts`
                        // that should (probably) avoid this case in a single-thread runtime, but
                        // since tokio does not offer a generally synchronous `abort`, the following
                        // sequence is still possible in multithreading runtime
                        //   event loop lock `timeouts`
                        //   event callback `unset` timer which calls `abort`
                        //   event callback returns, event loop unlock `timeouts`
                        //   timer coroutine keep alive, lock `timeouts` and push event into it
                        //   timer coroutine finally get aborted
                        // the (probably) only solution is to implement a synchronous abort, block
                        // in `unset` call until timer coroutine replies with somehow promise of not
                        // sending timer event anymore, i don't feel that worth
                        // anyway, as long as this fallback presents the `abort` is logically
                        // redundant, just for hopefully better performance
                        // (so wish i have direct access to the timer wheel...)
                        continue;
                    }
                }
                SessionEvent::Other(event) => event,
            };
            state.on_event(event, self)?
        }
    }
}

impl<M: Send + 'static> Timer<M> for Session<M> {
    fn set_internal(&mut self, duration: Duration, event: M) -> anyhow::Result<TimerId> {
        self.timer_id += 1;
        let timer_id = self.timer_id;
        let sender = self.sender.clone();
        // broken error propagation in detached task
        // the error probably only happens when session exits, which (currently) only happens when
        // some (more essential) error happens
        let timer = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            sender.send(SessionEvent::Timer(timer_id, event)).unwrap();
        });
        self.timers.insert(timer_id, timer);
        Ok(timer_id)
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        self.timers
            .remove(&timer_id)
            .ok_or(anyhow::anyhow!("timer not exists"))?
            .abort();
        Ok(())
    }
}

// alternative design: type-erasure event
pub mod erased {
    use std::{collections::HashMap, time::Duration};

    use tokio::{
        sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        task::JoinHandle,
        time::sleep,
    };

    use super::{SendEvent, TimerId};

    pub trait Timer<S: ?Sized> {
        fn set<M: Send + Sync + 'static>(
            &mut self,
            duration: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEvent<M>;

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
    }

    pub trait OnEvent<M> {
        fn on_event(&mut self, event: M, timer: &mut impl Timer<Self>) -> anyhow::Result<()>;
    }

    impl<S: SendEvent<M>, M> OnEvent<M> for S {
        fn on_event(&mut self, event: M, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
            self.send(event)
        }
    }

    pub struct Inline<'a, S: ?Sized, T: ?Sized>(pub &'a mut S, pub &'a mut T);

    impl<S: OnEvent<M>, M, T: Timer<S>> SendEvent<M> for Inline<'_, S, T> {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            self.0.on_event(event, self.1)
        }
    }

    // TODO convert to enum when there's second implementation
    pub type Sender<S> = SessionSender<S>;

    #[derive(Debug)]
    pub struct SessionSender<S>(UnboundedSender<SessionEvent<S>>);

    enum SessionEvent<S: ?Sized> {
        Timer(TimerId, Event<S>),
        Other(Event<S>),
    }

    type Event<S> = Box<dyn FnOnce(&mut S, &mut Session<S>) -> anyhow::Result<()> + Send + Sync>;

    impl<S> Clone for SessionSender<S> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<S: OnEvent<M> + 'static, M: Send + Sync + 'static> SendEvent<M> for SessionSender<S> {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            let event = move |state: &mut S, timer: &mut _| state.on_event(event, timer);
            self.0
                .send(SessionEvent::Other(Box::new(event) as Event<_>))
                .map_err(|_| anyhow::anyhow!("channel closed"))
        }
    }

    #[derive(Debug)]
    pub struct Session<S: ?Sized> {
        sender: UnboundedSender<SessionEvent<S>>,
        receiver: UnboundedReceiver<SessionEvent<S>>,
        timer_id: TimerId,
        timers: HashMap<TimerId, JoinHandle<()>>,
    }

    impl<S> Session<S> {
        pub fn new() -> Self {
            let (sender, receiver) = unbounded_channel();
            Self {
                sender,
                receiver,
                timer_id: 0,
                timers: Default::default(),
            }
        }
    }

    impl<S> Default for Session<S> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<S> Session<S> {
        pub fn sender(&self) -> SessionSender<S> {
            SessionSender(self.sender.clone())
        }

        pub async fn run(&mut self, state: &mut S) -> anyhow::Result<()> {
            loop {
                let event = match self
                    .receiver
                    .recv()
                    .await
                    .ok_or(anyhow::anyhow!("channel closed"))?
                {
                    SessionEvent::Timer(timer_id, event) => {
                        if self.timers.remove(&timer_id).is_some() {
                            event
                        } else {
                            continue;
                        }
                    }
                    SessionEvent::Other(event) => event,
                };
                event(state, self)?
            }
        }
    }

    impl<S: ?Sized + 'static> Timer<S> for Session<S> {
        fn set<M: Send + Sync + 'static>(
            &mut self,
            duration: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEvent<M>,
        {
            self.timer_id += 1;
            let timer_id = self.timer_id;
            let sender = self.sender.clone();
            let timer = tokio::spawn(async move {
                sleep(duration).await;
                let event = move |state: &mut S, timer: &mut _| state.on_event(event, timer);
                sender
                    .send(SessionEvent::Timer(timer_id, Box::new(event)))
                    .unwrap();
            });
            self.timers.insert(timer_id, timer);
            Ok(timer_id)
        }

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
            Session::unset(self, timer_id)
        }
    }

    impl<S: ?Sized> Session<S> {
        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
            self.timers
                .remove(&timer_id)
                .ok_or(anyhow::anyhow!("timer not exists"))?
                .abort();
            Ok(())
        }
    }

    impl<S: ?Sized> Drop for Session<S> {
        fn drop(&mut self) {
            while let Some(timer_id) = self.timers.keys().next() {
                self.unset(*timer_id).unwrap()
            }
        }
    }
}
