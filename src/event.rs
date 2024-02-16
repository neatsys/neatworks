use std::{collections::HashMap, fmt::Debug, time::Duration};

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::{AbortHandle, JoinError, JoinSet},
    time::interval,
};

pub trait SendEvent<M> {
    fn send(&mut self, event: M) -> anyhow::Result<()>;
}

impl<T: ?Sized + SendEvent<M>, M> SendEvent<M> for Box<T> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        T::send(self, event)
    }
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
    fn set_internal(
        &mut self,
        duration: Duration,
        event: Box<dyn FnMut() -> M + Send>,
    ) -> anyhow::Result<TimerId>;

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
}

impl<M> dyn Timer<M> + '_ {
    pub fn set<N: Into<M>>(
        &mut self,
        duration: Duration,
        mut event: impl FnMut() -> N + Send + 'static,
    ) -> anyhow::Result<u32> {
        self.set_internal(duration, Box::new(move || event().into()))
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
    timer_sessions: JoinSet<anyhow::Result<()>>,
    timer_handles: HashMap<TimerId, AbortHandle>,
}

impl<M> Debug for Session<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("timer_id", &self.timer_id)
            .field("timers", &self.timer_handles)
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
            timer_sessions: JoinSet::new(),
            timer_handles: Default::default(),
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
            enum Select<M> {
                JoinNext(Result<anyhow::Result<()>, JoinError>),
                Recv(Option<SessionEvent<M>>),
            }
            let event = match tokio::select! {
                Some(result) = self.timer_sessions.join_next() => Select::JoinNext(result),
                recv = self.receiver.recv() => Select::Recv(recv)
            } {
                Select::JoinNext(Err(err)) if err.is_cancelled() => continue,
                Select::JoinNext(result) => {
                    result??;
                    continue;
                }
                Select::Recv(event) => event.ok_or(anyhow::anyhow!("channel closed"))?,
            };
            let event = match event {
                SessionEvent::Timer(timer_id, event) => {
                    if self.timer_handles.contains_key(&timer_id) {
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
    fn set_internal(
        &mut self,
        period: Duration,
        mut event: Box<dyn FnMut() -> M + Send>,
    ) -> anyhow::Result<TimerId> {
        self.timer_id += 1;
        let timer_id = self.timer_id;
        let mut sender = self.sender.clone();
        let handle = self.timer_sessions.spawn(async move {
            let mut interval = interval(period);
            loop {
                interval.tick().await;
                SendEvent::send(&mut sender, SessionEvent::Timer(timer_id, event()))?
            }
        });
        self.timer_handles.insert(timer_id, handle);
        Ok(timer_id)
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
        self.timer_handles
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
        task::{AbortHandle, JoinError, JoinSet},
        time::interval,
    };

    use super::{SendEvent, TimerId};

    pub trait Timer<S: ?Sized> {
        fn set<M: Clone + Send + Sync + 'static>(
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

    type Event<S, T> = Box<dyn FnOnce(&mut S, &mut T) -> anyhow::Result<()> + Send + Sync>;

    impl<E: SendEvent<Event<S, T>>, S: OnEvent<M>, T: Timer<S>, M: Send + Sync + 'static>
        SendEvent<M> for Erasure<E, S, T>
    {
        fn send(&mut self, event: M) -> anyhow::Result<()> {
            let event = move |state: &mut S, timer: &mut T| state.on_event(event, timer);
            self.0.send(Box::new(event))
        }
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
    pub type Sender<S> = Erasure<SessionSender<S>, S, Session<S>>;

    #[derive(Debug)]
    pub struct SessionSender<S>(UnboundedSender<SessionEvent<S>>);

    enum SessionEvent<S: ?Sized> {
        Timer(TimerId, Event<S, Session<S>>),
        Other(Event<S, Session<S>>),
    }

    impl<S> Clone for SessionSender<S> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }

    impl<S> SendEvent<Event<S, Session<S>>> for SessionSender<S> {
        fn send(&mut self, event: Event<S, Session<S>>) -> anyhow::Result<()> {
            self.0
                .send(SessionEvent::Other(event))
                .map_err(|_| anyhow::anyhow!("channel closed"))
        }
    }

    #[derive(Debug)]
    pub struct Session<S: ?Sized> {
        sender: UnboundedSender<SessionEvent<S>>,
        receiver: UnboundedReceiver<SessionEvent<S>>,
        timer_id: TimerId,
        timer_sessions: JoinSet<anyhow::Result<()>>,
        timer_handles: HashMap<TimerId, AbortHandle>,
    }

    impl<S> Session<S> {
        pub fn new() -> Self {
            let (sender, receiver) = unbounded_channel();
            Self {
                sender,
                receiver,
                timer_id: 0,
                timer_sessions: JoinSet::new(),
                timer_handles: Default::default(),
            }
        }
    }

    impl<S> Default for Session<S> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<S> Session<S> {
        pub fn sender(&self) -> Sender<S> {
            Erasure::from(SessionSender(self.sender.clone()))
        }

        pub async fn run(&mut self, state: &mut S) -> anyhow::Result<()> {
            loop {
                enum Select<M> {
                    JoinNext(Result<anyhow::Result<()>, JoinError>),
                    Recv(Option<SessionEvent<M>>),
                }
                let event = match tokio::select! {
                    Some(result) = self.timer_sessions.join_next() => Select::JoinNext(result),
                    recv = self.receiver.recv() => Select::Recv(recv)
                } {
                    Select::JoinNext(Err(err)) if err.is_cancelled() => continue,
                    Select::JoinNext(result) => {
                        result??;
                        continue;
                    }
                    Select::Recv(event) => event.ok_or(anyhow::anyhow!("channel closed"))?,
                };
                let event = match event {
                    SessionEvent::Timer(timer_id, event) => {
                        if self.timer_handles.contains_key(&timer_id) {
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
        fn set<M: Clone + Send + Sync + 'static>(
            &mut self,
            period: Duration,
            event: M,
        ) -> anyhow::Result<TimerId>
        where
            S: OnEvent<M>,
        {
            self.timer_id += 1;
            let timer_id = self.timer_id;
            let mut sender = self.sender.clone();
            let handle = self.timer_sessions.spawn(async move {
                let mut interval = interval(period);
                loop {
                    interval.tick().await;
                    let event = event.clone();
                    let event = move |state: &mut S, timer: &mut _| state.on_event(event, timer);
                    SendEvent::send(&mut sender, SessionEvent::Timer(timer_id, Box::new(event)))?
                }
            });
            self.timer_handles.insert(timer_id, handle);
            Ok(timer_id)
        }

        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
            Session::unset(self, timer_id)
        }
    }

    impl<S: ?Sized> Session<S> {
        fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()> {
            self.timer_handles
                .remove(&timer_id)
                .ok_or(anyhow::anyhow!("timer not exists"))?
                .abort();
            Ok(())
        }
    }

    // JoinSet should automatically abort remaining task on drop
    // impl<S: ?Sized> Drop for Session<S> {
    //     fn drop(&mut self) {
    //         while let Some(timer_id) = self.timers.keys().next() {
    //             self.unset(*timer_id).unwrap()
    //         }
    //     }
    // }
}
