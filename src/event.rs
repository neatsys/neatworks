pub mod session;

use std::{fmt::Debug, time::Duration};

use tokio::sync::mpsc::UnboundedSender;

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

impl<N: Into<M>, M> SendEvent<N> for UnboundedSender<M> {
    fn send(&mut self, event: N) -> anyhow::Result<()> {
        UnboundedSender::send(self, event.into()).map_err(|_| anyhow::anyhow!("channel closed"))
    }
}

pub type TimerId = u32;

pub trait Timer<M> {
    fn set_dyn(
        &mut self,
        duration: Duration,
        event: Box<dyn FnMut() -> M + Send>,
    ) -> anyhow::Result<TimerId>;

    fn set(
        &mut self,
        duration: Duration,
        event: impl FnMut() -> M + Send + 'static,
    ) -> anyhow::Result<TimerId>
    where
        Self: Sized,
    {
        self.set_dyn(duration, Box::new(event))
    }

    fn unset(&mut self, timer_id: TimerId) -> anyhow::Result<()>;
}

impl<M> dyn Timer<M> + '_ {
    pub fn set<N: Into<M>>(
        &mut self,
        duration: Duration,
        mut event: impl FnMut() -> N + Send + 'static,
    ) -> anyhow::Result<u32> {
        self.set_dyn(duration, Box::new(move || event().into()))
    }
}

pub use session::Session;
pub type Sender<M> = session::SessionSender<M>;

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
