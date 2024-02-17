use std::{collections::HashMap, fmt::Debug, time::Duration};

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::{AbortHandle, JoinError, JoinSet},
    time::interval,
};

use super::{OnEvent, SendEvent, Sender, Timer, TimerId};

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
    pub fn sender(&self) -> Sender<M> {
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
    fn set_dyn(
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
