use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::task::JoinHandle;

pub trait SendEvent<M> {
    fn send(&self, event: M) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct SessionSender<M>(tokio::sync::mpsc::UnboundedSender<Option<M>>);

impl<M: Into<N>, N> SendEvent<M> for SessionSender<N> {
    fn send(&self, event: M) -> anyhow::Result<()> {
        self.0
            .send(Some(event.into()))
            .map_err(|_| anyhow::anyhow!("receiver closed"))
    }
}

#[derive(Debug)]
pub struct Session<M> {
    sender: tokio::sync::mpsc::UnboundedSender<Option<M>>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Option<M>>,
    timer_id: u32,
    timers: HashMap<u32, JoinHandle<()>>,
    timeouts: Arc<Mutex<Vec<(u32, M)>>>,
}

impl<M> Session<M> {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        Self {
            sender,
            receiver,
            timer_id: 0,
            timers: Default::default(),
            timeouts: Default::default(),
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

    pub async fn run(
        &mut self,
        mut on_event: impl FnMut(M, TimerEngine<'_, M>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        loop {
            let event = self
                .receiver
                .recv()
                .await
                .ok_or(anyhow::anyhow!("channel closed"))?;
            // TODO can/should poison error be propogated instead?
            let mut timeouts = self.timeouts.lock().unwrap();
            for (timer_id, event) in timeouts.drain(..) {
                self.timers.remove(&timer_id).unwrap();
                on_event(
                    event,
                    TimerEngine {
                        timer_id: &mut self.timer_id,
                        timers: &mut self.timers,
                        timeouts: self.timeouts.clone(),
                        sender: self.sender.clone(),
                    },
                )?
            }
            if let Some(event) = event {
                on_event(
                    event,
                    TimerEngine {
                        timer_id: &mut self.timer_id,
                        timers: &mut self.timers,
                        timeouts: self.timeouts.clone(),
                        sender: self.sender.clone(),
                    },
                )?
            }
            // explicitly drop mutex guard here to prevent bugs by future refactor
            drop(timeouts)
        }
    }
}

#[derive(Debug)]
pub struct TimerEngine<'a, M> {
    timer_id: &'a mut u32,
    timers: &'a mut HashMap<u32, JoinHandle<()>>,
    timeouts: Arc<Mutex<Vec<(u32, M)>>>,
    sender: tokio::sync::mpsc::UnboundedSender<Option<M>>,
}

impl<M> TimerEngine<'_, M> {
    pub fn set(&mut self, duration: Duration, event: impl Into<M>) -> u32
    where
        M: Send + 'static,
    {
        *self.timer_id += 1;
        let timer_id = *self.timer_id;
        let event = event.into();
        let timeouts = self.timeouts.clone();
        let sender = self.sender.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            timeouts.lock().unwrap().push((timer_id, event));
            sender.send(None).unwrap();
        });
        self.timers.insert(timer_id, timer);
        timer_id
    }

    pub fn unset(&mut self, timer_id: u32) -> anyhow::Result<()> {
        self.timers
            .remove(&timer_id)
            .ok_or(anyhow::anyhow!("timer not exists"))?
            .abort();
        Ok(())
    }
}
