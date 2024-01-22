use std::fmt::Debug;

use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

use crate::event::SendEvent;

pub type Work<S, M> = Box<dyn FnOnce(&S, &dyn SendEvent<M>) -> anyhow::Result<()> + Send + Sync>;

#[derive(Debug)]
pub struct SpawnExecutor<S, M> {
    state: S,
    receiver: UnboundedReceiver<Work<S, M>>,
    handles: JoinSet<anyhow::Result<()>>,
}

impl<S, M> SpawnExecutor<S, M> {
    pub async fn run(
        &mut self,
        sender: impl SendEvent<M> + Clone + Send + 'static,
    ) -> anyhow::Result<()>
    where
        S: Clone + Send + Sync + 'static,
        M: 'static,
    {
        loop {
            enum Select<S, E> {
                Recv(Work<S, E>),
                JoinNext(()),
            }
            if let Select::Recv(work) = tokio::select! {
                Some(result) = self.handles.join_next() => Select::JoinNext(result??),
                work = self.receiver.recv() => Select::Recv(work.ok_or(anyhow::anyhow!("channel closed"))?),
            } {
                let state = self.state.clone();
                let sender = sender.clone();
                self.handles.spawn(async move { work(&state, &sender) });
            }
        }
    }
}

pub type Worker<S, M> = SpawnWorker<S, M>;

#[derive(Debug, Clone)]
pub struct SpawnWorker<S, M>(UnboundedSender<Work<S, M>>);

impl<S, M> SpawnWorker<S, M> {
    pub fn submit(&self, work: Work<S, M>) -> anyhow::Result<()> {
        self.0
            .send(work)
            .map_err(|_| anyhow::anyhow!("receiver closed"))
    }
}
