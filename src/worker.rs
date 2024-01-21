use std::fmt::Debug;

use tokio::task::JoinSet;

use crate::event::SessionSender;

pub type Work<S, M> = Box<dyn FnOnce(&S, &SessionSender<M>) -> anyhow::Result<()> + Send + Sync>;

#[derive(Debug)]
pub struct SpawnExecutor<S, M> {
    state: S,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Work<S, M>>,
    handles: JoinSet<anyhow::Result<()>>,
}

impl<S, M> SpawnExecutor<S, M> {
    pub async fn run(&mut self, sender: SessionSender<M>) -> anyhow::Result<()>
    where
        M: Send + 'static,
        S: Clone + Send + Sync + 'static,
    {
        loop {
            enum Select<S, M> {
                Recv(Work<S, M>),
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

#[derive(Debug, Clone)]
pub struct SpawnWorker<S, M>(tokio::sync::mpsc::UnboundedSender<Work<S, M>>);
