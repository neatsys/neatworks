use std::fmt::Debug;

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

use crate::event::SendEvent;

// any explicit support for async work i.e. Pin<Box<dyn Future<...> + ...>>?
// currently it has been "kind of" supported by e.g.
// Work<tokio's Runtime, tokio's Sender>, move async block into closure, spawn
// a task with the runtime that captures it and a cloned sender, await it, then
// pass reply message(s) through the sender
// it's only kind of supported because currently `dyn SendEvent<M>` actually
// cannot be cloned (and probably never will since some impl literally cannot),
// so we will need to abstract it (back) away and let user specify their desired
// constratins for a second `&mut E` parameter of the closure e.g.
// `SendEvent<M> + Clone + Send + 'static`
// also, construct the async task locally/eagerly before submission may also
// expose some differences, not sure
pub type Work<S, M> =
    Box<dyn FnOnce(&S, &mut dyn SendEvent<M>) -> anyhow::Result<()> + Send + Sync>;

pub trait Work2<S, M> {
    // TODO allow implementors to specify additional requirements on `E`
    fn execute<E>(self, state: &S, sender: &mut E) -> anyhow::Result<()>;
}

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
                let mut sender = sender.clone();
                self.handles.spawn(async move { work(&state, &mut sender) });
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

pub fn spawn_backend<S, M>(state: S) -> (SpawnWorker<S, M>, SpawnExecutor<S, M>) {
    let (sender, receiver) = unbounded_channel();
    let worker = SpawnWorker(sender);
    let executor = SpawnExecutor {
        receiver,
        state,
        handles: Default::default(),
    };
    (worker, executor)
}
