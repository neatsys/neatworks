use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

use crate::event::SendEvent;

pub type Work<S, E> = Box<dyn FnOnce(&S, &mut E) -> anyhow::Result<()> + Send + Sync>;

pub trait Submit<S, E: ?Sized> {
    fn submit(&mut self, work: Work<S, E>) -> anyhow::Result<()>;
}

impl<W: ?Sized + Submit<S, E>, S, E: ?Sized> Submit<S, E> for Box<W> {
    fn submit(&mut self, work: Work<S, E>) -> anyhow::Result<()> {
        W::submit(self, work)
    }
}

#[derive(Debug, Clone)]
pub enum Worker<S, E> {
    Null,
    Inline(S, E),
    Send(UnboundedSender<Work<S, E>>),
}

// TODO fix the double dispatching of Box<dyn Submit<...>>
impl<S, E> Submit<S, E> for Worker<S, E> {
    fn submit(&mut self, work: Work<S, E>) -> anyhow::Result<()> {
        match self {
            Self::Null => Ok(()),
            Self::Inline(state, emit) => work(state, emit),
            Self::Send(sender) => sender.send(work),
        }
    }
}

#[derive(Debug)]
pub struct SpawnExecutor<S, E> {
    receiver: UnboundedReceiver<Work<S, E>>,
    handles: JoinSet<anyhow::Result<()>>,
}

impl<S: Clone + Send + 'static, E: Clone + Send + 'static> SpawnExecutor<S, E> {
    pub async fn run(&mut self, state: S, emit: E) -> anyhow::Result<()> {
        loop {
            enum Select<S, E> {
                Recv(Work<S, E>),
                JoinNext(()),
            }
            if let Select::Recv(work) = tokio::select! {
                Some(result) = self.handles.join_next() => Select::JoinNext(result??),
                work = self.receiver.recv() => Select::Recv(work.ok_or(anyhow::anyhow!("channel closed"))?),
            } {
                let state = state.clone();
                let mut emit = emit.clone();
                self.handles.spawn(async move { work(&state, &mut emit) });
            }
        }
    }
}

pub fn spawn_backend<S, E>() -> (Worker<S, E>, SpawnExecutor<S, E>) {
    let (sender, receiver) = unbounded_channel();
    let executor = SpawnExecutor {
        receiver,
        handles: Default::default(),
    };
    (Worker::Send(sender), executor)
}
