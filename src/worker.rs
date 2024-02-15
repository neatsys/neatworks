use std::fmt::Debug;

use tokio::{
    runtime::{self, RuntimeFlavor},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

use crate::event::SendEvent;

// any explicit support for async work i.e. Pin<Box<dyn Future<...> + ...>>?
// currently it probably can be supported with erased Work i.e.
// Work<tokio's Runtime, tokio's Sender>, move async block into closure, spawn
// a task with the runtime that captures it and a cloned sender, await it, then
// pass reply message(s) through the sender
// there's no way to propagate errors from detacked tasks though
// anyway, `Worker` is for parallelism. if the work is async for concurrency,
// directly working with `impl OnEvent`s is more reasonable
pub type Work<S, M> =
    Box<dyn FnOnce(&S, &mut dyn SendEvent<M>) -> anyhow::Result<()> + Send + Sync>;

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
        // println!("executor run");
        if runtime::Handle::current().runtime_flavor() != RuntimeFlavor::MultiThread {
            eprintln!("SpawnExecutor should be better run in multithread runtime")
        }
        loop {
            enum Select<S, E> {
                Recv(Work<S, E>),
                JoinNext(()),
            }
            if let Select::Recv(work) = tokio::select! {
                Some(result) = self.handles.join_next() => Select::JoinNext(result??),
                work = self.receiver.recv() => Select::Recv(work.ok_or(anyhow::anyhow!("channel closed"))?),
            } {
                // println!("work");
                let state = self.state.clone();
                let mut sender = sender.clone();
                self.handles.spawn(async move { work(&state, &mut sender) });
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Worker<S, M> {
    Spawn(SpawnWorker<S, M>),
    Null, // for testing
}

impl<S, M> Worker<S, M> {
    pub fn submit(&self, work: Work<S, M>) -> anyhow::Result<()> {
        match self {
            Self::Spawn(worker) => worker.submit(work),
            Self::Null => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpawnWorker<S, M>(UnboundedSender<Work<S, M>>);

impl<S, M> SpawnWorker<S, M> {
    fn submit(&self, work: Work<S, M>) -> anyhow::Result<()> {
        self.0
            .send(work)
            .map_err(|_| anyhow::anyhow!("receiver closed"))
    }
}

pub fn spawn_backend<S, M>(state: S) -> (Worker<S, M>, SpawnExecutor<S, M>) {
    let (sender, receiver) = unbounded_channel();
    let worker = SpawnWorker(sender);
    let executor = SpawnExecutor {
        receiver,
        state,
        handles: Default::default(),
    };
    (Worker::Spawn(worker), executor)
}

pub mod erased {
    use tokio::{
        sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        task::JoinSet,
    };

    pub type Work<S, E> = Box<dyn FnOnce(&S, &mut E) -> anyhow::Result<()> + Send + Sync>;

    #[derive(Debug)]
    pub struct SpawnExecutor<S, E: ?Sized> {
        state: S,
        receiver: UnboundedReceiver<Work<S, E>>,
        handles: JoinSet<anyhow::Result<()>>,
    }

    impl<S: Clone + Send + Sync + 'static, E: ?Sized + 'static> SpawnExecutor<S, E> {
        pub async fn run<F: Clone + Send + 'static>(
            &mut self,
            sender: F,
            as_sender: impl FnMut(&mut F) -> &mut E + Clone + Send + 'static,
        ) -> anyhow::Result<()> {
            loop {
                enum Select<S, E: ?Sized> {
                    Recv(Work<S, E>),
                    JoinNext(()),
                }
                if let Select::Recv(work) = tokio::select! {
                    Some(result) = self.handles.join_next() => Select::JoinNext(result??),
                    work = self.receiver.recv() => Select::Recv(work.ok_or(anyhow::anyhow!("channel closed"))?),
                } {
                    let state = self.state.clone();
                    let mut sender = sender.clone();
                    let mut as_sender = as_sender.clone();
                    self.handles
                        .spawn(async move { work(&state, as_sender(&mut sender)) });
                }
            }
        }
    }

    #[derive(Debug)]
    pub enum Worker<S, E: ?Sized> {
        Inline(InlineWorker<S, E>),
        Spawn(SpawnWorker<S, E>),
        Null, // for testing
    }

    impl<S, E: ?Sized> Worker<S, E> {
        pub fn new_inline(state: S, sender: Box<E>) -> Self {
            Self::Inline(InlineWorker(state, sender))
        }

        pub fn submit(&mut self, work: Work<S, E>) -> anyhow::Result<()> {
            match self {
                Self::Inline(worker) => worker.submit(work),
                Self::Spawn(worker) => worker.submit(work),
                Self::Null => Ok(()),
            }
        }
    }

    #[derive(Debug)]
    pub struct InlineWorker<S, E: ?Sized>(S, Box<E>);

    impl<S, E: ?Sized> InlineWorker<S, E> {
        fn submit(&mut self, work: Work<S, E>) -> anyhow::Result<()> {
            work(&self.0, &mut self.1)
        }
    }

    #[derive(Debug, Clone)]
    pub struct SpawnWorker<S, E: ?Sized>(UnboundedSender<Work<S, E>>);

    impl<S, E: ?Sized> SpawnWorker<S, E> {
        fn submit(&self, work: Work<S, E>) -> anyhow::Result<()> {
            self.0
                .send(work)
                .map_err(|_| anyhow::anyhow!("receiver closed"))
        }
    }

    pub fn spawn_backend<S, E: ?Sized>(state: S) -> (Worker<S, E>, SpawnExecutor<S, E>) {
        let (sender, receiver) = unbounded_channel();
        let worker = SpawnWorker(sender);
        let executor = SpawnExecutor {
            receiver,
            state,
            handles: Default::default(),
        };
        (Worker::Spawn(worker), executor)
    }
}
