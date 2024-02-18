use crate::event::SendEvent;

// TODO find a use case for non-erased (type-preseved?) variant
// or just remove it at all. anyway no performance gain here
pub type Work<S, M> = erased::Work<S, dyn SendEvent<M>>;
pub type Worker<S, M> = erased::Worker<S, dyn SendEvent<M>>;

pub type SpawnExecutor<S, M> = erased::SpawnExecutor<S, dyn SendEvent<M>>;
pub type SpawnWorker<S, M> = erased::SpawnWorker<S, dyn SendEvent<M>>;

pub fn spawn_backend<S, M>(state: S) -> (Worker<S, M>, SpawnExecutor<S, M>) {
    erased::spawn_backend(state)
}

pub mod erased {
    use tokio::{
        sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        task::JoinSet,
    };

    // any explicit support for async work i.e. Pin<Box<dyn Future<...> + ...>>?
    // not tried, but probably can be done with e.g.
    // * use tokio runtime as context state
    // * use dyn SendEvent<_> + Send + 'static as sender, and instantiate with tokio sender
    // * submit work that spawn into runtime and capture the sender
    // it's not the recommended way though, since detached task does not propagate errors (at least
    // not in the most nature way), and any desire of concurrency should be encoded into
    // `impl OnEvent` directly

    // `E` is probably `dyn ...`, as in non-erased variant above and in the example
    // i have been thinking for a better interface for a while, however no clue, and also the
    // current design seems usable enough
    // it leaves few hacky feel though, especially the `SpawnExecutor::run` signature
    // so keep working on this if possible
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
            // this probably has a form of `|sender| sender`, for unsized coercion that turns `F`
            // into `E` which is probably `dyn ...` as mentioned above
            // i have been looking for a way to better hiding this conversion, i.e.
            // trait AsDyn<T which is a trait> {
            //   fn as_dyn(value: impl T) -> &mut dyn T;
            // }
            // this rank 2 polymorphism probably will never make it into Rust
            // in the end, this `|sender| sender` thing has been by far the solution with minimal
            // footprint
            // sign
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
