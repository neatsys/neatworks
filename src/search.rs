use std::{
    fmt::Debug,
    hash::Hash,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Barrier, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use crossbeam_queue::SegQueue;
use dashmap::DashMap;

pub trait State {
    type Event;

    fn events(&self) -> Vec<Self::Event>;

    fn duplicate(&self) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, event: Self::Event) -> anyhow::Result<()>;

    fn steps(&self) -> Vec<anyhow::Result<Self>>
    where
        Self: Sized,
    {
        self.events()
            .into_iter()
            .map(|event| {
                let mut system = self.duplicate()?;
                system.step(event)?;
                Ok(system)
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Settings<I, G, P> {
    pub invariant: I,
    pub goal: G,
    pub prune: P,
    pub max_depth: Option<NonZeroUsize>,
}

pub enum SearchResult<S, T, E> {
    Err(Vec<(E, Arc<T>)>, E, anyhow::Error),
    InvariantViolation(Vec<(E, Arc<T>)>, anyhow::Error),
    GoalFound(S),
    SpaceExhausted,
    Timeout,
}

impl<S, T, E> Debug for SearchResult<S, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Err(_, _, err) => write!(f, "Err({err:?}"),
            Self::InvariantViolation(_, err) => write!(f, "InvariantViolation({err:?})"),
            Self::GoalFound(_) => write!(f, "GoalFound"),
            Self::SpaceExhausted => write!(f, "SpaceExhausted"),
            Self::Timeout => write!(f, "Timeout"),
        }
    }
}

enum SearchWorkerResult<S, E> {
    Error(S, E, anyhow::Error),
    InvariantViolation(S, anyhow::Error),
    GoalFound(S),
    SpaceExhausted,
}

pub fn breadth_first<S, T, I, G, P>(
    initial_state: S,
    settings: Settings<I, G, P>,
    num_worker: NonZeroUsize,
    max_duration: Option<Duration>,
) -> anyhow::Result<SearchResult<S, T, S::Event>>
where
    S: State + Into<T> + Send + 'static,
    S::Event: Clone + Send + Sync,
    T: Eq + Hash + Send + Sync + 'static,
    I: Fn(&S) -> anyhow::Result<()> + Clone + Send + 'static,
    G: Fn(&S) -> bool + Clone + Send + 'static,
    P: Fn(&S) -> bool + Clone + Send + 'static,

    T: Debug,
    S::Event: Debug,
{
    let discovered = Arc::new(DashMap::new());
    let queue = Arc::new(SegQueue::new());
    let pushing_queue = Arc::new(SegQueue::new());
    let depth = Arc::new(AtomicUsize::new(0));
    let depth_barrier = Arc::new(Barrier::new(num_worker.get()));
    let search_finished = Arc::new((Mutex::new(None), Condvar::new()));

    let initial_dry_state = Arc::new(initial_state.duplicate()?.into());
    queue.push((initial_state, initial_dry_state.clone()));
    discovered.insert(
        initial_dry_state,
        StateInfo {
            prev: None,
            depth: 0,
        },
    );

    let mut workers = Vec::new();
    for _ in 0..num_worker.get() {
        let settings = settings.clone();
        let discovered = discovered.clone();
        let queue = queue.clone();
        let pushing_queue = pushing_queue.clone();
        let depth = depth.clone();
        let depth_barrier = depth_barrier.clone();
        let search_finished = search_finished.clone();
        workers.push(std::thread::spawn(move || {
            breath_first_worker(
                settings,
                discovered,
                queue,
                pushing_queue,
                depth,
                depth_barrier,
                search_finished,
            )
        }))
    }
    let status_worker = std::thread::spawn({
        let search_finished = search_finished.clone();
        let discovered = discovered.clone();
        move || {
            status_worker(
                |elapsed| {
                    format!(
                        "Explored: {}, Depth {} ({:.2}s, {:.2}K states/s)",
                        discovered.len(),
                        depth.load(SeqCst),
                        elapsed.as_secs_f32(),
                        discovered.len() as f32 / elapsed.as_secs_f32() / 1000.
                    )
                },
                search_finished,
            )
        }
    });

    let result = search_finished
        .0
        .lock()
        .map_err(|_| anyhow::anyhow!("posioned"))?;
    let result = if let Some(max_duration) = max_duration {
        search_finished
            .1
            .wait_timeout_while(result, max_duration, |result| result.is_none())
            .map_err(|_| anyhow::anyhow!("posioned"))?
            .0
            .take()
    } else {
        search_finished
            .1
            .wait_while(result, |result| result.is_none())
            .map_err(|_| anyhow::anyhow!("posioned"))?
            .take()
    };
    // println!("search finished");
    for worker in workers {
        worker.join().map_err(|_| anyhow::anyhow!("worker panic"))?
    }
    // println!("worker joined");
    // safe fallback for waking status worker
    std::thread::sleep(Duration::from_millis(20));
    search_finished.1.notify_all();
    status_worker
        .join()
        .map_err(|_| anyhow::anyhow!("status worker panic"))?;
    // println!("status worker joined");

    let Some(result) = result else {
        return Ok(SearchResult::Timeout);
    };
    let result = match result {
        SearchWorkerResult::Error(state, event, err) => {
            SearchResult::Err(trace(&discovered, Arc::new(state.into())), event, err)
        }
        SearchWorkerResult::InvariantViolation(state, err) => {
            SearchResult::InvariantViolation(trace(&discovered, Arc::new(state.into())), err)
        }
        SearchWorkerResult::GoalFound(state) => SearchResult::GoalFound(state),
        SearchWorkerResult::SpaceExhausted => SearchResult::SpaceExhausted,
    };
    Ok(result)
}

type SearchFinished<R> = Arc<(Mutex<Option<R>>, Condvar)>;

fn status_worker<R>(status: impl Fn(Duration) -> String, search_finished: SearchFinished<R>) {
    let start = Instant::now();
    let mut result = search_finished.0.lock().unwrap();
    let mut wait_result;
    while {
        (result, wait_result) = search_finished
            .1
            .wait_timeout(result, Duration::from_secs(5))
            .unwrap();
        wait_result.timed_out()
    } {
        println!("{}", status(start.elapsed()))
    }
    println!("{}", status(start.elapsed()))
}

struct StateInfo<T, E> {
    prev: Option<(E, Arc<T>)>,
    #[allow(unused)]
    depth: usize, // to assert trace correctness?
}

fn trace<T: Eq + Hash, E: Clone>(
    discovered: &DashMap<Arc<T>, StateInfo<T, E>>,
    target: Arc<T>,
) -> Vec<(E, Arc<T>)> {
    let info = discovered.get(&target).unwrap();
    let Some(prev) = &info.prev else {
        return Vec::new();
    };
    let mut trace = trace(discovered, prev.1.clone());
    trace.push((prev.0.clone(), target));
    trace
}

type Discovered<T, E> = Arc<DashMap<Arc<T>, StateInfo<T, E>>>;

fn breath_first_worker<S: State, T, I, G, P>(
    settings: Settings<I, G, P>,
    discovered: Discovered<T, S::Event>,
    mut queue: Arc<SegQueue<(S, Arc<T>)>>,
    mut pushing_queue: Arc<SegQueue<(S, Arc<T>)>>,
    depth: Arc<AtomicUsize>,
    depth_barrier: Arc<Barrier>,
    search_finished: SearchFinished<SearchWorkerResult<S, S::Event>>,
) where
    S: Into<T>,
    S::Event: Clone,
    T: Eq + Hash,
    I: Fn(&S) -> anyhow::Result<()>,
    G: Fn(&S) -> bool,
    P: Fn(&S) -> bool,

    T: Debug,
    S::Event: Debug,
{
    let search_finish = |result| {
        search_finished.0.lock().unwrap().get_or_insert(result);
        search_finished.1.notify_all()
    };
    let mut local_depth = 0;
    loop {
        // println!("start depth {local_depth}");
        'depth: while let Some((state, dry_state)) = queue.pop() {
            // println!("check invariant");
            if let Err(err) = (settings.invariant)(&state) {
                search_finish(SearchWorkerResult::InvariantViolation(state, err));
                break;
            }
            // println!("check goal");
            if (settings.goal)(&state) {
                search_finish(SearchWorkerResult::GoalFound(state));
                break;
            }
            // println!("check events");
            for event in state.events() {
                // these duplication will probably never panic, since initial state duplication
                // already success
                let mut next_state = state.duplicate().unwrap();
                // println!("step {event:?}");
                if let Err(err) = next_state.step(event.clone()) {
                    search_finish(SearchWorkerResult::Error(state, event, err));
                    break 'depth;
                }
                let next_dry_state = Arc::new(next_state.duplicate().unwrap().into());
                // do not replace a previously-found state, which may be reached with a shorter
                // trace from initial state
                let mut inserted = false;
                discovered.entry(next_dry_state.clone()).or_insert_with(|| {
                    inserted = true;
                    StateInfo {
                        prev: Some((event, dry_state.clone())),
                        depth: local_depth + 1,
                    }
                });
                // println!("dry state {next_dry_state:?} inserted {inserted}");
                if inserted
                    && Some(local_depth + 1) != settings.max_depth.map(Into::into)
                    && !(settings.prune)(&next_state)
                {
                    pushing_queue.push((next_state, next_dry_state))
                }
            }
        }
        // println!("end depth {local_depth} pushed {}", pushing_queue.len());

        // even if the above loop breaks, this wait always traps every worker
        // so that if some worker trap here first, then other worker `search_finish()`, the former
        // worker does not stuck here
        let wait_result = depth_barrier.wait();
        // println!("barrier");
        if search_finished.0.lock().unwrap().is_some() {
            // println!("search result is some");
            break;
        }
        // println!("continue on next depth");

        local_depth += 1;
        if wait_result.is_leader() {
            depth.store(local_depth, SeqCst);
        }
        // one corner case: if some worker happen to perform empty check very late, and by that time
        // other workers already working on the next depth for a while and have exhausted the queue,
        // then the late worker will false positive report SpaceExhausted
        if pushing_queue.is_empty() || Some(local_depth) == settings.max_depth.map(Into::into) {
            search_finish(SearchWorkerResult::SpaceExhausted);
            break;
        }
        // i don't want to deal with that seriously, so just slow down the fast wakers a little bit
        std::thread::sleep(Duration::from_millis(10));
        (queue, pushing_queue) = (pushing_queue, queue)
    }
    // println!("worker exit");
}
