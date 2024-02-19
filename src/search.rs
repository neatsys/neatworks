use std::{
    hash::Hash,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Barrier, Condvar, Mutex,
    },
    thread::available_parallelism,
    time::{Duration, Instant},
};

use crossbeam_queue::SegQueue;
use dashmap::DashMap;

pub trait State: Clone {
    type Event;

    fn events(&self) -> Vec<Self::Event>;

    fn step(&mut self, event: Self::Event) -> anyhow::Result<()>;

    fn steps(&self) -> Vec<anyhow::Result<Self>> {
        self.events()
            .into_iter()
            .map(|event| {
                let mut system = self.clone();
                system.step(event)?;
                Ok(system)
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Settings<I, G, P> {
    invariant: I,
    goal: G,
    prune: P,
    max_depth: Option<NonZeroUsize>,
}

pub enum SearchResult<S, T, E> {
    Error(Vec<(E, Arc<T>)>, E, anyhow::Error),
    InvariantViolation(Vec<(E, Arc<T>)>, anyhow::Error),
    GoalFound(S),
    SpaceExhausted,
}

enum SearchWorkerResult<S, E> {
    Error(S, E, anyhow::Error),
    InvariantViolation(S, anyhow::Error),
    GoalFound(S),
    SpaceExhausted,
}

pub fn bfs<S, T, I, G, P>(
    initial_state: S,
    settings: Settings<I, G, P>,
) -> anyhow::Result<SearchResult<S, T, S::Event>>
where
    S: State + Into<T> + Send + 'static,
    S::Event: Clone + Send + Sync,
    T: Eq + Hash + Send + Sync + 'static,
    I: Fn(&S) -> anyhow::Result<()> + Clone + Send + 'static,
    G: Fn(&S) -> bool + Clone + Send + 'static,
    P: Fn(&S) -> bool + Clone + Send + 'static,
{
    let num_worker = available_parallelism()?.get();

    let discovered = Arc::new(DashMap::new());
    let queue = Arc::new(SegQueue::new());
    let pushing_queue = Arc::new(SegQueue::new());
    let depth = Arc::new(AtomicUsize::new(0));
    let depth_barrier = Arc::new(Barrier::new(num_worker));
    let search_finished = Arc::new((Mutex::new(None), Condvar::new()));

    let initial_pure_state = Arc::new(initial_state.clone().into());
    queue.push((initial_state, initial_pure_state.clone()));
    discovered.insert(
        initial_pure_state,
        StateInfo {
            prev: None,
            depth: 0,
        },
    );

    let mut workers = Vec::new();
    for _ in 0..num_worker {
        let settings = settings.clone();
        let discovered = discovered.clone();
        let queue = queue.clone();
        let pushing_queue = pushing_queue.clone();
        let depth = depth.clone();
        let depth_barrier = depth_barrier.clone();
        let search_finished = search_finished.clone();
        workers.push(std::thread::spawn(move || {
            bfs_worker(
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
                        "Explored: {}, Depth {} ({:.2}s, {:.2}K states/s",
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

    // TODO timeout
    let result = search_finished.0.lock().unwrap();
    let mut result = search_finished
        .1
        .wait_while(result, |result| result.is_none())
        .unwrap();
    let result = result.take().unwrap();
    for worker in workers {
        worker.join().unwrap();
    }
    status_worker.join().unwrap();

    let result = match result {
        SearchWorkerResult::Error(state, event, err) => {
            SearchResult::Error(trace(&discovered, Arc::new(state.into())), event, err)
        }
        SearchWorkerResult::InvariantViolation(state, err) => {
            SearchResult::InvariantViolation(trace(&discovered, Arc::new(state.into())), err)
        }
        SearchWorkerResult::GoalFound(state) => SearchResult::GoalFound(state),
        SearchWorkerResult::SpaceExhausted => SearchResult::SpaceExhausted,
    };
    Ok(result)
}

fn status_worker<S, E>(
    status: impl Fn(Duration) -> String,
    search_finished: Arc<(Mutex<Option<SearchWorkerResult<S, E>>>, Condvar)>,
) {
    let start = Instant::now();
    let mut result = search_finished.0.lock().unwrap();
    let mut wait_result;
    while {
        (result, wait_result) = search_finished
            .1
            .wait_timeout_while(result, Duration::from_secs(5), |result| result.is_none())
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

fn bfs_worker<S: State, T, I, G, P>(
    settings: Settings<I, G, P>,
    discovered: Arc<DashMap<Arc<T>, StateInfo<T, S::Event>>>,
    mut queue: Arc<SegQueue<(S, Arc<T>)>>,
    mut pushing_queue: Arc<SegQueue<(S, Arc<T>)>>,
    depth: Arc<AtomicUsize>,
    depth_barrier: Arc<Barrier>,
    search_finished: Arc<(Mutex<Option<SearchWorkerResult<S, S::Event>>>, Condvar)>,
) where
    S: Into<T>,
    S::Event: Clone,
    T: Eq + Hash,
    I: Fn(&S) -> anyhow::Result<()>,
    G: Fn(&S) -> bool,
    P: Fn(&S) -> bool,
{
    let search_finish = |result| {
        search_finished.0.lock().unwrap().get_or_insert(result);
        search_finished.1.notify_all()
    };
    let mut local_depth = 0;
    loop {
        'depth: while let Some((state, pure_state)) = queue.pop() {
            if let Err(err) = (settings.invariant)(&state) {
                search_finish(SearchWorkerResult::InvariantViolation(state, err));
                break;
            }
            if (settings.goal)(&state) {
                search_finish(SearchWorkerResult::GoalFound(state));
                break;
            }
            for event in state.events() {
                let mut next_state = state.clone();
                if let Err(err) = next_state.step(event.clone()) {
                    search_finish(SearchWorkerResult::Error(state, event, err));
                    break 'depth;
                }
                let pure_next_state = Arc::new(next_state.clone().into());
                // do not replace a previously-found state, which may be reached with a shorter
                // trace from initial state
                let mut inserted = false;
                discovered
                    .entry(pure_next_state.clone())
                    .or_insert_with(|| {
                        inserted = true;
                        StateInfo {
                            prev: Some((event, pure_state.clone())),
                            depth: local_depth + 1,
                        }
                    });
                if inserted
                    && Some(local_depth + 1) < settings.max_depth.map(Into::into)
                    && !(settings.prune)(&next_state)
                {
                    pushing_queue.push((next_state, pure_next_state))
                }
            }
        }
        // even if the above loop breaks, this wait always blocks every worker
        // so that if some worker first block here, then other worker `search_finish()`, the former
        // worker does not stuck here
        let wait_result = depth_barrier.wait();
        if search_finished.0.lock().unwrap().is_some() {
            break;
        }

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
}
