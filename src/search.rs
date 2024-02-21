use std::{
    fmt::Debug,
    hash::Hash,
    iter::{repeat, repeat_with},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering::SeqCst},
        Arc, Barrier, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use rand::{seq::SliceRandom, thread_rng};

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
    Err(Vec<(E, T)>, E, anyhow::Error),
    InvariantViolation(Vec<(E, T)>, anyhow::Error),
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

pub fn breadth_first<S, T, I, G, P>(
    initial_state: S,
    settings: Settings<I, G, P>,
    num_worker: NonZeroUsize,
    max_duration: impl Into<Option<Duration>>,
) -> anyhow::Result<SearchResult<S, T, S::Event>>
where
    S: State + Into<T> + Send + 'static,
    S::Event: Clone + Send + Sync,
    T: Clone + Eq + Hash + Send + Sync + 'static,
    I: Fn(&S) -> anyhow::Result<()> + Clone + Send + 'static,
    G: Fn(&S) -> bool + Clone + Send + 'static,
    P: Fn(&S) -> bool + Clone + Send + 'static,
{
    let discovered = Arc::new(DashMap::new());
    let queue = Arc::new(SegQueue::new());
    let pushing_queue = Arc::new(SegQueue::new());
    let depth = Arc::new(AtomicUsize::new(0));
    let depth_barrier = Arc::new(Barrier::new(num_worker.get()));
    let search_finished = Arc::new((Mutex::new(None), Condvar::new(), AtomicBool::new(false)));

    let initial_dry_state = Arc::new(initial_state.duplicate()?.into());
    queue.push((initial_state, initial_dry_state.clone()));
    discovered.insert(
        initial_dry_state,
        StateInfo {
            prev: None,
            depth: 0,
        },
    );

    let result = search_internal(
        max_duration,
        repeat({
            let discovered = discovered.clone();
            let depth = depth.clone();
            let search_finished = search_finished.clone();
            move || {
                breath_first_worker(
                    settings,
                    discovered,
                    queue,
                    pushing_queue,
                    depth,
                    depth_barrier,
                    search_finished,
                )
            }
        })
        .take(num_worker.get()),
        {
            let discovered = discovered.clone();
            move |elapsed| {
                format!(
                    "Explored: {}, Depth {} ({:.2}s, {:.2}K states/s)",
                    discovered.len(),
                    depth.load(SeqCst),
                    elapsed.as_secs_f32(),
                    discovered.len() as f32 / elapsed.as_secs_f32() / 1000.
                )
            }
        },
        search_finished,
    )?;

    let Some(result) = result else {
        return Ok(SearchResult::Timeout);
    };
    let result = match result {
        SearchWorkerResult::Error(state, event, err) => {
            SearchResult::Err(trace(&discovered, state.into()), event, err)
        }
        SearchWorkerResult::InvariantViolation(state, err) => {
            SearchResult::InvariantViolation(trace(&discovered, state.into()), err)
        }
        SearchWorkerResult::GoalFound(state) => SearchResult::GoalFound(state),
        SearchWorkerResult::SpaceExhausted => SearchResult::SpaceExhausted,
    };
    Ok(result)
}

pub fn random_depth_first<S, T, I, G, P>(
    initial_state: S,
    settings: Settings<I, G, P>,
    num_worker: NonZeroUsize,
    max_duration: impl Into<Option<Duration>>,
) -> anyhow::Result<SearchResult<S, T, S::Event>>
where
    S: State + Into<T> + Send + 'static,
    S::Event: Clone + Send + Sync,
    T: Eq + Hash + Send + Sync + 'static,
    I: Fn(&S) -> anyhow::Result<()> + Clone + Send + 'static,
    G: Fn(&S) -> bool + Clone + Send + 'static,
    P: Fn(&S) -> bool + Clone + Send + 'static,
{
    let num_probe = Arc::new(AtomicU32::new(0));
    let num_state = Arc::new(AtomicU32::new(0));
    let search_finished = Arc::new((Mutex::new(None), Condvar::new(), AtomicBool::new(false)));

    let initial_state = initial_state.duplicate()?;
    let result = search_internal(
        max_duration,
        {
            let num_probe = num_probe.clone();
            let num_state = num_state.clone();
            let search_finished = search_finished.clone();
            repeat_with(move || {
                let settings = settings.clone();
                let initial_state = initial_state.duplicate().unwrap();
                let num_probe = num_probe.clone();
                let num_state = num_state.clone();
                let search_finished = search_finished.clone();
                move || {
                    random_depth_first_worker(
                        settings,
                        initial_state,
                        num_probe,
                        num_state,
                        search_finished,
                    )
                }
            })
            .take(num_worker.get())
        },
        move |elasped| {
            format!(
                "Explored: {}, Num Probes: {} ({:.2}s, {:.2}K explored/s)",
                num_state.load(SeqCst),
                num_probe.load(SeqCst),
                elasped.as_secs_f32(),
                num_state.load(SeqCst) as f32 / elasped.as_secs_f32() / 1000.
            )
        },
        search_finished,
    )?;
    Ok(result.unwrap_or(SearchResult::Timeout))
}

fn search_internal<R: Send + 'static, F: FnOnce() + Send + 'static>(
    max_duration: impl Into<Option<Duration>>,
    workers: impl Iterator<Item = F>,
    status: impl Fn(Duration) -> String + Send + 'static,
    search_finished: SearchFinished<R>,
) -> anyhow::Result<Option<R>>
where
{
    let max_duration = max_duration.into();

    let mut worker_tasks = Vec::new();
    for worker in workers {
        worker_tasks.push(std::thread::spawn(worker))
    }
    let status_worker = std::thread::spawn({
        let search_finished = search_finished.clone();
        move || status_worker(status, search_finished)
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
    std::thread::sleep(Duration::from_millis(20));
    search_finished.2.store(true, SeqCst);
    search_finished.1.notify_all();
    // println!("search finished");
    for worker in worker_tasks {
        worker.join().map_err(|_| anyhow::anyhow!("worker panic"))?
    }
    // println!("worker joined");
    status_worker
        .join()
        .map_err(|_| anyhow::anyhow!("status worker panic"))?;
    // println!("status worker joined");
    Ok(result)
}

type SearchFinished<R> = Arc<(Mutex<Option<R>>, Condvar, AtomicBool)>;

fn status_worker<R>(status: impl Fn(Duration) -> String, search_finished: SearchFinished<R>) {
    let start = Instant::now();
    let mut result = search_finished.0.lock().unwrap();
    let mut wait_result;
    while {
        (result, wait_result) = search_finished
            .1
            .wait_timeout_while(result, Duration::from_secs(5), |_| {
                !search_finished.2.load(SeqCst)
            })
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

fn trace<T: Eq + Hash + Clone, E: Clone>(
    discovered: &DashMap<Arc<T>, StateInfo<T, E>>,
    target: T,
) -> Vec<(E, T)> {
    let info = discovered.get(&target).unwrap();
    let Some((prev_event, prev_state)) = &info.prev else {
        return Vec::new();
    };
    let mut trace = trace(discovered, T::clone(prev_state));
    trace.push((prev_event.clone(), target));
    trace
}

enum SearchWorkerResult<S, E> {
    Error(S, E, anyhow::Error),
    InvariantViolation(S, anyhow::Error),
    GoalFound(S),
    SpaceExhausted,
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
    // T: Debug,
    // S::Event: Debug,
{
    let search_finish = |result| {
        search_finished.0.lock().unwrap().get_or_insert(result);
        search_finished.2.store(true, SeqCst);
        search_finished.1.notify_all()
    };
    let mut local_depth = 0;
    loop {
        // println!("start depth {local_depth}");
        'depth: while let Some((state, dry_state)) = queue.pop() {
            // TODO check initial state
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
                if !inserted {
                    continue;
                }
                // println!("check invariant");
                if let Err(err) = (settings.invariant)(&next_state) {
                    search_finish(SearchWorkerResult::InvariantViolation(next_state, err));
                    break 'depth;
                }
                // println!("check goal");
                if (settings.goal)(&next_state) {
                    search_finish(SearchWorkerResult::GoalFound(next_state));
                    break 'depth;
                }
                if Some(local_depth + 1) != settings.max_depth.map(Into::into)
                    && !(settings.prune)(&next_state)
                {
                    pushing_queue.push((next_state, next_dry_state))
                }
            }
            if search_finished.2.load(SeqCst) {
                break;
            }
        }
        // println!("end depth {local_depth} pushed {}", pushing_queue.len());

        // even if the above loop breaks, this wait always traps every worker
        // so that if some worker trap here first, then other worker `search_finish()`, the former
        // worker does not stuck here
        let wait_result = depth_barrier.wait();
        // println!("barrier");
        if search_finished.2.load(SeqCst) {
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

fn random_depth_first_worker<S: State, T, I, G, P>(
    settings: Settings<I, G, P>,
    initial_state: S,
    num_probe: Arc<AtomicU32>,
    num_state: Arc<AtomicU32>,
    search_finished: SearchFinished<SearchResult<S, T, S::Event>>,
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
        search_finished.2.store(true, SeqCst);
        search_finished.1.notify_all()
    };
    while !search_finished.2.load(SeqCst) {
        num_probe.fetch_add(1, SeqCst);
        let mut state = initial_state.duplicate().unwrap();
        let mut trace = Vec::new();
        // TODO check initial state
        for depth in 0.. {
            let events = state.events();
            let Some(event) = events.choose(&mut thread_rng()).cloned() else {
                break;
            };
            if let Err(err) = state.step(event.clone()) {
                search_finish(SearchResult::Err(trace, event, err));
                break;
            }
            num_state.fetch_add(1, SeqCst);
            trace.push((event, state.duplicate().unwrap().into()));
            if let Err(err) = (settings.invariant)(&state) {
                search_finish(SearchResult::InvariantViolation(trace, err));
                break;
            }
            // highly unpractical
            // effectively monkey-typing an OSDI paper
            if (settings.goal)(&state) {
                search_finish(SearchResult::GoalFound(state));
                break;
            }
            if (settings.prune)(&state)
                || Some(depth + 1) == settings.max_depth.map(Into::into)
                || search_finished.2.load(SeqCst)
            {
                break;
            }
        }
    }
}
