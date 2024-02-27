use std::{thread::available_parallelism, time::Duration};

use augustus::{
    app::kvstore::{static_workload, InfinitePutGet, Op, Result},
    search::{breadth_first, random_depth_first, Settings, State as _},
    unreplicated::check::{DryState, State},
    workload::Check,
};
use rand::thread_rng;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
    let mut state = State::new();
    state.push_client(static_workload(
        [
            (
                Op::Put(String::from("foo"), String::from("bar")),
                Result::PutOk,
            ),
            (
                Op::Append(String::from("foo"), String::from("baz")),
                Result::AppendResult(String::from("barbaz")),
            ),
            (
                Op::Get(String::from("foo")),
                Result::GetResult(String::from("barbaz")),
            ),
        ]
        .into_iter(),
    )?)?;
    state.launch()?;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| state.clients.iter().all(|client| client.close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState<_>, _, _, _>(
        state.duplicate()?,
        settings.clone(),
        1.try_into().unwrap(),
        None,
    )?;
    println!("{result:?}");

    let settings = Settings {
        invariant: settings.invariant,
        goal: |_: &_| false,
        prune: settings.goal,
        max_depth: None,
    };
    let result =
        breadth_first::<_, DryState<_>, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    let mut state = State::new();
    for i in 0..2 {
        state.push_client(static_workload((0..3).map(move |x| {
            (
                Op::Append(format!("KEY-{i}"), x.to_string()),
                Result::AppendResult((0..=x).map(|x| x.to_string()).collect::<Vec<_>>().concat()),
            )
        }))?)?
    }
    state.launch()?;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| state.clients.iter().all(|client| client.close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState<_>, _, _, _>(
        state.duplicate()?,
        settings.clone(),
        1.try_into().unwrap(),
        None,
    )?;
    println!("{result:?}");

    let settings = Settings {
        invariant: settings.invariant,
        goal: |_: &_| false,
        prune: settings.goal,
        max_depth: None,
    };
    let result =
        breadth_first::<_, DryState<_>, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    let mut state = State::new();
    state.push_client(Check::new(InfinitePutGet::new("KEY1", &mut thread_rng())?))?;
    state.push_client(Check::new(InfinitePutGet::new("KEY2", &mut thread_rng())?))?;
    state.launch()?;
    let mut settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |_: &_| false,
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState<()>, _, _, _>(
        state.duplicate()?,
        settings.clone(),
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");
    settings.max_depth = Some(1000.try_into().unwrap());
    let result = random_depth_first::<_, DryState<()>, _, _, _>(
        state,
        settings,
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");

    Ok(())
}
