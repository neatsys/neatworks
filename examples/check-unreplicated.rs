use std::{thread::available_parallelism, time::Duration};

use augustus::{
    app::{
        kvstore::{static_workload, InfinitePutGet},
        KVStoreOp, KVStoreResult,
    },
    search::{breadth_first, random_depth_first, Settings, State as _},
    unreplicated::check::{DryState, State},
};
use rand::thread_rng;

fn main() -> anyhow::Result<()> {
    let mut state = State::new();
    state.push_client(static_workload(
        [
            (
                KVStoreOp::Put(String::from("foo"), String::from("bar")),
                KVStoreResult::PutOk,
            ),
            (
                KVStoreOp::Append(String::from("foo"), String::from("baz")),
                KVStoreResult::AppendResult(String::from("barbaz")),
            ),
            (
                KVStoreOp::Get(String::from("foo")),
                KVStoreResult::GetResult(String::from("barbaz")),
            ),
        ]
        .into_iter(),
    )?)?;
    state.launch()?;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| state.close_loops.iter().all(|close_loop| close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState, _, _, _>(
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
        breadth_first::<_, DryState, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    let mut state = State::new();
    for i in 0..2 {
        state.push_client(static_workload((0..3).map(move |x| {
            (
                KVStoreOp::Append(format!("KEY-{i}"), x.to_string()),
                KVStoreResult::AppendResult(
                    (0..=x).map(|x| x.to_string()).collect::<Vec<_>>().concat(),
                ),
            )
        }))?)?
    }
    state.launch()?;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| state.close_loops.iter().all(|close_loop| close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState, _, _, _>(
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
        breadth_first::<_, DryState, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    let mut state = State::new();
    state.push_client(InfinitePutGet::new("KEY1", &mut thread_rng())?)?;
    state.push_client(InfinitePutGet::new("KEY2", &mut thread_rng())?)?;
    state.launch()?;
    let mut settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |_: &_| false,
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState, _, _, _>(
        state.duplicate()?,
        settings.clone(),
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");
    settings.max_depth = Some(1000.try_into().unwrap());
    let result = random_depth_first::<_, DryState, _, _, _>(
        state,
        settings,
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");

    Ok(())
}
