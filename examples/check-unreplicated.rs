use std::{thread::available_parallelism, time::Duration};

use augustus::{
    app::kvstore::{static_workload, InfinitePutGet, Op, Result},
    message::Payload,
    search::{breadth_first, random_depth_first, Settings},
    unreplicated::check::{DryState, State},
    workload::{Check, DryRecorded, Iter, Recorded, Workload},
};
use rand::thread_rng;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> anyhow::Result<()> {
    println!("* Single client; Put, Append, Get");
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
        state.clone(),
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

    println!("* Multi-client different keys");
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
        state.clone(),
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

    println!("* Multi-client same key");
    let mut state = State::new();
    for _ in 0..2 {
        state.push_client(Recorded::from(Iter(
            (0..3)
                .map(move |x| (Op::Append(String::from("foo"), x.to_string())))
                .map(|op| Ok(Payload(serde_json::to_vec(&op)?)))
                .collect::<anyhow::Result<Vec<_>>>()?
                .into_iter(),
        )))?
    }
    state.launch()?;

    fn append_linearizable<W: Workload>(state: &State<Recorded<W>>) -> anyhow::Result<()> {
        let mut all_results = Vec::new();
        for client in &state.clients {
            for (op, result) in &client.close_loop.workload.invocations {
                let Op::Append(_, append_value) = serde_json::from_slice::<Op>(op)? else {
                    anyhow::bail!("unexpected {op:?}")
                };
                let Result::AppendResult(value) = serde_json::from_slice::<Result>(result)? else {
                    anyhow::bail!("unexpected {result:?}")
                };
                if !value.ends_with(&append_value) {
                    anyhow::bail!("{op:?} get {result:?}")
                }
                all_results.push(value)
            }
        }
        all_results.sort_unstable_by_key(|s| s.len());
        for window in all_results.windows(2) {
            let [prev_value, value] = window else {
                unreachable!()
            };
            if !value.starts_with(prev_value) {
                anyhow::bail!("{value} inconsistent with {prev_value}")
            }
        }
        Ok(())
    }

    let settings = Settings {
        invariant: append_linearizable,
        goal: |state: &State<_>| state.clients.iter().all(|client| client.close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState<DryRecorded<()>>, _, _, _>(
        state.clone(),
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
    let result = breadth_first::<_, DryState<DryRecorded<()>>, _, _, _>(
        state,
        settings,
        1.try_into().unwrap(),
        None,
    )?;
    println!("{result:?}");

    println!("* Infinite workload searches (with 2 clients)");
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
        state.clone(),
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
