use std::{thread::available_parallelism, time::Duration};

use augustus::{
    app::kvstore::{
        self, InfinitePutGet,
        Op::{Append, Get, Put},
        Result::{AppendResult, GetResult, PutOk},
    },
    search::{breadth_first, random_depth_first, Settings},
    unreplicated::check::State,
    workload::{Check, Iter, Json, Recorded, Workload},
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
    state.push_client(Json(Check::new(
        [
            (Put(String::from("foo"), String::from("bar")), PutOk),
            (
                Append(String::from("foo"), String::from("baz")),
                AppendResult(String::from("barbaz")),
            ),
            (Get(String::from("foo")), GetResult(String::from("barbaz"))),
        ]
        .into_iter(),
    )));
    state.launch()?;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| state.clients.iter().all(|client| client.close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, State<_>, _, _, _>(
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
        breadth_first::<_, State<_>, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    println!("* Multi-client different keys");
    let mut state = State::new();
    for i in 0..2 {
        state.push_client(Json(Check::new((0..3).map(move |x| {
            (
                Append(format!("KEY-{i}"), x.to_string()),
                AppendResult((0..=x).map(|x| x.to_string()).collect::<Vec<_>>().concat()),
            )
        }))))
    }
    state.launch()?;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| state.clients.iter().all(|client| client.close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, State<_>, _, _, _>(
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
        breadth_first::<_, State<_>, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    println!("* Multi-client same key");
    let mut state = State::new();
    for _ in 0..2 {
        state.push_client(Json(Recorded::from(Iter::from(
            (0..3).map(move |x| (Append(String::from("foo"), x.to_string()))),
        ))))
    }
    state.launch()?;

    fn append_linearizable<W: Workload<Op = kvstore::Op, Result = kvstore::Result>>(
        state: &State<Json<Recorded<W>>>,
    ) -> anyhow::Result<()> {
        let mut all_results = Vec::new();
        for client in &state.clients {
            for (op, result) in &client.close_loop.workload.invocations {
                let Append(_, append_value) = op else {
                    anyhow::bail!("unexpected {op:?}")
                };
                let AppendResult(value) = result else {
                    anyhow::bail!("unexpected {result:?}")
                };
                anyhow::ensure!(value.ends_with(append_value), "{op:?} get {result:?}");
                all_results.push(value)
            }
        }
        all_results.sort_unstable_by_key(|s| s.len());
        for window in all_results.windows(2) {
            let [prev_value, value] = window else {
                unreachable!()
            };
            anyhow::ensure!(
                value.starts_with(*prev_value),
                "{value} inconsistent with {prev_value}"
            );
        }
        Ok(())
    }

    let settings = Settings {
        invariant: append_linearizable,
        goal: |state: &State<_>| state.clients.iter().all(|client| client.close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, State<_>, _, _, _>(
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
        breadth_first::<_, State<_>, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    println!("* Infinite workload searches (with 2 clients)");
    let mut state = State::new();
    state.push_client(Check::new(InfinitePutGet::new("KEY1", &mut thread_rng())?));
    state.push_client(Check::new(InfinitePutGet::new("KEY2", &mut thread_rng())?));
    state.launch()?;
    let mut settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |_: &_| false,
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, State<_>, _, _, _>(
        state.clone(),
        settings.clone(),
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");
    settings.max_depth = Some(1000.try_into().unwrap());
    let result = random_depth_first::<_, State<_>, _, _, _>(
        state,
        settings,
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");

    Ok(())
}

// cSpell:words jemalloc jemallocator tikv msvc kvstore unreplicated linearizable
// cSpell:ignore barbaz
