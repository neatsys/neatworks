use std::{thread::available_parallelism, time::Duration};

use neatworks::{
    codec::{Decode, Encode},
    model::search::{breadth_first, random_depth_first, Settings},
    unreplicated::model::{ClientLocalContext, State},
    workload::{
        app::kvstore::{
            self, InfinitePutGet,
            Op::{Append, Get, Put},
            Result::{AppendResult, GetResult, PutOk},
        },
        combinators::{Iter, Record, UncheckedIter},
        Workload,
    },
};
use rand::thread_rng;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    println!("* Single client; Put, Append, Get");
    let mut state = State::new();
    state.push_client(Iter::new([
        (Put(String::from("foo"), String::from("bar")), PutOk),
        (
            Append(String::from("foo"), String::from("baz")),
            AppendResult(String::from("barbaz")),
        ),
        (Get(String::from("foo")), GetResult(String::from("barbaz"))),
    ]));
    state.init()?;

    type O = kvstore::Op;
    type R = kvstore::Result;
    type TypedW<W> = Decode<R, Encode<O, W>>;
    type C<W> = ClientLocalContext<TypedW<W>>;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| {
            state
                .clients
                .iter()
                .all(|(_, context): &(_, C<Iter<_, _>>)| context.upcall.workload.done)
        },
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first(state.clone(), settings.clone(), 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    let settings = Settings {
        invariant: settings.invariant,
        goal: |_: &_| false,
        prune: settings.goal,
        max_depth: None,
    };
    let result = breadth_first(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    println!("* Multi-client different keys");
    let mut state = State::new();
    for i in 0..2 {
        state.push_client(Iter::new((0..3).map(move |x| {
            (
                Append(format!("KEY-{i}"), x.to_string()),
                AppendResult((0..=x).map(|x| x.to_string()).collect::<Vec<_>>().concat()),
            )
        })))
    }
    state.init()?;

    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |state: &State<_>| {
            state
                .clients
                .iter()
                .all(|(_, context): &(_, C<Iter<_, _>>)| context.upcall.workload.done)
        },
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first(state.clone(), settings.clone(), 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    let settings = Settings {
        invariant: settings.invariant,
        goal: |_: &_| false,
        prune: settings.goal,
        max_depth: None,
    };
    let result = breadth_first(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    println!("* Multi-client same key");
    let mut state = State::new();
    for _ in 0..2 {
        state.push_client(Record::new(UncheckedIter::new(
            (0..3).map(move |x| (Append(String::from("foo"), x.to_string()))),
        )))
    }
    state.init()?;

    fn append_linearizable<W: Workload<Op = O, Result = R>>(
        state: &State<TypedW<Record<W, O, R>>>,
    ) -> anyhow::Result<()> {
        let mut all_results = Vec::new();
        for (_, context) in &state.clients {
            for (op, result) in &context.upcall.workload.invocations {
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
        goal: |state: &State<_>| {
            state
                .clients
                .iter()
                .all(|(_, context): &(_, C<Record<UncheckedIter<_, _>, _, _>>)| {
                    context.upcall.workload.done
                })
        },
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first(state.clone(), settings.clone(), 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    let settings = Settings {
        invariant: settings.invariant,
        goal: |_: &_| false,
        prune: settings.goal,
        max_depth: None,
    };
    let result = breadth_first(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    println!("* Infinite workload searches (with 2 clients)");
    let mut state = State::new();
    state.push_client(Iter::new(InfinitePutGet::new("KEY1", &mut thread_rng())?));
    state.push_client(Iter::new(InfinitePutGet::new("KEY2", &mut thread_rng())?));
    state.init()?;
    let mut settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |_: &_| false,
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first(
        state.clone(),
        settings.clone(),
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");
    settings.max_depth = Some(1000.try_into().unwrap());
    let result = random_depth_first(
        state,
        settings,
        available_parallelism()?,
        // 1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");

    Ok(())
}

// cSpell:ignore barbaz
