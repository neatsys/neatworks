use std::time::Duration;

use augustus::{
    app::{KVStoreOp, KVStoreResult},
    replication::Payload,
    search::{breadth_first, Settings, State as _},
    unreplicated::check::{DryState, State},
};
use rand::{distributions::Alphanumeric, rngs::StdRng, thread_rng, Rng, SeedableRng};

fn main() -> anyhow::Result<()> {
    let mut state = State::new();
    state.push_client(
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
        .into_iter()
        .map(|(op, result)| {
            (
                Payload(serde_json::to_vec(&op).unwrap()),
                Some(Payload(serde_json::to_vec(&result).unwrap())),
            )
        }),
    )?;
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
        state.push_client(
            (0..3)
                .map(move |x| {
                    (
                        KVStoreOp::Append(format!("KEY-{i}"), x.to_string()),
                        KVStoreResult::AppendResult(
                            (0..=x).map(|x| x.to_string()).collect::<Vec<_>>().concat(),
                        ),
                    )
                })
                .map(|(op, result)| {
                    (
                        Payload(serde_json::to_vec(&op).unwrap()),
                        Some(Payload(serde_json::to_vec(&result).unwrap())),
                    )
                }),
        )?
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

    #[derive(Clone)]
    struct InfinitePutGet {
        rng: StdRng,
        values: [String; 5],
        should_get: bool,
    }
    impl Iterator for InfinitePutGet {
        type Item = (Payload, Option<Payload>);
        fn next(&mut self) -> Option<Self::Item> {
            let index = self.rng.gen_range(0..5);
            let (op, result) = if self.should_get {
                (
                    KVStoreOp::Get(format!("KEY-{index}")),
                    KVStoreResult::GetResult(self.values[index].clone()),
                )
            } else {
                let value = (&mut self.rng)
                    .sample_iter(Alphanumeric)
                    .take(8)
                    .map(char::from)
                    .collect::<String>();
                self.values[index] = value.clone();
                (
                    KVStoreOp::Put(format!("KEY-{index}"), value),
                    KVStoreResult::PutOk,
                )
            };
            Some((
                Payload(serde_json::to_vec(&op).unwrap()),
                Some(Payload(serde_json::to_vec(&result).unwrap())),
            ))
        }
    }
    let mut state = State::new();
    state.push_client(InfinitePutGet {
        rng: StdRng::from_rng(thread_rng())?,
        values: Default::default(),
        should_get: false,
    })?;
    state.launch()?;
    let settings = Settings {
        invariant: |_: &_| Ok(()),
        goal: |_: &_| false,
        prune: |_: &_| false,
        max_depth: None,
    };
    let result = breadth_first::<_, DryState, _, _, _>(
        state,
        settings,
        1.try_into().unwrap(),
        Duration::from_secs(15),
    )?;
    println!("{result:?}");

    Ok(())
}
