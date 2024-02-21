use augustus::{
    app::{KVStoreOp, KVStoreResult},
    replication::Payload,
    search::{breadth_first, Settings, State as _},
    unreplicated::check::{DryState, State},
};

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
        invariant: |_: &State<_>| Ok(()),
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
        invariant: |_: &State<_>| Ok(()),
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

    Ok(())
}

// fn check_result<I>(
//     close_loop: &CloseLoop<I>,
//     index: usize,
//     expected_result: KVStoreResult,
// ) -> anyhow::Result<()> {
//     let invocations = close_loop
//         .invocations
//         .as_ref()
//         .ok_or(anyhow::anyhow!("result not recorded"))?;
//     if let Some((_, result)) = invocations.get(index) {
//         let result = serde_json::from_slice::<KVStoreResult>(result)?;
//         if result != expected_result {
//             anyhow::bail!("expect {expected_result:?} get {result:?}")
//         }
//     }
//     Ok(())
// }
