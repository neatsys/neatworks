use augustus::{
    app::{KVStoreOp, KVStoreResult},
    replication::CloseLoop,
    search::{breadth_first, Settings, State as _},
    unreplicated::check::{DryState, State},
};

fn main() -> anyhow::Result<()> {
    let mut state = State::new(1);
    state.push_workload(
        [
            KVStoreOp::Put(String::from("foo"), String::from("bar")),
            KVStoreOp::Append(String::from("foo"), String::from("baz")),
            KVStoreOp::Get(String::from("foo")),
        ]
        .into_iter()
        .map(|op| serde_json::to_vec(&op).unwrap()),
    )?;
    state.launch()?;

    let settings = Settings {
        invariant: |state: &State<_>| {
            state.close_loops.iter().try_for_each(|close_loop| {
                check_result(close_loop, 0, KVStoreResult::PutOk)?;
                check_result(
                    close_loop,
                    1,
                    KVStoreResult::AppendResult(String::from("barbaz")),
                )?;
                check_result(
                    close_loop,
                    2,
                    KVStoreResult::GetResult(String::from("barbaz")),
                )?;
                Ok(())
            })
        },
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

fn check_result<I>(
    close_loop: &CloseLoop<I>,
    index: usize,
    expected_result: KVStoreResult,
) -> anyhow::Result<()> {
    let invocations = close_loop
        .invocations
        .as_ref()
        .ok_or(anyhow::anyhow!("result not recorded"))?;
    if let Some((_, result)) = invocations.get(index) {
        let result = serde_json::from_slice::<KVStoreResult>(result)?;
        if result != expected_result {
            anyhow::bail!("expect {expected_result:?} get {result:?}")
        }
    }
    Ok(())
}
