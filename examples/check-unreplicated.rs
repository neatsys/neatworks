use augustus::{
    app::KVStoreOp,
    search::{breadth_first, Settings},
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
        invariant: |_: &_| Ok(()), // TODO
        goal: |state: &State<_>| state.close_loops.iter().all(|close_loop| close_loop.done),
        prune: |_: &_| false,
        max_depth: None,
    };
    let result =
        breadth_first::<_, DryState, _, _, _>(state, settings, 1.try_into().unwrap(), None)?;
    println!("{result:?}");

    Ok(())
}
