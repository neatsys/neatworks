use std::collections::BTreeMap;

pub trait State {
    type Event;

    fn events(&self) -> Vec<Self::Event>;

    fn step(&mut self, event: Self::Event) -> anyhow::Result<()>;

    fn fix(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub struct Network<A, M, T> {
    pub messages: BTreeMap<A, M>,
    pub timers: BTreeMap<A, T>,
}
