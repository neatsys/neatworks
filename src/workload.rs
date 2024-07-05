use crate::codec::Payload;

pub mod events {
    #[derive(Debug)]
    pub struct Invoke<M>(pub M);

    #[derive(Debug)]
    pub struct InvokeOk<M>(pub M);
}
pub trait App {
    fn execute(&mut self, op: events::Invoke<Payload>)
        -> anyhow::Result<events::InvokeOk<Payload>>;
}

#[derive(Debug)]
pub struct Null;

impl App for Null {
    fn execute(&mut self, _: events::Invoke<Payload>) -> anyhow::Result<events::InvokeOk<Payload>> {
        Ok(events::InvokeOk(Payload(Default::default())))
    }
}
