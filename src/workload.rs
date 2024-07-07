use bytes::Bytes;
use events::{Invoke, InvokeOk};

use crate::event::{combinators::Map, SendEvent};

pub mod events {
    #[derive(Debug)]
    pub struct Invoke<M>(pub M);

    #[derive(Debug)]
    pub struct InvokeOk<M>(pub M);
}

pub mod app {
    pub mod combinators;
    pub mod kvstore;
}

pub mod combinators;

pub trait App {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Bytes>;
}

#[derive(Debug)]
pub struct Null;

impl App for Null {
    fn execute(&mut self, _: &[u8]) -> anyhow::Result<Bytes> {
        Ok(Default::default())
    }
}

pub struct Typed<O, R, A> {
    pub decode: fn(&[u8]) -> anyhow::Result<O>,
    pub encode: fn(&R) -> anyhow::Result<Bytes>,
    pub inner: A,
}

impl<O, R, A> App for Typed<O, R, A>
where
    for<'a, 'b> (&'a mut A, &'b mut Option<InvokeOk<R>>): SendEvent<Invoke<O>>,
{
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Bytes> {
        let op = (self.decode)(op)?;
        let mut response = None;
        (&mut self.inner, &mut response).send(Invoke(op))?;
        let Some(InvokeOk(result)) = response.take() else {
            anyhow::bail!("missing execution result")
        };
        (self.encode)(&result)
    }
}

pub trait Workload {
    type Op;
    type Result;

    fn init(&mut self, sender: impl SendEvent<Self::Op>) -> anyhow::Result<()>;

    fn on_result(
        &mut self,
        result: Self::Result,
        sender: impl SendEvent<Self::Op>,
    ) -> anyhow::Result<()>;
}

pub struct CloseLoop<W, E> {
    pub workload: W,
    pub sender: E,
}

impl<W, E> CloseLoop<W, E> {
    pub fn new(workload: W, sender: E) -> Self {
        Self { workload, sender }
    }
}

impl<W: Workload, E: SendEvent<Invoke<W::Op>>> CloseLoop<W, E> {
    pub fn init(&mut self) -> anyhow::Result<()> {
        self.workload.init(Map(events::Invoke, &mut self.sender))
    }
}

impl<W: Workload, E: SendEvent<Invoke<W::Op>>> SendEvent<InvokeOk<W::Result>> for CloseLoop<W, E> {
    fn send(&mut self, InvokeOk(result): InvokeOk<W::Result>) -> anyhow::Result<()> {
        self.workload
            .on_result(result, Map(events::Invoke, &mut self.sender))
    }
}
