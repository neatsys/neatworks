use bytes::Bytes;
use events::{Invoke, InvokeOk};

use crate::event::{combinators::Transient, SendEvent};

pub mod events {
    #[derive(Debug)]
    pub struct Invoke<M>(pub M);

    #[derive(Debug)]
    pub struct InvokeOk<M>(pub M);
}

pub mod app {
    pub mod kvstore;
}

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
    for<'a, 'b> (&'a mut A, &'b mut Transient<InvokeOk<R>>): SendEvent<Invoke<O>>,
{
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Bytes> {
        let op = (self.decode)(op)?;
        let mut response = Transient::new();
        (&mut self.inner, &mut response).send(Invoke(op))?;
        let Some(InvokeOk(result)) = response.pop() else {
            anyhow::bail!("missing execution result")
        };
        anyhow::ensure!(response.is_empty());
        (self.encode)(&result)
    }
}
