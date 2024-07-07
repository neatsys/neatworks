use bytes::Bytes;
use derive_where::derive_where;

use crate::{
    event::SendEvent,
    workload::{
        events::{Invoke, InvokeOk},
        App,
    },
};

#[derive_where(Debug; A)]
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
