use bytes::Bytes;
use derive_more::Deref;
use derive_where::derive_where;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    event::SendEvent,
    net::events::Cast,
    workload::{
        events::{Invoke, InvokeOk},
        App, Workload,
    },
};

#[derive(Deref)]
#[derive_where(Debug, Clone, PartialEq, Eq, Hash; T)]
pub struct Encode<M, T>(fn(&M) -> anyhow::Result<Bytes>, #[deref] T);

impl<M: Into<L>, L, N: SendEvent<Cast<A, Bytes>>, A> SendEvent<Cast<A, M>> for Encode<L, N> {
    fn send(&mut self, Cast(remote, message): Cast<A, M>) -> anyhow::Result<()> {
        let encoded = (self.0)(&message.into())?;
        self.1.send(Cast(remote, encoded))
    }
}

impl<M, E: SendEvent<InvokeOk<Bytes>>> SendEvent<InvokeOk<M>> for Encode<M, E> {
    fn send(&mut self, InvokeOk(result): InvokeOk<M>) -> anyhow::Result<()> {
        let encoded = (self.0)(&result)?;
        self.1.send(InvokeOk(encoded))
    }
}

impl<R, A, O, E> SendEvent<Invoke<O>> for (&'_ mut Encode<R, A>, E)
where
    E: SendEvent<InvokeOk<Bytes>>,
    for<'a, 'b> (&'a mut A, Encode<R, &'b mut E>): SendEvent<Invoke<O>>,
{
    fn send(&mut self, event: Invoke<O>) -> anyhow::Result<()> {
        (&mut self.0 .1, Encode(self.0 .0, &mut self.1)).send(event)
    }
}

impl<M, E: SendEvent<Invoke<Bytes>>> SendEvent<Invoke<M>> for Encode<M, E> {
    fn send(&mut self, Invoke(result): Invoke<M>) -> anyhow::Result<()> {
        let encoded = (self.0)(&result)?;
        self.1.send(Invoke(encoded))
    }
}

impl<W: Workload> Workload for Encode<W::Op, W> {
    type Op = Bytes;
    type Result = W::Result;

    fn init(&mut self, sender: impl SendEvent<Invoke<Self::Op>>) -> anyhow::Result<()> {
        self.1.init(Encode(self.0, sender))
    }

    fn on_result(
        &mut self,
        result: InvokeOk<Self::Result>,
        sender: impl SendEvent<Invoke<Self::Op>>,
    ) -> anyhow::Result<()> {
        self.1.on_result(result, Encode(self.0, sender))
    }
}

#[derive(Deref)]
#[derive_where(Debug, Clone, PartialEq, Eq, Hash; T)]
pub struct Decode<O, T>(fn(&[u8]) -> anyhow::Result<O>, #[deref] T);

impl<O, A> App for Decode<O, A>
where
    for<'a, 'b> (&'a mut A, &'b mut Option<InvokeOk<Bytes>>): SendEvent<Invoke<O>>,
{
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Bytes> {
        let op = (self.0)(op)?;
        let mut response = None;
        (&mut self.1, &mut response).send(Invoke(op))?;
        let Some(InvokeOk(result)) = response.take() else {
            anyhow::bail!("missing execution result")
        };
        Ok(result)
    }
}

impl<W: Workload> Workload for Decode<W::Result, W> {
    type Op = W::Op;
    type Result = Bytes;

    fn init(&mut self, sender: impl SendEvent<Invoke<Self::Op>>) -> anyhow::Result<()> {
        self.1.init(sender)
    }

    fn on_result(
        &mut self,
        InvokeOk(result): InvokeOk<Self::Result>,
        sender: impl SendEvent<Invoke<Self::Op>>,
    ) -> anyhow::Result<()> {
        self.1.on_result(InvokeOk((self.0)(&result)?), sender)
    }
}

// TODO proper Debug impl
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deref, Serialize, Deserialize)]
pub struct Payload(pub Bytes);

pub mod bincode {
    use bincode::Options as _;
    use bytes::Bytes;
    use serde::{de::DeserializeOwned, Serialize};

    pub fn encode<M: Serialize>(message: &M) -> anyhow::Result<Bytes> {
        bincode::options()
            .serialize(message)
            .map(Into::into)
            .map_err(Into::into)
    }

    pub fn decode<M: DeserializeOwned>(buf: &[u8]) -> anyhow::Result<M> {
        bincode::options()
            .allow_trailing_bytes()
            .deserialize(buf)
            .map_err(Into::into)
    }
}

pub mod json {
    use bytes::Bytes;
    use serde::{de::DeserializeOwned, Serialize};

    pub fn encode<M: Serialize>(message: &M) -> anyhow::Result<Bytes> {
        serde_json::to_vec(message)
            .map(Into::into)
            .map_err(Into::into)
    }

    pub fn decode<M: DeserializeOwned>(buf: &[u8]) -> anyhow::Result<M> {
        serde_json::from_slice(buf).map_err(Into::into)
    }
}

impl<M: Serialize, T> Encode<M, T> {
    pub fn bincode(inner: T) -> Self {
        Self(bincode::encode, inner)
    }

    pub fn json(inner: T) -> Self {
        Self(json::encode, inner)
    }
}

impl<M: DeserializeOwned, T> Decode<M, T> {
    pub fn bincode(inner: T) -> Self {
        Self(bincode::decode, inner)
    }

    pub fn json(inner: T) -> Self {
        Self(json::decode, inner)
    }
}
