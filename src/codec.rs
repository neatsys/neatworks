use bincode::Options;
use bytes::Bytes;
use derive_more::Deref;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    event::SendEvent,
    net::events::Send,
    workload::events::{Invoke, InvokeOk},
};

pub struct Encode<M, T>(fn(&M) -> anyhow::Result<Bytes>, pub T);

impl<M: Into<L>, L, N: SendEvent<Send<A, Bytes>>, A> SendEvent<Send<A, M>> for Encode<L, N> {
    fn send(&mut self, Send(remote, message): Send<A, M>) -> anyhow::Result<()> {
        let encoded = (self.0)(&message.into())?;
        self.1.send(Send(remote, encoded))
    }
}

// only encode for result and decode for op
impl<M, E: SendEvent<InvokeOk<Bytes>>> SendEvent<InvokeOk<M>> for Encode<M, E> {
    fn send(&mut self, InvokeOk(result): InvokeOk<M>) -> anyhow::Result<()> {
        let encoded = (self.0)(&result)?;
        self.1.send(InvokeOk(encoded))
    }
}

fn bincode_encode<M: Serialize>(message: &M) -> anyhow::Result<Bytes> {
    bincode::options()
        .serialize(message)
        .map(Into::into)
        .map_err(Into::into)
}

impl<M: Serialize, N> Encode<M, N> {
    pub fn bincode(inner: N) -> Self {
        Self(bincode_encode, inner)
    }
}

pub fn bincode_decode<M: DeserializeOwned>(buf: &[u8]) -> anyhow::Result<M> {
    bincode::options()
        .allow_trailing_bytes()
        .deserialize(buf)
        .map_err(Into::into)
}

pub fn send_invoke<M>(
    decode: fn(&[u8]) -> anyhow::Result<M>,
    mut sender: impl SendEvent<Invoke<M>>,
) -> impl FnMut(&[u8]) -> anyhow::Result<()> {
    move |buf| sender.send(Invoke(decode(buf)?))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deref, Serialize, Deserialize)]
pub struct Payload(pub Bytes);
