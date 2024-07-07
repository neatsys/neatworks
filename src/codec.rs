use bincode::Options;
use bytes::Bytes;
use derive_more::Deref;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    event::SendEvent,
    net::events::Cast,
    workload::{app::combinators::Typed, events::InvokeOk},
};

pub struct Encode<M, T>(fn(&M) -> anyhow::Result<Bytes>, pub T);

impl<M: Into<L>, L, N: SendEvent<Cast<A, Bytes>>, A> SendEvent<Cast<A, M>> for Encode<L, N> {
    fn send(&mut self, Cast(remote, message): Cast<A, M>) -> anyhow::Result<()> {
        let encoded = (self.0)(&message.into())?;
        self.1.send(Cast(remote, encoded))
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

impl<O: DeserializeOwned, R: Serialize, A> Typed<O, R, A> {
    pub fn bincode(app: A) -> Self {
        Self {
            encode: bincode_encode,
            decode: bincode_decode,
            inner: app,
        }
    }
}

fn json_encode<M: Serialize>(message: &M) -> anyhow::Result<Bytes> {
    serde_json::to_vec(message)
        .map(Into::into)
        .map_err(Into::into)
}

impl<M: Serialize, N> Encode<M, N> {
    pub fn json(inner: N) -> Self {
        Self(json_encode, inner)
    }
}

pub fn json_decode<M: DeserializeOwned>(buf: &[u8]) -> anyhow::Result<M> {
    serde_json::from_slice(buf).map_err(Into::into)
}

impl<O: DeserializeOwned, R: Serialize, A> Typed<O, R, A> {
    pub fn json(app: A) -> Self {
        Self {
            encode: json_encode,
            decode: json_decode,
            inner: app,
        }
    }
}

// TODO proper Debug impl
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deref, Serialize, Deserialize)]
pub struct Payload(pub Bytes);
