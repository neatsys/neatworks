use bincode::Options;
use bytes::Bytes;
use derive_more::Deref;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{event::SendEvent, net::events::Send};

pub struct Encode<M, N>(fn(&M) -> anyhow::Result<Bytes>, pub N);

impl<M, N: SendEvent<Send<A, Bytes>>, A> SendEvent<Send<A, M>> for Encode<M, N> {
    fn send(&mut self, Send(remote, message): Send<A, M>) -> anyhow::Result<()> {
        let encoded = (self.0)(&message)?;
        self.1.send(Send(remote, encoded))
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deref, Serialize, Deserialize)]
pub struct Payload(pub Bytes);
