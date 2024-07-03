use std::marker::PhantomData;

use bincode::Options;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

use crate::event::SendEvent;

use super::{
    events::{Recv, Send},
    Addr,
};

#[derive(Debug)]
pub struct Forward<A, N>(pub A, pub N);

impl<A: Addr, N: SendEvent<Send<A, M>>, M> SendEvent<Send<(), M>> for Forward<A, N> {
    fn send(&mut self, Send((), message): Send<(), M>) -> anyhow::Result<()> {
        self.1.send(Send(self.0.clone(), message))
    }
}

pub struct BincodeEncode<N>(pub N);

impl<N: SendEvent<Send<A, Bytes>>, A, M: Serialize> SendEvent<Send<A, M>> for BincodeEncode<N> {
    fn send(&mut self, Send(remote, message): Send<A, M>) -> anyhow::Result<()> {
        let encoded = bincode::options().serialize(&message)?;
        self.0.send(Send(remote, encoded.into()))
    }
}

pub struct BincodeDecode<M, E>(pub E, PhantomData<M>);

impl<E: SendEvent<Recv<M>>, M: DeserializeOwned> SendEvent<Recv<Bytes>> for BincodeDecode<M, E> {
    fn send(&mut self, Recv(message): Recv<Bytes>) -> anyhow::Result<()> {
        let decoded = bincode::options()
            .allow_trailing_bytes()
            .deserialize(&message)?;
        self.0.send(Recv(decoded))
    }
}
