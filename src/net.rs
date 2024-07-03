use std::{fmt::Debug, hash::Hash, net::SocketAddr};

use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

use crate::event::SendEvent;

pub mod combinators;
pub mod task {
    pub mod udp;
}

pub trait Addr:
    Debug + Clone + Eq + Ord + Hash + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

impl Addr for u8 {}
impl Addr for SocketAddr {}

pub mod events {
    pub struct Send<A, M>(pub A, pub M);

    pub struct Recv<M>(pub M);
}

pub fn send_bytes(
    buf: &[u8],
    mut sender: impl SendEvent<events::Recv<Bytes>>,
) -> anyhow::Result<()> {
    sender.send(events::Recv(Bytes::copy_from_slice(buf)))
}
