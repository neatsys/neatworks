use std::{fmt::Debug, hash::Hash, net::SocketAddr};

pub trait Addr: Debug + Clone + Eq + Ord + Hash + Send + Sync + 'static {}

impl Addr for u8 {}
impl Addr for SocketAddr {}

pub mod events {
    pub struct Send<A, M>(pub A, pub M);

    pub struct Recv<M>(pub M);
}
