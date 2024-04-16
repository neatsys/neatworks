use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Mutex {
    Untrusted(MutexUntrusted),
    Replicated(MutexReplicated),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MutexUntrusted {
    pub addrs: Vec<SocketAddr>,
    pub id: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MutexReplicated {
    pub addrs: Vec<SocketAddr>,
    pub client_addrs: Vec<SocketAddr>,
    pub id: u8,
    pub num_faulty: usize,
}
