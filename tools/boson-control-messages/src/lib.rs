use std::net::{IpAddr, SocketAddr};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct CopsServer {
    pub addrs: Vec<SocketAddr>,
    pub id: u8,
    pub variant: CopsVariant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CopsClient {
    pub addrs: Vec<SocketAddr>,
    pub ip: IpAddr,
    pub num_concurrent: usize,
    pub variant: CopsVariant,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CopsVariant {
    Untrusted,
    Replicated(CopsReplicated),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CopsReplicated {
    pub num_faulty: usize,
}
