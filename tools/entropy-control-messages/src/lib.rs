use std::{net::IpAddr, num::NonZeroUsize, time::Duration};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartPeersConfig {
    pub ips: Vec<IpAddr>,
    pub ip_index: usize,
    pub num_peer_per_ip: usize,

    pub fragment_len: u32,
    pub chunk_k: NonZeroUsize,
    pub chunk_n: NonZeroUsize,
    pub chunk_m: NonZeroUsize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerUrl {
    Ipfs(String),
    Entropy(String, usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutConfig {
    pub chunk_len: u32,
    pub k: NonZeroUsize,
    pub peer_urls: Vec<Vec<PeerUrl>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutResult {
    pub digest: [u8; 32],
    pub chunks: Vec<(u32, String)>,
    pub latency: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfig {
    pub chunk_len: u32,
    pub k: NonZeroUsize,
    pub chunks: Vec<(u32, String)>,
    pub peer_urls: Vec<PeerUrl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetResult {
    pub digest: [u8; 32],
    pub latency: Duration,
}
