use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub throughput: f32,
    pub latency: Duration,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Protocol {
    Unreplicated,
    Pbft,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum App {
    Null,
    Ycsb(Ycsb),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ycsb {
    pub profile: YcsbProfile,
    pub backend: YcsbBackend,
    pub record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum YcsbProfile {
    A,
    B,
    C,
    D,
    E,
    F,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum YcsbBackend {
    BTree,
    Sqlite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub protocol: Protocol,
    pub app: App,
    pub num_close_loop: usize,
    pub replica_addrs: Vec<SocketAddr>,
    pub num_replica: usize,
    pub num_faulty: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub protocol: Protocol,
    pub app: App,
    pub replica_id: u8,
    pub replica_addrs: Vec<SocketAddr>,
    pub num_replica: usize,
    pub num_faulty: usize,
}
