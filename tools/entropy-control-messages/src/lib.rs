use std::net::IpAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartPeersConfig {
    pub ips: Vec<IpAddr>,
    pub ip_index: usize,
    pub num_peer_per_ip: usize,
}
