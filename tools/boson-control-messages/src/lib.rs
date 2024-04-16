use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MutexUntrusted {
    pub addrs: Vec<SocketAddr>,
    pub id: u8,
}
