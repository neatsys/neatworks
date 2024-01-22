use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub throughput: f32,
    pub latency: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Protocol {
    Unreplicated,
    Pbft,
}
