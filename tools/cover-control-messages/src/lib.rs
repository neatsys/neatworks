use std::{
    net::{IpAddr, SocketAddr},
    ops::Range,
};

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Mutex {
    pub addrs: Vec<SocketAddr>,
    pub id: u8,
    pub num_faulty: usize,
    pub variant: Variant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CopsServer {
    pub addrs: Vec<SocketAddr>,
    pub id: u8,
    pub record_count: usize,
    pub variant: Variant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CopsClient {
    pub addrs: Vec<SocketAddr>,
    pub ip: IpAddr,
    pub num_concurrent: usize, // per instance
    pub put_ratio: f64,
    pub record_count: usize,
    pub put_range: Range<usize>, // probably redundant to `record_count` and `index`?
    pub variant: Variant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Variant {
    Untrusted,
    Replicated(Replicated),
    Quorum(Quorum),
    NitroEnclaves,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replicated {
    pub num_faulty: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quorum {
    pub addrs: Vec<SocketAddr>,
    pub num_faulty: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuorumServer {
    pub quorum: Quorum,
    pub index: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CopsClientOk(#[serde(with = "histogram_serialization")] pub Histogram<u64>);

#[derive(Debug, Serialize, Deserialize)]
pub struct Microbench {
    pub variant: Variant,
    pub ip: IpAddr,
    pub num_concurrent: Option<usize>,
}

mod histogram_serialization {
    use hdrhistogram::{
        serialization::{Deserializer, Serializer, V2Serializer},
        Histogram,
    };

    pub fn serialize<S: serde::Serializer>(
        histogram: &Histogram<u64>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let mut buf = Vec::new();
        V2Serializer::new()
            .serialize(histogram, &mut buf)
            .map_err(serde::ser::Error::custom)?;
        s.serialize_bytes(&buf)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Histogram<u64>, D::Error> {
        let buf = <Vec<u8> as serde::Deserialize<'de>>::deserialize(d)?;
        Deserializer::new()
            .deserialize(&mut &*buf)
            .map_err(serde::de::Error::custom)
    }
}
