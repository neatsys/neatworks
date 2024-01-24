use std::net::SocketAddr;

use primitive_types::U256;
use secp256k1::PublicKey;
use serde::{Deserialize, Serialize};

// 32 bytes array refers to sha256 digest by default in this codebase
// consider new types if necessary
// (hopefully no demand on digest-based routing...)
pub type PeerId = [u8; 32];
pub type Target = [u8; 32];

const BITS: usize = 256;

fn distance(peer_id: &PeerId, target: &Target) -> U256 {
    U256::from_little_endian(peer_id) ^ U256::from_little_endian(target)
}

fn distance_from(peer_id: &PeerId, distance: U256) -> Target {
    let mut target = [0; 32];
    (U256::from_little_endian(peer_id) ^ distance).to_little_endian(&mut target);
    target
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRecord {
    pub id: PeerId,
    pub key: PublicKey,
    pub addr: SocketAddr,
}

#[derive(Debug)]
pub struct Buckets {
    origin: PeerRecord,
    distances: Vec<Bucket>,
}

#[derive(Debug, Clone, Default)]
struct Bucket {
    records: Vec<PeerRecord>,
    cached_records: Vec<PeerRecord>,
}

impl Bucket {
    const SIZE: usize = 20;
}

impl Buckets {
    pub fn new(origin: PeerRecord) -> Self {
        Self {
            origin,
            distances: vec![Default::default(); BITS],
        }
    }

    fn index(&self, target: &Target) -> usize {
        distance(&self.origin.id, target).leading_zeros() as _
    }

    pub fn insert(&mut self, record: PeerRecord) {
        let index = self.index(&record.id);
        let bucket = &mut self.distances[index];
        // if record exists in the bucket, move it to the end
        // otherwise insert it only if the bucket is not full yet
        if let Some(bucket_index) = bucket
            .records
            .iter()
            .position(|bucket_record| bucket_record.id == record.id)
        {
            bucket.records.remove(bucket_index);
        }
        if bucket.records.len() < Bucket::SIZE {
            bucket.records.push(record);
            return;
        }

        // repeat on cached entries, only shifting on a full cache
        // this is surprisingly duplicated code to the above
        if let Some(bucket_index) = bucket
            .cached_records
            .iter()
            .position(|bucket_record| bucket_record.id == record.id)
        {
            bucket.cached_records.remove(bucket_index);
        }
        if bucket.cached_records.len() == Bucket::SIZE {
            bucket.cached_records.remove(0);
        }
        bucket.cached_records.push(record)
    }

    pub fn remove(&mut self, id: &PeerId) -> Option<PeerRecord> {
        let index = self.index(id);
        let bucket = &mut self.distances[index];
        let Some(bucket_index) = bucket.records.iter().position(|record| &record.id == id) else {
            return None;
        };
        let record = bucket.records.remove(bucket_index);
        if let Some(cache_record) = bucket.cached_records.pop() {
            bucket.records.push(cache_record)
        } else {
            // "mark" the record as stalled by prepending it back to the list
            // should stalled record appear in find result? paper not mentioned
            bucket.records.insert(0, record.clone())
        }
        Some(record)
    }

    fn find_closest(&self, target: &Target, count: usize) -> Vec<PeerRecord> {
        let mut records = Vec::new();
        let index = self.index(target);
        let origin_distance = distance(&self.origin.id, target);
        // look up order derived from libp2p::kad, personally i don't understand why this works
        // anyway the result is asserted before returning
        // notice that bucket index here is reversed to libp2p's, i.e. libp2p_to_this(i) = 255 - i
        for index in (index..BITS)
            .filter(|i| origin_distance.bit(BITS - 1 - *i))
            .chain(
                (0..BITS)
                    .rev()
                    .filter(|i| !origin_distance.bit(BITS - 1 - *i)),
            )
        {
            let mut index_records = self.distances[index].records.clone();
            // ensure center peer is included if it is indeed close enough
            // can it be more elegant?
            if index == BITS - 1 {
                index_records.push(self.origin.clone())
            }
            index_records.sort_unstable_by_key(|record| distance(&record.id, target));
            records.extend(index_records.into_iter().take(count - records.len()));
            assert!(records.len() <= count);
            if records.len() == count {
                break;
            }
        }
        assert!(records
            .windows(2)
            .all(|records| distance(&records[0].id, target) < distance(&records[1].id, target)));
        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::crypto::DigestHash as _;

    #[test]
    fn distance_inversion() {
        let id = rand::random::<PeerId>();
        let d = <U256 as From<[_; 32]>>::from(rand::random());
        assert_eq!(distance(&id, &distance_from(&id, d)), d);
    }

    fn ordered_closest() -> anyhow::Result<()> {
        let secp = secp256k1::Secp256k1::signing_only();
        let (_, public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let origin = PeerRecord {
            id: public_key.sha256(),
            key: public_key,
            addr: SocketAddr::from(([0, 0, 0, 0], 0)),
        };
        let mut buckets = Buckets::new(origin);
        for _ in 0..1000 {
            let (_, public_key) = secp.generate_keypair(&mut rand::thread_rng());
            buckets.insert(PeerRecord {
                id: public_key.sha256(),
                key: public_key,
                addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            })
        }
        for _ in 0..1000 {
            let records = buckets.find_closest(&rand::random(), 20);
            assert_eq!(records.len(), 20)
        }
        Ok(())
    }

    #[test]
    fn ordered_closest_100() -> anyhow::Result<()> {
        for _ in 0..100 {
            ordered_closest()?
        }
        Ok(())
    }
}
