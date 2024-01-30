use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    iter::repeat_with,
    num::NonZeroUsize,
};

use primitive_types::{H256, U256};
use secp256k1::PublicKey;
use serde::{Deserialize, Serialize};

use crate::{
    crypto::{
        events::{Signed, Verified},
        Crypto, DigestHash, Verifiable,
    },
    event::{
        erased::{OnEvent, Timer},
        SendEvent,
    },
    net::{events::Recv, Addr, SendMessage, SendMessageToEach, SendMessageToEachExt as _},
    worker::erased::Worker,
};

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerRecord<A> {
    pub id: PeerId,
    pub key: PublicKey,
    pub addr: A,
}

impl<A> PeerRecord<A> {
    pub fn new(key: PublicKey, addr: A) -> Self {
        Self {
            id: key.sha256(),
            key,
            addr,
        }
    }
}

#[derive(Debug)]
pub struct Buckets<A> {
    origin: PeerRecord<A>,
    distances: Vec<Bucket<A>>,
}

#[derive(Debug, Clone)]
struct Bucket<A> {
    records: Vec<PeerRecord<A>>,
    cached_records: Vec<PeerRecord<A>>,
}

impl<A> Default for Bucket<A> {
    fn default() -> Self {
        Self {
            records: Default::default(),
            cached_records: Default::default(),
        }
    }
}

const BUCKET_SIZE: usize = 20;
const NUM_CONCURRENCY: usize = 3;

impl<A> Buckets<A> {
    pub fn new(origin: PeerRecord<A>) -> Self {
        Self {
            origin,
            distances: repeat_with(Default::default).take(BITS).collect(),
        }
    }

    fn index(&self, target: &Target) -> usize {
        distance(&self.origin.id, target).leading_zeros() as _
    }

    pub fn insert(&mut self, record: PeerRecord<A>) -> anyhow::Result<()> {
        if record.id == self.origin.id {
            anyhow::bail!("cannot insert origin id")
        }
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
        if bucket.records.len() < BUCKET_SIZE {
            bucket.records.push(record);
            return Ok(());
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
        if bucket.cached_records.len() == BUCKET_SIZE {
            bucket.cached_records.remove(0);
        }
        bucket.cached_records.push(record);
        Ok(())
    }
}

impl<A: Addr> Buckets<A> {
    pub fn remove(&mut self, id: &PeerId) -> Option<PeerRecord<A>> {
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

    fn find_closest(&self, target: &Target, count: NonZeroUsize) -> Vec<PeerRecord<A>> {
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
            // ensure origin peer is included if it is indeed close enough
            // can it be more elegant?
            if index == BITS - 1 {
                index_records.push(self.origin.clone())
            }
            index_records.sort_unstable_by_key(|record| distance(&record.id, target));
            records.extend(index_records.into_iter().take(count.get() - records.len()));
            assert!(records.len() <= count.into());
            if records.len() == count.into() {
                break;
            }
        }
        assert!(records
            .windows(2)
            .all(|records| distance(&records[0].id, target) < distance(&records[1].id, target)));
        records
    }
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct FindPeer<A> {
    target: Target,
    count: NonZeroUsize,
    record: PeerRecord<A>,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct FindPeerOk<A> {
    seq: u32,
    target: Target,
    closest: Vec<PeerRecord<A>>,
    record: PeerRecord<A>,
}

pub trait Net<A>:
    SendMessageToEach<A, Verifiable<FindPeer<A>>> + SendMessage<A, Verifiable<FindPeerOk<A>>>
{
}
impl<
        T: SendMessageToEach<A, Verifiable<FindPeer<A>>> + SendMessage<A, Verifiable<FindPeerOk<A>>>,
        A,
    > Net<A> for T
{
}

#[derive(Debug, Clone, Copy)]
pub struct Query(pub Target, pub NonZeroUsize);

#[derive(Debug)]
pub struct QueryResult<A> {
    pub status: QueryStatus,
    pub target: Target,
    // every peer here has been contacted just now, so they are probably reachable for a while
    pub closest: Vec<PeerRecord<A>>,
}

#[derive(Debug)]
pub enum QueryStatus {
    Progress,
    // termination condition is satisfied
    // there should be `count` peers in `closest`, and any peer that is closer to any of the peers
    // in `closest` is either contacted (and thus in `closest`) or unreachable
    Converge,
    // termination condition is not satisfied, but nothing can be further done
    // this can either because the total number of discovered peers is less than `count`, otherwise
    // i'm not sure
    Halted,
}

pub trait Upcall<A>: SendEvent<QueryResult<A>> {}
impl<T: SendEvent<QueryResult<A>>, A> Upcall<A> for T {}

pub trait SendCryptoEvent<A>:
    SendEvent<Signed<FindPeer<A>>>
    + SendEvent<Signed<FindPeerOk<A>>>
    + SendEvent<Verified<FindPeer<A>>>
    + SendEvent<Verified<FindPeerOk<A>>>
{
}
impl<
        T: SendEvent<Signed<FindPeer<A>>>
            + SendEvent<Signed<FindPeerOk<A>>>
            + SendEvent<Verified<FindPeer<A>>>
            + SendEvent<Verified<FindPeerOk<A>>>,
        A,
    > SendCryptoEvent<A> for T
{
}

#[derive(Debug, Clone)]
pub struct SendCrypto<E>(pub E);

impl<'a, E: SendCryptoEvent<A> + Send + Sync + 'a, A>
    AsMut<dyn SendCryptoEvent<A> + Send + Sync + 'a> for SendCrypto<E>
{
    fn as_mut(&mut self) -> &mut (dyn SendCryptoEvent<A> + Send + Sync + 'a) {
        &mut self.0
    }
}

pub struct Peer<A> {
    record: PeerRecord<A>,

    buckets: Buckets<A>,
    query_states: HashMap<Target, QueryState<A>>,
    refresh_targets: HashSet<Target>,
    on_bootstrap: Option<OnBootstrap>,
    find_peer_ok_seq: u32,
    find_peer_ok_dests: HashMap<u32, A>,

    net: Box<dyn Net<A> + Send + Sync>,
    upcall: Box<dyn Upcall<A> + Send + Sync>,
    crypto_worker: CryptoWorker<A>,
}

type OnBootstrap = Box<dyn FnOnce() -> anyhow::Result<()> + Send + Sync>;
pub type CryptoWorker<A> = Worker<Crypto<PeerId>, dyn SendCryptoEvent<A> + Send + Sync>;

impl<A> Debug for Peer<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer").finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct QueryState<A> {
    find_peer: Verifiable<FindPeer<A>>,
    records: Vec<PeerRecord<A>>,
    contacting: HashSet<PeerId>,
    contacted: HashSet<PeerId>,
}

impl<A: Addr> Peer<A> {
    pub fn new(
        buckets: Buckets<A>, // seed peers inserted
        net: impl Net<A> + Send + Sync + 'static,
        upcall: impl Upcall<A> + Send + Sync + 'static,
        crypto_worker: CryptoWorker<A>,
    ) -> Self {
        Self {
            record: buckets.origin.clone(),
            buckets,
            net: Box::new(net),
            upcall: Box::new(upcall),
            crypto_worker,
            query_states: Default::default(),
            refresh_targets: Default::default(),
            on_bootstrap: None,
            find_peer_ok_seq: 0,
            find_peer_ok_dests: Default::default(),
        }
    }
}

impl<A: Addr> Peer<A> {
    pub fn bootstrap(&mut self, on_bootstrap: OnBootstrap) -> anyhow::Result<()> {
        // assert only bootstrap once?
        // seems no harm to bootstrap multiple times anyway, so just assert a clear state for now
        if !self.query_states.is_empty() {
            anyhow::bail!("start bootsrap while query in progress")
        }
        if !self.refresh_targets.is_empty() {
            anyhow::bail!("start bootsrap while refresh in progress")
        }
        if self.on_bootstrap.is_some() {
            anyhow::bail!("start bootstrap while another bootstrap in progress")
        }
        self.on_bootstrap = Some(on_bootstrap);
        let target = self.record.id;
        self.start_query(&target, BUCKET_SIZE.try_into().unwrap())
    }
}

impl<A: Addr> OnEvent<Query> for Peer<A> {
    fn on_event(
        &mut self,
        Query(target, count): Query,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if self.on_bootstrap.is_some() {
            anyhow::bail!("start query while bootstrap in progress")
        }
        // there's tiny little chance that the refreshing target (which is randomly chosen) that
        // happens to be queried
        // universe will explode before that happens
        self.start_query(&target, count)
    }
}

impl<A: Addr> Peer<A> {
    fn start_query(&self, target: &Target, count: NonZeroUsize) -> anyhow::Result<()> {
        // println!("start query {} {count}", primitive_types::H256(*target));
        let find_peer = FindPeer {
            target: *target,
            count,
            record: self.record.clone(),
        };
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            // println!("signing {find_peer:?}");
            sender.send(Signed(crypto.sign(find_peer)))
        }))
    }
}

impl<A: Addr> OnEvent<Signed<FindPeer<A>>> for Peer<A> {
    fn on_event(
        &mut self,
        Signed(find_peer): Signed<FindPeer<A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let target = find_peer.target;
        let records = self.buckets.find_closest(
            &target,
            find_peer.count.max(NUM_CONCURRENCY.try_into().unwrap()),
        );
        if records.is_empty() {
            anyhow::bail!("empty buckets when finding {}", H256(target))
        }
        let mut contacting = records
            .iter()
            .map(|record| (record.id, record.addr.clone()))
            .collect::<HashMap<_, _>>();
        contacting.remove(&self.record.id);
        let state = QueryState {
            find_peer: find_peer.clone(),
            contacting: contacting.keys().copied().collect(),
            contacted: [self.record.id].into_iter().collect(),
            records,
        };
        let replaced = self.query_states.insert(target, state);
        if replaced.is_some() {
            anyhow::bail!("concurrent query to {}", H256(target))
        }
        self.net.send_to_each(contacting.into_values(), find_peer)
    }
}

impl<A: Addr> OnEvent<Recv<Verifiable<FindPeer<A>>>> for Peer<A> {
    fn on_event(
        &mut self,
        Recv(find_peer): Recv<Verifiable<FindPeer<A>>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if crypto
                .verify_with_public_key(
                    Some(find_peer.record.id),
                    &find_peer.record.key,
                    &find_peer,
                )
                .is_ok()
            {
                sender.send(Verified(find_peer))
            } else {
                Ok(())
            }
        }))
    }
}

impl<A: Addr> OnEvent<Verified<FindPeer<A>>> for Peer<A> {
    fn on_event(
        &mut self,
        Verified(find_peer): Verified<FindPeer<A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let find_peer = find_peer.into_inner();
        let dest = find_peer.record.addr.clone();
        self.buckets.insert(find_peer.record)?;
        self.find_peer_ok_seq += 1;
        let find_peer_ok = FindPeerOk {
            seq: self.find_peer_ok_seq,
            target: find_peer.target,
            closest: self
                .buckets
                .find_closest(&find_peer.target, find_peer.count),
            record: self.record.clone(),
        };
        self.find_peer_ok_dests.insert(self.find_peer_ok_seq, dest);
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            sender.send(Signed(crypto.sign(find_peer_ok)))
        }))
    }
}

impl<A> OnEvent<Signed<FindPeerOk<A>>> for Peer<A> {
    fn on_event(
        &mut self,
        Signed(find_peer_ok): Signed<FindPeerOk<A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let dest = self
            .find_peer_ok_dests
            .remove(&find_peer_ok.seq)
            .ok_or(anyhow::anyhow!(
                "unknown FindPeerOk seq {}",
                find_peer_ok.seq
            ))?;
        self.net.send(dest, find_peer_ok)
    }
}

impl<A: Addr> OnEvent<Recv<Verifiable<FindPeerOk<A>>>> for Peer<A> {
    fn on_event(
        &mut self,
        Recv(find_peer_ok): Recv<Verifiable<FindPeerOk<A>>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if crypto
                .verify_with_public_key(
                    Some(find_peer_ok.record.id),
                    &find_peer_ok.record.key,
                    &find_peer_ok,
                )
                .is_ok()
            {
                sender.send(Verified(find_peer_ok))
            } else {
                Ok(())
            }
        }))
    }
}

impl<A: Addr> OnEvent<Verified<FindPeerOk<A>>> for Peer<A> {
    fn on_event(
        &mut self,
        Verified(find_peer_ok): Verified<FindPeerOk<A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let find_peer_ok = find_peer_ok.into_inner();
        let peer_id = find_peer_ok.record.id;
        self.buckets.insert(find_peer_ok.record)?;
        // any necessity of ignoring replies of previous queries (that happens to be for the same
        // target)?
        let target = find_peer_ok.target;
        let Some(state) = self.query_states.get_mut(&target) else {
            return Ok(());
        };
        if !state.contacting.remove(&peer_id) {
            return Ok(());
        }
        state.contacted.insert(peer_id);
        for record in find_peer_ok.closest {
            match state
                .records
                .binary_search_by_key(&distance(&record.id, &target), |record| {
                    distance(&record.id, &target)
                }) {
                Ok(index) => {
                    if record != state.records[index] {
                        anyhow::bail!("non-distinct distance")
                    }
                }
                Err(index) => state.records.insert(index, record),
            }
        }
        let closest = state
            .records
            .iter()
            .filter(|record| state.contacted.contains(&record.id))
            .take(state.find_peer.count.into())
            .cloned()
            .collect();
        let status = 'upcall: {
            let count = state
                .records
                .iter()
                .take_while(|record| state.contacted.contains(&record.id))
                .count();
            if count >= state.find_peer.count.into() {
                self.query_states.remove(&target);
                break 'upcall QueryStatus::Converge;
            }
            if count == state.records.len() {
                self.query_states.remove(&target);
                break 'upcall QueryStatus::Halted;
            }
            let mut addrs = Vec::new();
            while state.contacting.len() < NUM_CONCURRENCY {
                let Some(record) = state.records.iter().find(|record| {
                    !state.contacted.contains(&record.id) && !state.contacting.contains(&record.id)
                }) else {
                    break;
                };
                state.contacting.insert(record.id);
                addrs.push(record.addr.clone());
            }
            if !addrs.is_empty() {
                self.net
                    .send_to_each(addrs.into_iter(), state.find_peer.clone())?
            }
            QueryStatus::Progress
        };
        // println!("target {} status {status:?}", H256(target));
        if self.on_bootstrap.is_none() {
            if !self.refresh_targets.contains(&target) {
                self.upcall.send(QueryResult {
                    status,
                    target,
                    closest,
                })?
            } else {
                // TODO log warning on halted refreshing
                if matches!(status, QueryStatus::Converge | QueryStatus::Halted) {
                    self.refresh_targets.remove(&target);
                }
            }
            return Ok(());
        }
        match status {
            QueryStatus::Converge => {}
            QueryStatus::Progress => return Ok(()),
            QueryStatus::Halted => {} // log warn/return error?
        }
        if self.refresh_targets.is_empty() {
            self.refresh_buckets()?;
            if self.refresh_targets.is_empty() {
                self.on_bootstrap.take().unwrap()()?
            }
            Ok(())
        } else {
            let removed = self.refresh_targets.remove(&target);
            assert!(removed);
            if let Some(target) = self.refresh_targets.iter().next() {
                self.start_query(target, BUCKET_SIZE.try_into().unwrap())
            } else {
                self.on_bootstrap.take().unwrap()()
            }
        }
    }
}

impl<A: Addr> Peer<A> {
    fn refresh_buckets(&mut self) -> anyhow::Result<()> {
        let mut end_index = BITS;
        if self.on_bootstrap.is_some() {
            // optimize for bootstrap: skip from the closest bucket until the first non-empty one,
            // and skip that non-empty bucket as well. assert the refreshing of those buckets cannot
            // discover any peer
            while {
                end_index -= 1;
                self.buckets.distances[end_index].records.is_empty()
            } {}
        }
        for i in 0..end_index {
            let d = rand_distance(i, &mut rand::thread_rng());
            let target = distance_from(&self.record.id, d);
            assert_eq!(self.buckets.index(&target), BITS - i - 1);
            self.refresh_targets.insert(target);
        }
        // refresh the buckets without concurrency
        // if refresh query for all buckets at the same time, testing on loopback network drops
        // maybe docker's loopback network is too bad, but mitigating transient performance
        // degradation caused by refreshing is still generally good to have
        if let Some(target) = self.refresh_targets.iter().next() {
            self.start_query(target, BUCKET_SIZE.try_into().unwrap())?
        }
        Ok(())
    }
}

// https://github.com/libp2p/rust-libp2p/blob/master/protocols/kad/src/kbucket.rs
/// Generates a random distance that falls into the bucket for this index.
fn rand_distance(index: usize, rng: &mut impl rand::Rng) -> U256 {
    let mut bytes = [0u8; 32];
    let quot = index / 8;
    rng.fill_bytes(&mut bytes[..quot]);
    let rem = (index % 8) as u32;
    let lower = usize::pow(2, rem);
    let upper = usize::pow(2, rem + 1);
    bytes[quot] = rng.gen_range(lower..upper) as u8;
    U256::from_little_endian(&bytes)
}

// #[derive(Debug, Clone, derive_more::From, Serialize, Deserialize)]
// pub enum Message<A> {
//     FindPeer(Verifiable<FindPeer<A>>),
//     FindPeerOk(Verifiable<FindPeerOk<A>>),
// }

pub trait SendRecvEvent<A>:
    SendEvent<Recv<Verifiable<FindPeer<A>>>> + SendEvent<Recv<Verifiable<FindPeerOk<A>>>>
{
}
impl<
        T: SendEvent<Recv<Verifiable<FindPeer<A>>>> + SendEvent<Recv<Verifiable<FindPeerOk<A>>>>,
        A,
    > SendRecvEvent<A> for T
{
}

#[cfg(test)]
mod tests {
    use crate::net::IterAddr;

    use super::*;

    #[test]
    fn distance_inversion() {
        let id = rand::random::<PeerId>();
        let d = <U256 as From<[_; 32]>>::from(rand::random());
        assert_eq!(distance(&id, &distance_from(&id, d)), d);
    }

    fn ordered_closest() -> anyhow::Result<()> {
        let secp = secp256k1::Secp256k1::signing_only();
        let (_, public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let origin = PeerRecord::new(public_key, ());
        let mut buckets = Buckets::new(origin);
        for _ in 0..1000 {
            let (_, public_key) = secp.generate_keypair(&mut rand::thread_rng());
            buckets.insert(PeerRecord::new(public_key, ()))?
        }
        for _ in 0..1000 {
            let records = buckets.find_closest(&rand::random(), 20.try_into().unwrap());
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

    struct NullNet;
    impl SendMessage<IterAddr<'_, ()>, Verifiable<FindPeer<()>>> for NullNet {
        fn send(&mut self, _: IterAddr<'_, ()>, _: Verifiable<FindPeer<()>>) -> anyhow::Result<()> {
            Ok(())
        }
    }
    impl SendMessage<(), Verifiable<FindPeerOk<()>>> for NullNet {
        fn send(&mut self, (): (), _: Verifiable<FindPeerOk<()>>) -> anyhow::Result<()> {
            Ok(())
        }
    }
    struct NullUpcall;
    impl SendEvent<QueryResult<()>> for NullUpcall {
        fn send(&mut self, _: QueryResult<()>) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn refresh_buckets() -> anyhow::Result<()> {
        let secp = secp256k1::Secp256k1::signing_only();
        let (_, public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let origin = PeerRecord::new(public_key, ());
        let buckets = Buckets::new(origin.clone());
        let mut peer = Peer::new(buckets, NullNet, NullUpcall, Worker::Null);
        peer.refresh_buckets()
    }
}
