use std::{
    array::from_fn,
    collections::{HashMap, HashSet},
    fmt::Debug,
    num::NonZeroUsize,
    time::Duration,
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
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent, TimerId,
    },
    net::{events::Recv, Addr, SendMessage, SendMessageToEach, SendMessageToEachExt as _},
    worker::erased::Worker,
};

// 32 bytes array refers to sha256 digest by default in this codebase
// consider new types if necessary
// (hopefully no demand on digest-based routing...)
pub type PeerId = [u8; 32];
pub type Target = [u8; 32];

const U256_BITS: usize = 256;

fn distance(peer_id: &PeerId, target: &Target) -> U256 {
    U256::from_little_endian(peer_id) ^ U256::from_little_endian(target)
}

fn distance_from(peer_id: &PeerId, distance: U256) -> Target {
    let mut target = [0; 32];
    (U256::from_little_endian(peer_id) ^ distance).to_little_endian(&mut target);
    target
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerRecord<K, A> {
    pub id: PeerId,
    pub key: K,
    pub addr: A,
}

impl<K: Debug, A: Debug> Debug for PeerRecord<K, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerRecord")
            .field("id", &format!("{}", H256(self.id)))
            .field("key", &"..")
            .field("addr", &self.addr)
            .finish()
    }
}

impl<K: DigestHash, A> PeerRecord<K, A> {
    pub fn new(key: K, addr: A) -> Self {
        Self {
            id: key.sha256(),
            key,
            addr,
        }
    }
}

// this is NOT a proper generalization on `BITS` because the underlying
// `PeerRecord` always contains a fixed 256-bit `PeerId`, which cannot match
// `BITS` easily since it permits incomplete byte
// currently this is just a monkey patch for model checking on a reduced scale,
// intented for internal use only
#[derive(Debug)]
pub struct Buckets<K, A, const BITS: usize = U256_BITS> {
    origin: PeerRecord<K, A>,
    distances: [Bucket<K, A>; BITS],
}

#[derive(Debug, Clone)]
struct Bucket<K, A> {
    records: Vec<PeerRecord<K, A>>,
    cached_records: Vec<PeerRecord<K, A>>,
}

impl<K, A> Default for Bucket<K, A> {
    fn default() -> Self {
        Self {
            records: Default::default(),
            cached_records: Default::default(),
        }
    }
}

const BUCKET_SIZE: usize = 20;
const NUM_CONCURRENCY: usize = 30;

impl<K, A, const BITS: usize> Buckets<K, A, BITS> {
    pub fn new(origin: PeerRecord<K, A>) -> Self {
        Self {
            origin,
            distances: from_fn(|_| Default::default()),
        }
    }

    fn index(&self, target: &Target) -> usize {
        let rev_log = distance(&self.origin.id, target).leading_zeros() as usize;
        assert!(rev_log < U256_BITS);
        U256_BITS - 1 - rev_log
    }

    pub fn insert(&mut self, record: PeerRecord<K, A>) -> anyhow::Result<()> {
        #[cfg(not(kani))]
        if record.id == self.origin.id {
            anyhow::bail!("cannot insert origin id")
        }
        // `assume` not equal for kani
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

impl<K: Clone, A: Addr, const BITS: usize> Buckets<K, A, BITS> {
    pub fn remove(&mut self, id: &PeerId) -> Option<PeerRecord<K, A>> {
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

    fn find_closest(&self, target: &Target, count: NonZeroUsize) -> Vec<PeerRecord<K, A>> {
        let mut records = Vec::new();
        let index = self.index(target);
        let origin_distance = distance(&self.origin.id, target);
        // look up order derived from libp2p::kad, personally i don't understand why this works
        // anyway the result is asserted before returning
        for index in (index..BITS)
            .filter(|i| origin_distance.bit(*i))
            .chain((0..BITS).rev().filter(|i| !origin_distance.bit(*i)))
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
            if records.len() == count.get() {
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
    record: PeerRecord<PublicKey, A>,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct FindPeerOk<A> {
    seq: u32,
    target: Target,
    closest: Vec<PeerRecord<PublicKey, A>>,
    record: PeerRecord<PublicKey, A>,
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

pub struct QueryResult<A> {
    pub status: QueryStatus,
    pub target: Target,
    // every peer here has been contacted just now, so they are probably reachable for a while
    pub closest: Vec<PeerRecord<PublicKey, A>>,
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

impl<A: Debug> Debug for QueryResult<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("status", &self.status)
            .field("target", &format!("{}", H256(self.target)))
            .field("closest", &self.closest)
            .finish()
    }
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

pub struct Peer<A> {
    record: PeerRecord<PublicKey, A>,

    buckets: Buckets<PublicKey, A>,
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
    records: Vec<PeerRecord<PublicKey, A>>,
    contacting: HashMap<PeerId, TimerId>,
    contacted: HashSet<PeerId>,
}

impl<A: Addr> Peer<A> {
    pub fn new(
        buckets: Buckets<PublicKey, A>, // seed peers inserted
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
    fn start_query(&mut self, target: &Target, count: NonZeroUsize) -> anyhow::Result<()> {
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

#[derive(Debug, Clone)]
struct ResendFindPeer<A>(Target, PeerRecord<PublicKey, A>);
const RESEND_FIND_PEER_DURATION: Duration = Duration::from_millis(2500);

impl<A: Addr> OnEvent<Signed<FindPeer<A>>> for Peer<A> {
    fn on_event(
        &mut self,
        Signed(find_peer): Signed<FindPeer<A>>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let target = find_peer.target;
        let records = self.buckets.find_closest(
            &target,
            find_peer.count.max(NUM_CONCURRENCY.try_into().unwrap()),
        );
        if records.is_empty() {
            anyhow::bail!("empty buckets when finding {}", H256(target))
        }
        let mut contacting = HashMap::new();
        let mut addrs = Vec::new();
        for record in records
            .iter()
            .filter(|record| record.id != self.record.id)
            .take(NUM_CONCURRENCY)
        {
            contacting.insert(
                record.id,
                timer.set(
                    RESEND_FIND_PEER_DURATION,
                    ResendFindPeer(target, record.clone()),
                )?,
            );
            addrs.push(record.addr.clone())
        }
        let state = QueryState {
            find_peer: find_peer.clone(),
            contacting,
            contacted: [self.record.id].into_iter().collect(),
            records,
        };
        let replaced = self.query_states.insert(target, state);
        if replaced.is_some() {
            anyhow::bail!("concurrent query to {}", H256(target))
        }
        self.net.send_to_each(addrs.into_iter(), find_peer)
    }
}

impl<A: Addr> OnEvent<ResendFindPeer<A>> for Peer<A> {
    fn on_event(
        &mut self,
        ResendFindPeer(target, record): ResendFindPeer<A>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let state = self.query_states.get_mut(&target).unwrap();
        // eprintln!(
        //     "Resend FindPeer({}, {}) {record:?}",
        //     H256(target),
        //     state.find_peer.count
        // );
        // self.net
        //     .send_to_each([record.addr.clone()].into_iter(), state.find_peer.clone())
        Err(anyhow::anyhow!(
            "FindPeer({}, {}) timeout {record:?}",
            H256(target),
            state.find_peer.count
        ))
    }
}

impl<A: Addr> OnEvent<Recv<Verifiable<FindPeer<A>>>> for Peer<A> {
    fn on_event(
        &mut self,
        Recv(find_peer): Recv<Verifiable<FindPeer<A>>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // eprintln!(
        //     "{} contacted for target {}",
        //     H256(self.record.id),
        //     H256(find_peer.target)
        // );
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
                eprintln!("fail to verify FindPeer");
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
                eprintln!("fail to verify FindPeerOk");
                Ok(())
            }
        }))
    }
}

impl<A: Addr> OnEvent<Verified<FindPeerOk<A>>> for Peer<A> {
    fn on_event(
        &mut self,
        Verified(find_peer_ok): Verified<FindPeerOk<A>>,
        timer: &mut impl Timer<Self>,
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
        let Some(resend_timer) = state.contacting.remove(&peer_id) else {
            return Ok(());
        };
        timer.unset(resend_timer)?;
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
        // use std::fmt::Write;
        // let mut out = String::new();
        // writeln!(&mut out, "records of {}", H256(target))?;
        // for record in &state.records {
        //     write!(&mut out, "  {record:?}")?;
        //     if state.contacted.contains(&record.id) {
        //         writeln!(&mut out, " (contacted)")?
        //     } else if state.contacting.contains(&record.id) {
        //         writeln!(&mut out, " (contacting)")?
        //     } else {
        //         writeln!(&mut out)?
        //     }
        // }
        // eprint!("{out}");
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
                let state = self.query_states.remove(&target).unwrap();
                for resend_timer in state.contacting.into_values() {
                    timer.unset(resend_timer)?
                }
                break 'upcall QueryStatus::Converge;
            }
            let mut addrs = Vec::new();
            while state.contacting.len() < NUM_CONCURRENCY {
                let Some(record) = state.records.iter().find(|record| {
                    !state.contacted.contains(&record.id)
                        && !state.contacting.contains_key(&record.id)
                }) else {
                    break;
                };
                // eprintln!("contacting {record:?}");
                state.contacting.insert(
                    record.id,
                    timer.set(
                        RESEND_FIND_PEER_DURATION,
                        ResendFindPeer(target, record.clone()),
                    )?,
                );
                addrs.push(record.addr.clone());
            }
            if state.contacting.is_empty() {
                self.query_states.remove(&target);
                break 'upcall QueryStatus::Halted;
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
            if let Some(&target) = self.refresh_targets.iter().next() {
                self.start_query(&target, BUCKET_SIZE.try_into().unwrap())
            } else {
                self.on_bootstrap.take().unwrap()()
            }
        }
    }
}

impl<A: Addr> Peer<A> {
    fn refresh_buckets(&mut self) -> anyhow::Result<()> {
        let mut start_index = 0;
        if self.on_bootstrap.is_some() {
            // optimize for bootstrap: skip from the closest bucket until the first non-empty one,
            // and skip that non-empty bucket as well. assert the refreshing of those buckets cannot
            // discover any peer
            while {
                start_index += 1;
                self.buckets.distances[start_index].records.is_empty()
            } {}
        }
        for i in start_index..U256_BITS {
            let d = rand_distance(i, &mut rand::thread_rng());
            let target = distance_from(&self.record.id, d);
            assert_eq!(self.buckets.index(&target), i);
            self.refresh_targets.insert(target);
        }
        // refresh the buckets without concurrency
        // if refresh query for all buckets at the same time, testing on loopback network drops
        // maybe docker's loopback network is too bad, but mitigating transient performance
        // degradation caused by refreshing is still generally good to have
        if let Some(&target) = self.refresh_targets.iter().next() {
            self.start_query(&target, BUCKET_SIZE.try_into().unwrap())?
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

#[cfg(kani)]
fn rand_distance_model(index: usize) -> U256 {
    // let mut bytes = [0u8; 32];
    let quot = index / 8;
    // rng.fill_bytes(&mut bytes[..quot]);

    let mut bytes = kani::any::<[u8; 32]>();
    const ZERO_BYTES: [u8; 32] = [0; 32];
    kani::assume(bytes[quot..] == ZERO_BYTES[quot..]);

    let rem = (index % 8) as u32;
    let lower = usize::pow(2, rem);
    let upper = usize::pow(2, rem + 1);

    // bytes[quot] = rng.gen_range(lower..upper) as u8;
    bytes[quot] = kani::any();
    kani::assume(bytes[quot] >= lower as u8);
    kani::assume(bytes[quot] < upper as u8);

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
use proptest::prelude::*;

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn distance_inversion(id: PeerId, d: [u8; 32]) {
        let d = U256::from_little_endian(&d);
        assert_eq!(distance(&id, &distance_from(&id, d)), d)
    }

    #[test]
    fn ordered_closest(id: PeerId, insert_ids: [PeerId; 1000], targets: [Target; 100]) {
        let origin = PeerRecord {
            id,
            key: (),
            addr: (),
        };
        let mut buckets = Buckets::<_, _>::new(origin);
        for insert_id in insert_ids {
            let record = PeerRecord {
                id: insert_id,
                key: (),
                addr: (),
            };
            // not 100% safe, but only crash by chance 1/(2^256)
            buckets.insert(record).unwrap()
        }
        for target in targets {
            let records = buckets.find_closest(&target, 20.try_into().unwrap());
            assert_eq!(records.len(), 20)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::net::IterAddr;

    use super::*;

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

    // there's `thread_rng()` inside `refresh_buckets` implementation so it cannot be easily convert
    // into property test
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

// run with
// cargo kani \
//   --enable-unstable $@ \
//   --cbmc-args \
//   --unwindset memcmp.0:257 \
//   --unwindset _ZN4core5slice6memchr12memchr_naive17h64b537d458d6e690E.0:257 \
//   --unwindset _ZN4core5slice6memchr14memchr_aligned17h75bd820b1dfabaafE.0:257 \
//   --unwindset _RNvMs2m_CscBgMDJYxYlU_15primitive_typesNtB6_4U25618from_little_endian.0:257 \
//   --unwindset _RNvXs2I_CscBgMDJYxYlU_15primitive_typesNtB6_4U256NtNtNtCsiCvmSzcCjPe_4core3ops3bit6BitXor6bitxor.0:257 \
//   --unwindset _RNvMs2m_CscBgMDJYxYlU_15primitive_typesNtB6_4U25613leading_zeros.0:257 \
//   --unwindset _RINvXs2T_NtNtCsiCvmSzcCjPe_4core5slice4iterINtB7_4IterINtNtCskqTXbRBwjM9_8augustus8kademlia10PeerRecorduuEENtNtNtNtBb_4iter6traits8iterator8Iterator8positionNCNvMs1_BT_INtBT_7BucketsuuKj8_E6insert0EBV_.0:21 \
//   --unwindset _RINvXs2T_NtNtCsiCvmSzcCjPe_4core5slice4iterINtB7_4IterINtNtCskqTXbRBwjM9_8augustus8kademlia10PeerRecorduuEENtNtNtNtBb_4iter6traits8iterator8Iterator8positionNCNvMs1_BT_INtBT_7BucketsuuKj8_E6inserts_0EBV_.0:21 \
//   --unwindset _RINvNtCsiCvmSzcCjPe_4core5array18try_from_fn_erasedINtNtCskqTXbRBwjM9_8augustus8kademlia6BucketuuEINtNtNtB4_3ops9try_trait17NeverShortCircuitBN_ENCINvMB1B_B1y_10wrap_mut_1jNCNvMs1_BQ_INtBQ_7BucketsuuKj8_E3new0E0EBS_.0:9 \
//   --unwindset _RNvNtNtCskqTXbRBwjM9_8augustus8kademlia12verification15ordered_closest.0:3
// to set proper unwind depth for various loops
// easily get invalid by name mangling rule change or implementation update
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    fn distance_inversion() {
        let id = kani::any();
        let d = <U256 as From<[_; 32]>>::from(kani::any());
        assert_eq!(distance(&id, &distance_from(&id, d)), d);
    }

    // this test cannot finish within reasonable time and memory in current form
    // #[kani::proof]
    // #[kani::unwind(1)]
    // fn ordered_closest() {
    //     let origin = PeerRecord {
    //         id: kani::any(),
    //         key: (),
    //         addr: (),
    //     };
    //     const BITS: usize = 8;
    //     let mut buckets = Buckets::<_, _, BITS>::new(origin.clone());
    //     for _ in 0..2 {
    //         let record = PeerRecord {
    //             id: kani::any(),
    //             key: (),
    //             addr: (),
    //         };
    //         kani::assume(record.id != origin.id);
    //         kani::assume(record.id[BITS / 8..] == origin.id[BITS / 8..]);
    //         let result = buckets.insert(record);
    //         assert!(result.is_ok())
    //     }
    //     let records = buckets.find_closest(&kani::any(), 2.try_into().unwrap());
    //     assert_eq!(records.len(), 2)
    // }

    // this one does work, but must manually synchronize with the actual implementation
    // i.e. this function vs `refresh_buckets`, `rand_distance_model` vs `rand_distance`
    #[kani::proof]
    fn refresh_buckets() {
        let origin = PeerRecord {
            id: kani::any(),
            key: (),
            addr: (),
        };
        const BITS: usize = 32;
        let buckets = Buckets::<_, _, BITS>::new(origin.clone());

        let i = kani::any();
        kani::assume(i < BITS);
        let d = rand_distance_model(i);
        let target = distance_from(&origin.id, d);
        assert_eq!(buckets.index(&target), i);
    }
}
