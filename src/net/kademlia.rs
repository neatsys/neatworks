use std::{collections::HashMap, fmt::Debug, hash::Hash, num::NonZeroUsize, time::Duration};

use lru::LruCache;
use primitive_types::H256;

use crate::{
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent, TimerId,
    },
    kademlia::{PeerId, Query, QueryResult, QueryStatus, Target},
};

use super::{Addr, SendMessage, SendMessageToEach, SendMessageToEachExt as _};

#[derive(Debug, Clone)]
pub struct PeerNet<E>(pub E);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Multicast<A = Target>(pub A, pub NonZeroUsize);

// SendMessage<A, M>: unicast M to A
// SendMessage<Multicast<A>, M>: multicast M to A
// wrap unicast with some Unicast<A> as well?
impl<E: SendEvent<(A, M)>, A, M> SendMessage<A, M> for PeerNet<E> {
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        self.0.send((dest, message))
    }
}

// is it useful to have a variant that suppress loopback?

pub trait Net<A, M>: SendMessage<A, M> + for<'a> SendMessageToEach<A, M> {}
impl<T: SendMessage<A, M> + for<'a> SendMessageToEach<A, M>, A, M> Net<A, M> for T {}

pub struct Control<M, A, B = [u8; 32]> {
    inner_net: Box<dyn Net<A, M> + Send + Sync>,
    peer: Box<dyn SendEvent<Query> + Send + Sync>, // sender handle of a kademlia Peer
    querying_unicasts: HashMap<B, QueryingUnicast<M>>,
    querying_multicasts: HashMap<B, QueryMulticast<M>>,
    pending_multicasts: HashMap<B, Vec<(NonZeroUsize, M)>>,
    // addresses that recently send messages to
    // assuming temporal locality of addresses that those sent addresses are more likely to be sent
    // to (again) later
    addrs: LruCache<B, A>,
}

impl<M: Debug, A: Debug, B: Debug + Eq + Hash> Debug for Control<M, A, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control")
            .field("pending_messages", &self.querying_unicasts)
            .field("records", &self.addrs)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct QueryingUnicast<M> {
    messages: Vec<M>,
    timer: TimerId,
}

#[derive(Debug)]
struct QueryMulticast<M> {
    count: NonZeroUsize,
    messages: Vec<(NonZeroUsize, M)>,
    timer: TimerId,
}

impl<M, A, B: Eq + Hash> Control<M, A, B> {
    // peer must have finished bootstrap
    pub fn new(
        inner_net: impl Net<A, M> + Send + Sync + 'static,
        peer: impl SendEvent<Query> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner_net: Box::new(inner_net),
            peer: Box::new(peer),
            querying_unicasts: Default::default(),
            querying_multicasts: Default::default(),
            pending_multicasts: Default::default(),
            // cache size is arbitrary chosen for entropy
            // in entropy an address may be unicast after it has been multicast, and the multicast
            // has at most 120 receivers
            addrs: LruCache::new(160.try_into().unwrap()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryTimeout(pub Query);

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

impl<M, A: Addr> OnEvent<(PeerId, M)> for Control<M, A, PeerId> {
    fn on_event(
        &mut self,
        (peer_id, message): (PeerId, M),
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(addr) = self.addrs.get(&peer_id) {
            // eprintln!("Unicast({}) cached", H256(peer_id));
            self.inner_net.send(addr.clone(), message)?
        } else if let Some(querying) = self.querying_multicasts.get_mut(&peer_id) {
            // the multicast query happens to accomplish the desired query, so "pretend" to be a
            // multicast
            // is this ok?
            querying.messages.push((1.try_into().unwrap(), message))
        } else if let Some(querying) = self.querying_unicasts.get_mut(&peer_id) {
            querying.messages.push(message)
        } else {
            // eprintln!("Unicast({}) start query", H256(peer_id));
            let query = Query(peer_id, 1.try_into().unwrap());
            self.peer.send(query)?;
            self.querying_unicasts.insert(
                peer_id,
                QueryingUnicast {
                    messages: vec![message],
                    timer: timer.set(QUERY_TIMEOUT, QueryTimeout(query))?,
                },
            );
        }
        Ok(())
    }
}

impl<M, A> OnEvent<(Multicast<Target>, M)> for Control<M, A, Target> {
    fn on_event(
        &mut self,
        (Multicast(target, count), message): (Multicast<Target>, M),
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // eprintln!("Multicast({}, {count})", H256(target));
        if let Some(querying) = self.querying_multicasts.get_mut(&target) {
            if count <= querying.count {
                querying.messages.push((count, message))
            } else {
                self.pending_multicasts
                    .entry(target)
                    .or_default()
                    .push((count, message))
            }
            return Ok(());
        }
        if self.querying_unicasts.contains_key(&target) {
            self.pending_multicasts
                .entry(target)
                .or_default()
                .push((count, message));
            return Ok(());
        }
        let query = Query(target, count);
        self.peer.send(query)?;
        self.querying_multicasts.insert(
            target,
            QueryMulticast {
                count,
                messages: vec![(count, message)],
                timer: timer.set(QUERY_TIMEOUT, QueryTimeout(query))?,
            },
        );
        Ok(())
    }
}

impl<M, A, B> OnEvent<QueryTimeout> for Control<M, A, B> {
    fn on_event(
        &mut self,
        QueryTimeout(Query(target, count)): QueryTimeout,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // TODO gracefully handle if necessary
        Err(anyhow::anyhow!(
            "query timeout for ({}, {count})",
            H256(target)
        ))
    }
}

impl<M: Clone, A: Addr> OnEvent<QueryResult<A>> for Control<M, A, PeerId> {
    fn on_event(
        &mut self,
        upcall: QueryResult<A>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // eprintln!("{upcall:?}");
        for record in &upcall.closest {
            // the unicast happens to be resolved by this query result
            if let Some(querying) = self.querying_unicasts.remove(&record.id) {
                self.addrs.push(record.id, record.addr.clone());
                // we don't care whether that original query will finish (or have finished) or not
                timer.unset(querying.timer)?;
                for message in querying.messages {
                    self.inner_net.send(record.addr.clone(), message)?
                }
            }
        }
        if matches!(upcall.status, QueryStatus::Progress) {
            return Ok(());
        }
        if let Some(querying) = self.querying_unicasts.remove(&upcall.target) {
            timer.unset(querying.timer)?;
            assert!(!self.addrs.contains(&upcall.target));
            // the destination is unreachable and the messages are dropped
            return Ok(());
        }
        if let Some(querying) = self.querying_multicasts.remove(&upcall.target) {
            for record in &upcall.closest {
                self.addrs.push(record.id, record.addr.clone());
            }
            timer.unset(querying.timer)?;
            for (count, message) in querying.messages {
                if count.get() > upcall.closest.len() {
                    // log insufficient multicast
                }
                let addrs = upcall
                    .closest
                    .iter()
                    .take(count.into())
                    .map(|record| record.addr.clone());
                self.inner_net.send_to_each(addrs, message.clone())?
            }
        }
        // assert at least one branch above has been entered
        if let Some(multicasts) = self.pending_multicasts.remove(&upcall.target) {
            assert!(!self.querying_unicasts.contains_key(&upcall.target));
            assert!(!self.querying_multicasts.contains_key(&upcall.target));
            let count = *multicasts.iter().map(|(count, _)| count).max().unwrap();
            let query = Query(upcall.target, count);
            self.peer.send(query)?;
            self.querying_multicasts.insert(
                upcall.target,
                QueryMulticast {
                    count,
                    messages: multicasts,
                    timer: timer.set(QUERY_TIMEOUT, QueryTimeout(query))?,
                },
            );
        }
        Ok(())
    }
}
