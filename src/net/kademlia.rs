// an overlay network backed by something behaves like a `kademlia::Peer`,
// enabling peer id as address and content hash as address
// this is a leaky abstraction: by definition `SendMessage<_, _>` is unreliable,
// and sender will lose all feedbacks from the underlying kademlia peer. if the
// feedback is critical e.g. building a key value store service upon DHT, or
// taking actions when the query takes too long, should directly work with the
// peer. this network provides an otherwise convenient interface that conforms
// the generic network model
use std::{collections::HashMap, fmt::Debug, hash::Hash, num::NonZeroUsize, time::Duration};

use derive_where::derive_where;
use lru::LruCache;

use crate::{
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent, TimerId,
    },
    kademlia::{PeerId, Query, QueryResult, QueryStatus, Target},
};

use super::{Addr, DetachSend, SendMessage, SendMessageToEach, SendMessageToEachExt as _};

// SendMessage<PeerId, M>: unicast M to PeerId
// SendMessage<Multicast<Target>, M>: multicast M to Target
// it is confusing to actually enforce PeerId and Target must be the same type,
// while keep calling them with difference names everywhere
// wrap unicast with some Unicast<PeerId> as well?
// is it useful to have a variant that exclude sender from receivers i.e.
// similar to `All`?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Multicast<A = Target>(pub A, pub NonZeroUsize);

pub trait Net<A, M>: SendMessage<A, M> + for<'a> SendMessageToEach<A, M> {}
impl<T: SendMessage<A, M> + for<'a> SendMessageToEach<A, M>, A, M> Net<A, M> for T {}

#[derive_where(Debug; N, P, M, A, B, B: Eq + Hash)] // awesome!
pub struct Control<N, P, M, A, B = Target, _M = (N, P, M, A)> {
    inner_net: N,
    peer: P, // a kademlia `Peer`

    querying_unicasts: HashMap<B, QueryingUnicast<M>>,
    querying_multicasts: HashMap<B, QueryingMulticast<M>>,
    pending_multicasts: HashMap<B, Vec<(NonZeroUsize, M)>>,
    // addresses that recently send messages to
    // assuming temporal locality of addresses: those sent addresses are more likely to be sent to
    // (again) later
    addrs: LruCache<B, A>,

    _m: std::marker::PhantomData<_M>,
}

#[derive(Debug)]
struct QueryingUnicast<M> {
    messages: Vec<M>,
    timer: TimerId,
}

#[derive(Debug)]
struct QueryingMulticast<M> {
    count: NonZeroUsize,
    messages: Vec<(NonZeroUsize, M)>,
    timer: TimerId,
}

impl<N, P, M, A, B: Eq + Hash> Control<N, P, M, A, B> {
    // peer must be ready to be immediately queries e.g. have finished bootstrap
    pub fn new(inner_net: N, peer: P) -> Self {
        Self {
            inner_net,
            peer,
            querying_unicasts: Default::default(),
            querying_multicasts: Default::default(),
            pending_multicasts: Default::default(),
            // cache size is currently arbitrary chosen while prioritizing entropy usage
            // in entropy an address may be unicast after it has been multicast, and the multicast
            // has at most 120 receivers
            addrs: LruCache::new(160.try_into().unwrap()),
            _m: Default::default(),
        }
    }
}

pub trait ControlCommon {
    type N: Net<Self::A, Self::M>;
    type P: SendEvent<Query>;
    type M: Clone;
    type A: Addr;
    // have tries also add `B` here but that would cause conflict impl below
}

impl<N, P, M, A> ControlCommon for (N, P, M, A)
where
    N: Net<A, M>,
    P: SendEvent<Query>,
    M: Clone,
    A: Addr,
{
    type N = N;
    type P = P;
    type M = M;
    type A = A;
}

#[derive(Debug, Clone)]
pub struct QueryTimeout(pub Query);

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

// i want to have `OnEvent<DetachSend<impl Into<B>, _>>` like the one below for
// multicast, but that would cause an conflict
// so if one day we really want to go with some `B` other than just using H256,
// pick that according to unicast needs and make sure multicast target can
// `Into` it
// before that, these are just over engineering jibber jabbers
impl<M: ControlCommon, B: Into<Target> + Clone + Eq + Hash> OnEvent<DetachSend<B, M::M>>
    for Control<M::N, M::P, M::M, M::A, B, M>
// or maybe just B: Addr?
{
    fn on_event(
        &mut self,
        DetachSend(peer_id, message): DetachSend<B, M::M>,
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
            let query = Query(peer_id.clone().into(), 1.try_into().unwrap());
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

impl<M: ControlCommon, T: Into<B>, B: Into<Target> + Clone + Eq + Hash>
    OnEvent<DetachSend<Multicast<T>, M::M>> for Control<M::N, M::P, M::M, M::A, B, M>
{
    fn on_event(
        &mut self,
        DetachSend(Multicast(target, count), message): DetachSend<Multicast<T>, M::M>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let target = target.into();
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
        let query = Query(target.clone().into(), count);
        self.peer.send(query)?;
        self.querying_multicasts.insert(
            target,
            QueryingMulticast {
                count,
                messages: vec![(count, message)],
                timer: timer.set(QUERY_TIMEOUT, QueryTimeout(query))?,
            },
        );
        Ok(())
    }
}

impl<M: ControlCommon, B> OnEvent<QueryTimeout> for Control<M::N, M::P, M::M, M::A, B, M> {
    fn on_event(
        &mut self,
        QueryTimeout(Query(target, count)): QueryTimeout,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // TODO gracefully handle if necessary
        // maybe just log and discard
        Err(anyhow::format_err!("query timeout for ({target}, {count})"))
    }
}

impl<M: ControlCommon, B: Hash + Eq> OnEvent<QueryResult<M::A>>
    for Control<M::N, M::P, M::M, M::A, B, M>
where
    PeerId: Into<B>,
    Target: Into<B>,
{
    fn on_event(
        &mut self,
        upcall: QueryResult<M::A>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // eprintln!("{upcall:?}");
        for record in &upcall.closest {
            let id = record.id.into();
            // the unicast happens to be resolved by this query result
            if let Some(querying) = self.querying_unicasts.remove(&id) {
                self.addrs.push(id, record.addr.clone());
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
        let target = upcall.target.into();
        if let Some(querying) = self.querying_unicasts.remove(&target) {
            timer.unset(querying.timer)?;
            assert!(!self.addrs.contains(&target));
            // the destination is unreachable and the messages are dropped
            return Ok(());
        }
        if let Some(querying) = self.querying_multicasts.remove(&target) {
            for record in &upcall.closest {
                self.addrs.push(record.id.into(), record.addr.clone());
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
        if let Some(multicasts) = self.pending_multicasts.remove(&target) {
            assert!(!self.querying_unicasts.contains_key(&target));
            assert!(!self.querying_multicasts.contains_key(&target));
            let count = *multicasts.iter().map(|(count, _)| count).max().unwrap();
            let query = Query(upcall.target, count);
            self.peer.send(query)?;
            self.querying_multicasts.insert(
                target,
                QueryingMulticast {
                    count,
                    messages: multicasts,
                    timer: timer.set(QUERY_TIMEOUT, QueryTimeout(query))?,
                },
            );
        }
        Ok(())
    }
}

// cSpell:words upcall kademlia
