use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
};

use crate::{
    event::{
        erased::{OnEvent, Timer},
        SendEvent,
    },
    kademlia::{PeerId, PeerRecord, Query, QueryResult, QueryStatus, Target},
};

use super::{Addr, IterAddr, SendMessage};

#[derive(Debug, Clone)]
pub struct PeerNet<E>(pub E);

impl<E: SendEvent<(PeerId, M)>, M> SendMessage<PeerId, M> for PeerNet<E> {
    fn send(&mut self, dest: PeerId, message: M) -> anyhow::Result<()> {
        self.0.send((dest, message))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Multicast(pub Target, pub usize);

// is it useful to have a variant that suppress loopback?

impl<E: SendEvent<(Multicast, M)>, M> SendMessage<Multicast, M> for PeerNet<E> {
    fn send(&mut self, dest: Multicast, message: M) -> anyhow::Result<()> {
        self.0.send((dest, message))
    }
}

pub trait Net<A, M>: SendMessage<A, M> + for<'a> SendMessage<IterAddr<'a, A>, M> {}
impl<T: SendMessage<A, M> + for<'a> SendMessage<IterAddr<'a, A>, M>, A, M> Net<A, M> for T {}

pub struct Control<M, A> {
    inner_net: Box<dyn Net<A, M> + Send + Sync>,
    peer: Box<dyn SendEvent<Query> + Send + Sync>, // sender handle of a kademlia Peer
    querying_unicasts: HashMap<PeerId, Vec<M>>,
    querying_multicasts: HashMap<Target, (usize, Vec<(usize, M)>)>,
    pending_multicasts: HashMap<Target, Vec<(usize, M)>>,
    records: HashMap<PeerId, PeerRecord<A>>,
}

impl<M: Debug, A: Debug> Debug for Control<M, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control")
            .field("pending_messages", &self.querying_unicasts)
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}

impl<M, A> Control<M, A> {
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
            records: Default::default(),
        }
    }
}

impl<M, A: Addr> OnEvent<(PeerId, M)> for Control<M, A> {
    fn on_event(
        &mut self,
        (peer_id, message): (PeerId, M),
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(record) = self.records.get(&peer_id) {
            self.inner_net.send(record.addr.clone(), message)
        } else {
            let entry = self.querying_unicasts.entry(peer_id);
            if matches!(entry, Entry::Vacant(_)) &&
                // the multicast query happens to accomplish the desired query
                !self.querying_multicasts.contains_key(&peer_id)
            {
                self.peer.send(Query(peer_id, 1))? // TODO set timer
            }
            entry.or_default().push(message);
            Ok(())
        }
    }
}

impl<M, A> OnEvent<(Multicast, M)> for Control<M, A> {
    fn on_event(
        &mut self,
        (Multicast(target, count), message): (Multicast, M),
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some((query_count, multicasts)) = self.querying_multicasts.get_mut(&target) {
            if count <= *query_count {
                multicasts.push((count, message))
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
        self.peer.send(Query(target, count))?; // TODO set timer
        self.querying_multicasts
            .insert(target, (count, vec![(count, message)]));
        Ok(())
    }
}

impl<M: Clone, A: Addr> OnEvent<QueryResult<A>> for Control<M, A> {
    fn on_event(&mut self, upcall: QueryResult<A>, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        // println!("{upcall:?}");
        for record in &upcall.closest {
            self.records.insert(record.id, record.clone());
        }
        if matches!(upcall.status, QueryStatus::Progress) {
            return Ok(());
        }
        if let Some(messages) = self.querying_unicasts.remove(&upcall.target) {
            if let Some(record) = self.records.get(&upcall.target) {
                for message in messages {
                    self.inner_net.send(record.addr.clone(), message)?
                }
            }
            // otherwise, the destination is unreachable and the messages are dropped
        }
        if let Some((_, multicasts)) = self.querying_multicasts.remove(&upcall.target) {
            for (count, message) in multicasts {
                let mut addrs = upcall
                    .closest
                    .iter()
                    .take(count)
                    .map(|record| record.addr.clone());
                self.inner_net.send(IterAddr(&mut addrs), message.clone())?
            }
        }
        // assert at least one branch above has been entered
        if let Some(multicasts) = self.pending_multicasts.remove(&upcall.target) {
            assert!(!self.querying_unicasts.contains_key(&upcall.target));
            assert!(!self.querying_multicasts.contains_key(&upcall.target));
            let count = *multicasts.iter().map(|(count, _)| count).max().unwrap();
            self.peer.send(Query(upcall.target, count))?; // TODO set timer
            self.querying_multicasts
                .insert(upcall.target, (count, multicasts));
        }
        Ok(())
    }
}
