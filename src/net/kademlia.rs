use std::{collections::HashMap, fmt::Debug};

use crate::{
    event::{
        erasured::{OnEvent, Timer},
        SendEvent,
    },
    kademlia::{PeerId, PeerRecord, QueryResult, QueryStatus},
};

use super::{Addr, SendMessage};

#[derive(Debug, Clone)]
pub struct Net<E>(pub E);

impl<E: SendEvent<(PeerId, M)>, M> SendMessage<PeerId, M> for Net<E> {
    fn send(&mut self, dest: PeerId, message: M) -> anyhow::Result<()> {
        self.0.send((dest, message))
    }
}

pub struct Control<M, A> {
    inner_net: Box<dyn SendMessage<A, M> + Send + Sync>,
    peer: Box<dyn SendEvent<(PeerId, usize)> + Send + Sync>, // sender handle of a kademlia Peer
    pending_messages: HashMap<PeerId, Vec<M>>,
    records: HashMap<PeerId, PeerRecord<A>>,
}

impl<M: Debug, A: Debug> Debug for Control<M, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control")
            .field("pending_messages", &self.pending_messages)
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}

impl<M, A> Control<M, A> {
    // peer must have finished bootstrap
    pub fn new(
        inner_net: impl SendMessage<A, M> + Send + Sync + 'static,
        peer: impl SendEvent<(PeerId, usize)> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner_net: Box::new(inner_net),
            peer: Box::new(peer),
            pending_messages: Default::default(),
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
            self.pending_messages
                .entry(peer_id)
                .or_default()
                .push(message);
            // TODO deduplicated
            self.peer.send((peer_id, 1))
        }
    }
}

impl<M, A: Addr> OnEvent<QueryResult<A>> for Control<M, A> {
    fn on_event(&mut self, upcall: QueryResult<A>, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        // println!("{upcall:?}");
        for record in upcall.closest {
            self.records.insert(record.id, record);
        }
        if matches!(upcall.status, QueryStatus::Progress) {
            return Ok(());
        }
        if let Some(messages) = self.pending_messages.remove(&upcall.target) {
            if let Some(record) = self.records.get(&upcall.target) {
                for message in messages {
                    self.inner_net.send(record.addr.clone(), message)?
                }
            }
            // otherwise, the destination is unreachable and the messages are dropped
        }
        Ok(())
    }
}
