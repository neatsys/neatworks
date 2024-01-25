use std::collections::HashMap;

use crate::{
    event::{OnEvent, SendEvent},
    kademlia::{PeerId, PeerRecord, QueryResult, QueryStatus},
};

use super::{Addr, SendMessage};

#[derive(Debug)]
pub struct Net<E>(pub E);

impl<E: SendEvent<(PeerId, M)>, M> SendMessage<PeerId, M> for Net<E> {
    fn send(&mut self, dest: PeerId, message: M) -> anyhow::Result<()> {
        self.0.send((dest, message))
    }
}

#[derive(Debug)]
pub struct Control<N, M, P, A> {
    inner_net: N,
    peer: P, // sender handle of a kademlia Peer
    pending_messages: HashMap<PeerId, Vec<M>>,
    records: HashMap<PeerId, PeerRecord<A>>,
}

impl<N, M, P, A> Control<N, M, P, A> {
    // peer must have finished bootstrap
    pub fn new(inner_net: N, peer: P) -> Self {
        Self {
            inner_net,
            peer,
            pending_messages: Default::default(),
            records: Default::default(),
        }
    }
}

#[derive(Debug, derive_more::From)]
pub enum ControlEvent<M, A> {
    SendMessage(PeerId, M),
    Upcall(QueryResult<A>),
    // TODO timeout for clearing record cache, cancel query, etc
}

impl<N: SendMessage<A, M>, M, P: SendEvent<(PeerId, usize)>, A: Addr> OnEvent<ControlEvent<M, A>>
    for Control<N, M, P, A>
{
    fn on_event(
        &mut self,
        event: ControlEvent<M, A>,
        _timer: &mut dyn crate::event::Timer<ControlEvent<M, A>>,
    ) -> anyhow::Result<()> {
        match event {
            ControlEvent::SendMessage(peer_id, message) => self.on_send_message(&peer_id, message),
            ControlEvent::Upcall(upcall) => self.on_upcall(upcall),
        }
    }
}

impl<N: SendMessage<A, M>, M, P: SendEvent<(PeerId, usize)>, A: Addr> Control<N, M, P, A> {
    fn on_send_message(&mut self, peer_id: &PeerId, message: M) -> anyhow::Result<()> {
        if let Some(record) = self.records.get(peer_id) {
            self.inner_net.send(record.addr.clone(), message)
        } else {
            self.pending_messages
                .entry(*peer_id)
                .or_default()
                .push(message);
            self.peer.send((*peer_id, 1))
        }
    }

    fn on_upcall(&mut self, upcall: QueryResult<A>) -> anyhow::Result<()> {
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
