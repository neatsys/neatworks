use std::{cmp::Ordering, collections::VecDeque};

use crate::{
    event::{erased::OnEvent, SendEvent},
    net::{events::Recv, All, SendMessage},
};

pub trait Clock: PartialOrd + Clone + Send + Sync + 'static {
    // this is different from just `+ Ord` above: this is another arbitrary defined total ordering
    // that when `PartialOrd::partial_cmp` returns `Some(_)`, this must also returns `_`, but the
    // partial order may still return `None` sometimes (thus `Clock` does not `impl Ord`), and then
    // this would return arbitrary result (but still some result)
    fn cmp(&self, other: &Self) -> Ordering;
}

pub struct Clocked<M, C> {
    pub clock: C,
    pub message: M,
}

pub struct Request(u8);

pub struct RequestOk(u8);

pub struct Release(u8);

pub struct Processor<CS, U, C> {
    id: u8,
    latests: Vec<C>,
    requests: Vec<(C, u8)>,

    pub clock_service: CS,
    upcall: U,
}

pub mod event {
    pub struct Request;
    pub struct RequestOk;
    pub struct Release;
}

impl<CS: SendMessage<All, Request>, U, C> OnEvent<event::Request> for Processor<CS, U, C> {
    fn on_event(
        &mut self,
        event::Request: event::Request,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        // in this protocol we always expect to `Recv(_)` our own messages
        self.clock_service.send(All, Request(self.id))
    }
}

impl<CS, U, C: Clock> OnEvent<Recv<Clocked<Request, C>>> for Processor<CS, U, C> {
    fn on_event(
        &mut self,
        Recv(clocked): Recv<Clocked<Request, C>>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let Request(id) = clocked.message;
        let Some(Ordering::Less) = self.latests[id as usize].partial_cmp(&clocked.clock) else {
            return Ok(());
        };
        self.latests[id as usize] = clocked.clock.clone();
        if let Err(index) = self
            .requests
            .binary_search_by(|(clock, _)| clocked.clock.cmp(clock))
        {
            self.requests.insert(index, (clocked.clock, id))
        };
        // TODO
        Ok(())
    }
}

pub struct Replicated<E, N> {
    seq: u32,
    recv_sender: E,
    net: N,
}

impl<E: SendEvent<Recv<Clocked<M, u32>>>, M, N> SendEvent<Recv<M>> for Replicated<E, N> {
    fn send(&mut self, Recv(message): Recv<M>) -> anyhow::Result<()> {
        self.seq += 1;
        self.recv_sender.send(Recv(Clocked {
            clock: self.seq,
            message,
        }))
    }
}

impl<E, N: SendMessage<A, M>, A, M> SendMessage<A, M> for Replicated<E, N> {
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        self.net.send(dest, message)
    }
}

pub struct Causal<E, CS, N, C, M> {
    clock: C,
    pending_recv: VecDeque<Clocked<M, C>>,
    recv_sender: E,
    clock_service: CS,
    net: N,
}

impl<E, CS: SendEvent<Update<C>>, N, C: Clone, M> OnEvent<Recv<Clocked<M, C>>>
    for Causal<E, CS, N, C, M>
{
    fn on_event(
        &mut self,
        Recv(clocked): Recv<Clocked<M, C>>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        let update = Update {
            prev: self.clock.clone(),
            other: clocked.clock,
        };
        self.clock_service.send(update)
    }
}

impl<E: SendEvent<Recv<Clocked<M, C>>>, M, CS, N, C> OnEvent<UpdateOk<C>>
    for Causal<E, CS, N, C, M>
{
    fn on_event(
        &mut self,
        event: UpdateOk<C>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<E, CS, N: SendMessage<A, Clocked<M, C>>, A, C: Clone, M> SendMessage<A, M>
    for Causal<E, CS, N, C, M>
{
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        // in original work the clock may need to be incremented for each sending (as well as for
        // each receiving). omitting it is probably safe, while save much overhead in the context of
        // Boson
        self.net.send(
            dest,
            Clocked {
                clock: self.clock.clone(),
                message,
            },
        )
    }
}

pub struct Update<C> {
    prev: C,
    other: C,
}

pub struct UpdateOk<C>(C);

pub struct Lamport<E>(E, u8);

pub type LamportClock = (u32, u8); // (counter, processor id)

impl Clock for LamportClock {
    fn cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(self, other)
    }
}

impl<E: SendEvent<UpdateOk<LamportClock>>> SendEvent<Update<LamportClock>> for Lamport<E> {
    fn send(&mut self, update: Update<LamportClock>) -> anyhow::Result<()> {
        let counter = update.prev.0.max(update.other.0) + 1;
        self.0.send(UpdateOk((counter, self.1)))
    }
}

// cSpell:words lamport deque upcall
