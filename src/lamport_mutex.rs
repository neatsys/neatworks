// notes on this implementation of
// Time, Clocks, and the Ordering of Events in a Distributed System
// (Commun. ACM'78)
// despite the module name, this code also covers the other aspects of the
// original work, namely the logical clock definition and an abstraction of
// causally ordered communication
// (by the way, the module name is chosen to avoid using a too broad `mutex`)
// the `Clock` here is not lamport clock. it's the abstraction of causality i.e.
// the happens before relation, that specific implementation may or may not
// guarantee to have false positive (through the `PartialOrd` interface)
// this implementation assumes a potentially arbitrary fault setup. processors
// performs additional checks to ensure receiving ordered messages from a remote
// processor. the causally ordered communication model i.e. `Causal` also
// assumes an ordered and reliable underlying network i.e. `net`
// in this implementation updating clock is assumed to have potentially large
// overhead, so `Causal` is designed to work with asynchronous clock service
// instead of updating clock inline. the clock updating strategy is also adapted
// a little, as comment below. the caveat is that with this `Causal` processors
// may send sequential messages with the same clock value, instead of
// incrementing for every sending, so should use more >= instead of > to check
// for new messages
use std::{cmp::Ordering, collections::VecDeque, mem::replace};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::debug;

use crate::{
    event::{erased::OnEvent, OnTimer, SendEvent, Timer},
    net::{deserialize, events::Recv, All, SendMessage},
};

pub trait Clock: PartialOrd + Clone + Send + Sync + 'static {
    // this is different from just `+ Ord` above: a `+ Ord` (which would nullify the `PartialOrd`)
    // makes additional restriction on the *same* relation, while what we desired is yet another
    // relation that has total ordering i.e. the "arbitrary total ordering" that "break ties" in the
    // original work, hence the method name
    fn arbitrary_cmp(&self, other: &Self) -> Ordering;
    // as the original work states this total ordering must be aligned with the `PartialOrd`. for
    // clock types that have inherent total ordering (e.g. the integer type used by lamport clock),
    // the two ordering are indeed the same relation, as specified by the following blanket impl
}

impl<C: Ord + Clone + Send + Sync + 'static> Clock for C {
    fn arbitrary_cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Clocked<M, C> {
    pub clock: C,
    pub inner: M,
}

pub struct Replicated<E> {
    seq: u32,
    recv_sender: E,
}

impl<E> Replicated<E> {
    pub fn new(recv_sender: E) -> Self {
        Self {
            recv_sender,
            seq: 0,
        }
    }
}

impl<E: SendEvent<Recv<Clocked<M, u32>>>, M> SendEvent<Recv<M>> for Replicated<E> {
    fn send(&mut self, Recv(message): Recv<M>) -> anyhow::Result<()> {
        self.seq += 1;
        self.recv_sender.send(Recv(Clocked {
            clock: self.seq,
            inner: message,
        }))
    }
}

pub struct Causal<E, CS, N, C, M> {
    clock: C,
    pending_recv: Option<VecDeque<Clocked<M, C>>>,
    // erasing address type
    // this is definitely wrong, at least not right
    // sadly i cannot think out any other thing that works by now
    // even more sadly the message type is not erased completely: it still remains above
    #[allow(clippy::type_complexity)]
    pending_send: Vec<Box<dyn FnOnce(C, &mut N) -> anyhow::Result<()> + Send + Sync>>,

    recv_sender: E,
    clock_service: CS,
    net: N,
}

impl<E, CS: SendEvent<Update<C>>, N, C: Clone, M> Causal<E, CS, N, C, M> {
    pub fn new(
        clock_zero: C,
        recv_sender: E,
        mut clock_service: CS,
        net: N,
    ) -> anyhow::Result<Self> {
        // make sure all egress message is clocked > `clock_zero`
        let update = Update {
            prev: clock_zero.clone(),
            remote: clock_zero.clone(),
        };
        clock_service.send(update)?;
        Ok(Self {
            clock: clock_zero,
            recv_sender,
            clock_service,
            net,
            pending_recv: Some(Default::default()),
            pending_send: Default::default(),
        })
    }
}

impl<E: SendEvent<Recv<Clocked<M, C>>>, CS: SendEvent<Update<C>>, N, C: Clone, M>
    OnEvent<Recv<Clocked<M, C>>> for Causal<E, CS, N, C, M>
{
    fn on_event(
        &mut self,
        Recv(clocked): Recv<Clocked<M, C>>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        debug!("recv clocked");
        if let Some(pending_recv) = &mut self.pending_recv {
            debug!("recv clocked pending");
            pending_recv.push_back(clocked);
            return Ok(());
        }
        self.pending_recv = Some(Default::default());
        let update = Update {
            prev: self.clock.clone(),
            remote: clocked.clock.clone(),
        };
        self.clock_service.send(update)?;
        debug!("forward recv clocked");
        self.recv_sender.send(Recv(clocked))
    }
}

impl<
        E,
        CS,
        N: SendMessage<A, Clocked<M, C>>,
        A: Send + Sync + 'static,
        C: Clone,
        M: Send + Sync + 'static,
    > SendMessage<A, M> for Causal<E, CS, N, C, M>
{
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        if self.pending_recv.is_some() {
            self.pending_send.push(Box::new(move |clock, net| {
                let clocked = Clocked {
                    inner: message,
                    clock,
                };
                net.send(dest, clocked)
            }));
            return Ok(());
        }
        // IR2 (a) if event a is the sending of a message m by process p_i then the message m
        // contains a timestamp T_m = C_i(a)
        // as what condition C1 expects, we probably should increment `self.clock` before sending.
        // omitted for potential large overhead in certain clock implementations, hopefully safe to
        // do so in the presence of the extra `+ 1` below
        let clocked = Clocked {
            clock: self.clock.clone(),
            inner: message,
        };
        debug!("send clocked");
        self.net.send(dest, clocked)
    }
}

impl<E: SendEvent<Recv<Clocked<M, C>>>, M, CS: SendEvent<Update<C>>, N, C: Clock>
    OnEvent<UpdateOk<C>> for Causal<E, CS, N, C, M>
{
    fn on_event(&mut self, UpdateOk(clock): UpdateOk<C>, _: &mut impl Timer) -> anyhow::Result<()> {
        anyhow::ensure!(matches!(
            clock.partial_cmp(&self.clock),
            Some(Ordering::Greater)
        ));
        self.clock = clock;
        let Some(pending_recv) = &mut self.pending_recv else {
            anyhow::bail!("missing pending recv queue")
        };
        if let Some(clocked) = pending_recv.pop_front() {
            debug!("pended recv clocked popped");
            let update = Update {
                prev: self.clock.clone(),
                remote: clocked.clock.clone(),
            };
            self.clock_service.send(update)?;
            self.recv_sender.send(Recv(clocked))?
        } else {
            debug!("pended recv clock cleared");
            self.pending_recv = None
        }
        for send in self.pending_send.drain(..) {
            send(self.clock.clone(), &mut self.net)?
        }
        Ok(())
    }
}

impl<E, CS, N, C, M> OnTimer for Causal<E, CS, N, C, M> {
    fn on_timer(&mut self, _: crate::event::TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        unreachable!()
    }
}

pub struct Update<C> {
    pub prev: C,
    pub remote: C,
}

pub struct UpdateOk<C>(pub C);

pub struct Lamport<E>(pub E, pub u8);

pub type LamportClock = (u32, u8); // (counter, processor id)

impl<E: SendEvent<UpdateOk<LamportClock>>> SendEvent<Update<LamportClock>> for Lamport<E> {
    fn send(&mut self, update: Update<LamportClock>) -> anyhow::Result<()> {
        // IR2 (b) upon receiving a message m, process p_j sets C_j greater than or equal to its
        // present value and greater than T_m
        // this would sound like `update.prev.0.max(update.remote.0 + 1)`, taking this alternative
        // because `self.clock` is used by events *after* this receiving, so increment is always
        // expected according to C1
        // caveat: there are (intentional) loopback messages in the mutex case, should be careful
        // about how this modification interacts with that fact
        let counter = update.prev.0.max(update.remote.0) + 1;
        self.0.send(UpdateOk((counter, self.1)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Request(u8),
    RequestOk(u8),
    Release(u8),
}

// `V` for verifiable
pub struct Processor<CN, U, C, const V: bool = false> {
    id: u8,
    latests: Vec<C>,
    requests: Vec<(C, u8)>,
    requesting: bool,

    pub causal_net: CN,
    upcall: U,
}

impl<CN, U, C, const V: bool> Processor<CN, U, C, V> {
    pub fn new(
        id: u8,
        num_processor: usize,
        clock_zero: impl Fn(u8) -> C,
        causal_net: CN,
        upcall: U,
    ) -> Self {
        Self {
            id,
            causal_net,
            upcall,
            latests: (0..num_processor as u8).map(clock_zero).collect(),
            requests: Default::default(),
            requesting: false,
        }
    }
}

pub mod events {
    pub use super::{Update, UpdateOk};

    pub struct Request;
    pub struct RequestOk;
    pub struct Release;
}

pub trait Net: SendMessage<u8, Message> + SendMessage<All, Message> {}
impl<T: SendMessage<u8, Message> + SendMessage<All, Message>> Net for T {}

impl<CN: SendMessage<All, Message>, U, C, const V: bool> OnEvent<events::Request>
    for Processor<CN, U, C, V>
{
    fn on_event(
        &mut self,
        events::Request: events::Request,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        let replaced = replace(&mut self.requesting, true);
        anyhow::ensure!(!replaced, "concurrent request");
        // in this protocol we always expect to loopback `Recv(_)` our own messages
        // the Request will be added into ourselves queue there
        self.causal_net.send(All, Message::Request(self.id))
    }
}

impl<
        CN: SendMessage<u8, Message> + SendMessage<All, Message>,
        U: SendEvent<events::RequestOk>,
        C: Clock,
        const V: bool,
    > OnEvent<Recv<Clocked<Message, C>>> for Processor<CN, U, C, V>
{
    fn on_event(
        &mut self,
        Recv(message): Recv<Clocked<Message, C>>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        debug!("{:?}", message.inner);
        let id = match &message.inner {
            Message::Request(id) | Message::RequestOk(id) | Message::Release(id) => *id,
        };
        let Some(Ordering::Greater | Ordering::Equal) =
            message.clock.partial_cmp(&self.latests[id as usize])
        else {
            return Ok(());
        };
        self.latests[id as usize] = message.clock.clone();
        match message.inner {
            Message::Request(_) => {
                if let Err(index) = self
                    .requests
                    .binary_search_by(|(clock, _)| message.clock.arbitrary_cmp(clock))
                {
                    self.requests.insert(index, (message.clock, id))
                };
                if V {
                    self.causal_net.send(All, Message::RequestOk(self.id))?;
                } else {
                    self.causal_net.send(id, Message::RequestOk(self.id))?;
                }
            }
            Message::RequestOk(_) => {}
            Message::Release(_) => {
                if let Some(index) = self
                    .requests
                    .iter()
                    .position(|(_, other_id)| *other_id == id)
                {
                    let (clock, _) = self.requests.remove(index);
                    // not so sure whether faulty processors can cause this break on other processors
                    // anyway let's go with this for now, since it should always be the case for the
                    // evaluated path
                    anyhow::ensure!(message.clock.arbitrary_cmp(&clock).is_gt());
                }
            }
        }
        if self.requesting {
            self.check_requested()?
        }
        Ok(())
    }
}

impl<CN, U: SendEvent<events::RequestOk>, C: Clock, const V: bool> Processor<CN, U, C, V> {
    fn check_requested(&mut self) -> anyhow::Result<()> {
        // self Request, requesting == true
        // all others Request are Release, while loopback Request still not received
        let Some((clock, id)) = self.requests.first() else {
            return Ok(());
        };
        if *id == self.id
            && self.latests.iter().all(|other_clock| {
                matches!(
                    other_clock.partial_cmp(clock),
                    Some(Ordering::Greater | Ordering::Equal)
                )
            })
        {
            let replaced = replace(&mut self.requesting, false);
            anyhow::ensure!(replaced);
            self.upcall.send(events::RequestOk)?
        }
        Ok(())
    }
}

impl<CN: SendMessage<All, Message>, U, C, const V: bool> OnEvent<events::Release>
    for Processor<CN, U, C, V>
{
    fn on_event(
        &mut self,
        events::Release: events::Release,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        // consider further check whether we have requested
        anyhow::ensure!(!self.requesting, "release while requesting");
        // in this protocol we always expect to loopback `Recv(_)` our own messages
        // the Request will be added into ourselves queue there
        self.causal_net.send(All, Message::Release(self.id))
    }
}

impl<CN, U, C, const V: bool> OnTimer for Processor<CN, U, C, V> {
    fn on_timer(&mut self, _: crate::event::TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        unreachable!()
    }
}

pub type MessageNet<N, C> = crate::net::MessageNet<N, Clocked<Message, C>>;

pub trait SendRecvEvent<C>: SendEvent<Recv<Clocked<Message, C>>> {}
impl<T: SendEvent<Recv<Clocked<Message, C>>>, C> SendRecvEvent<C> for T {}

pub fn on_buf<C: DeserializeOwned>(
    buf: &[u8],
    sender: &mut impl SendRecvEvent<C>,
) -> anyhow::Result<()> {
    sender.send(Recv(deserialize(buf)?))
}

// cSpell:words lamport deque upcall
// cSpell:ignore commun
