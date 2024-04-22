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
// `Causal`, as the network middleware that assigns proper logical clock value
// to each outgoing message, has defined a slightly different event model that
// considers the processor state machine as blackbox and only manages message
// sending/receiving events, which is considered to be more practical than the
// definition in the original work from an engineering aspect. the `Causal` also
// assumes that updating clock may have potentially large overhead and works
// with asynchronous clock service instead of updating clock inline
// last but not least, a modified version of the mutex protocol that is
// prepared for tolerating arbitrary faulty behaviors is appended. check the
// note below for details
use std::{cmp::Ordering, collections::VecDeque, mem::replace};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    event::{
        erased::{events::Init, OnEvent},
        OnTimer, SendEvent, Timer,
    },
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

// a trivial causality middleware that assumes a replicated network i.e. all
// messages are received in the same order on everyone
// it does not assign clock values for outgoing messages (so it does not impl
// `SendMessage<_, _>`), just assign a sequence number for every incoming
// message. the sequence number will be the same for the same message on every
// recipient, as guaranteed by the relied replication protocol
// the clock value type i.e. `C` above is `u32`
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

// a network middleware that tracks and specified causal relation of network
// events i.e. message sending/receiving
// the causal relation is defined as
// * messages are received sequentially i.e. every receiving event happens after
//   the event of receiving its previous message
// * sending events are effectively grouped into sending batches. a sending
//   batch consists of sending events that happen between two consecutive
//   receiving events. sending events of the same batch is the *same* event:
//   they share the same clock value, and the aggregated event happens after the
//   immediately predecessor receiving event
// in another word the event "timeline" looks like this
//   [recv] <- [send 3 message] <- [recv] <- [recv] <- [send 1 message] <- [recv] ...
// each of the <- is the partial ordering between local events respecting
// execution order i.e. partial ordering across processors are not shown
// this middleware guarantees to provide real time correspondence of the
// timeline above to the network user. for example, if a processors is observed
// to have the timeline above, then it must have send all three messages before
// processing the second receiving message, the other cases e.g. it sends one
// of the three messages after the second receiving, but a stalled clock value
// is assigned to that message, are guaranteed to be impossible
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
    pub fn new(clock_zero: C, recv_sender: E, clock_service: CS, net: N) -> anyhow::Result<Self> {
        // make sure all egress message is clocked > `clock_zero`
        Ok(Self {
            clock: clock_zero,
            recv_sender,
            clock_service,
            net,
            pending_recv: None,
            pending_send: Default::default(),
        })
    }
}

impl<E, CS: SendEvent<Update<C>>, N, C: Clone, M> OnEvent<Init> for Causal<E, CS, N, C, M> {
    fn on_event(&mut self, Init: Init, _: &mut impl Timer) -> anyhow::Result<()> {
        let update = Update {
            prev: self.clock.clone(),
            remote: self.clock.clone(),
        };
        self.clock_service.send(update)?;
        self.pending_recv = Some(Default::default());
        Ok(())
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
        // IR2. (b) upon receiving a message m, process P_j sets C_j greater than or equal to its
        // present value and greater than T_m
        // this would sound like `update.prev.0.max(update.remote.0 + 1)`, but the definition of
        // `Update` is to "return the clock value of the event happens after observing `remote`
        // based on `prev`", so there is actually an implicit IR1 follows
        // IR1. Each process P_i increments C_i between any two successive events.
        // and this would be a little bit optimization over the naive `_.max(_ + 1) + 1`
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

#[derive(Debug)]
pub struct Processor<CN, U, C> {
    id: u8,
    latests: Vec<C>,
    requests: Vec<(C, u8)>,
    requesting: bool,

    causal_net: CN,
    upcall: U,
}

impl<CN, U, C> Processor<CN, U, C> {
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
            latests: (0..num_processor as u8).map(&clock_zero).collect(),
            requests: Default::default(),
            requesting: false,
        }
    }
}

pub mod events {
    pub use super::{Update, UpdateOk};

    #[derive(Debug)]
    pub struct Request;
    #[derive(Debug)]
    pub struct RequestOk;
    #[derive(Debug)]
    pub struct Release;
}

pub trait Net: SendMessage<u8, Message> + SendMessage<All, Message> {}
impl<T: SendMessage<u8, Message> + SendMessage<All, Message>> Net for T {}

impl<CN: SendMessage<All, Message>, U, C> OnEvent<events::Request> for Processor<CN, U, C> {
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

impl<CN: SendMessage<u8, Message>, U: SendEvent<events::RequestOk>, C: Clock>
    OnEvent<Recv<Clocked<Message, C>>> for Processor<CN, U, C>
{
    fn on_event(
        &mut self,
        Recv(message): Recv<Clocked<Message, C>>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        debug!("{:?}", message.inner);
        self.handle_clocked(message, |id| id)?;
        if self.requesting {
            self.check_requested()?
        }
        Ok(())
    }
}

impl<CN, U, C: Clock> Processor<CN, U, C> {
    fn handle_clocked<A>(
        &mut self,
        message: Clocked<Message, C>,
        into_addr: impl Fn(u8) -> A,
    ) -> anyhow::Result<()>
    where
        CN: SendMessage<A, Message>,
    {
        let id = match &message.inner {
            Message::Request(id) | Message::RequestOk(id) | Message::Release(id) => *id,
        };
        let Some(Ordering::Greater | Ordering::Equal) =
            message.clock.partial_cmp(&self.latests[id as usize])
        else {
            warn!("out of order clock received from {id}");
            return Ok(());
        };
        self.latests[id as usize] = message.clock.clone();
        match message.inner {
            Message::Request(_) => {
                if let Err(index) = self
                    .requests
                    .binary_search_by(|(clock, _)| clock.arbitrary_cmp(&message.clock))
                {
                    self.requests.insert(index, (message.clock, id))
                };
                self.causal_net
                    .send(into_addr(id), Message::RequestOk(self.id))?;
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
        Ok(())
    }
}

impl<CN, U: SendEvent<events::RequestOk>, C: Clock> Processor<CN, U, C> {
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

impl<CN: SendMessage<All, Message>, U, C> OnEvent<events::Release> for Processor<CN, U, C> {
    fn on_event(
        &mut self,
        events::Release: events::Release,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        // consider further check whether we have requested
        anyhow::ensure!(!self.requesting, "release while requesting");
        self.causal_net.send(All, Message::Release(self.id))
    }
}

impl<CN, U, C> OnTimer for Processor<CN, U, C> {
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

// the modified mutex protocol that can tolerate arbitrary faulty processors
// the protocol is specifically designed for two kinds of faulty behaviors:
// * sending messages disregard their clock values. another word, the messages
//   that sent later does not contain clock values that is greater (could be
//   either less or equal or incomparable). this is actually solved above: there
//   are explicit checks on clock values upon receiving messages, and any out of
//   order incoming message is ignored
// * completely disregard the protocol, claim to have the lock at arbitrary
//   time. to prevent this we have added a message `Ordered` that is sent by
//   every processor for every `Request`, both of itself and of others. the
//   order of a `Request` to get a lock relative to the other `Request`s, in
//   another word, the `Release` of what set of `Request`s must be present
//   before this `Request` can have the lock. f + 1 signed `Ordered`s, paired
//   with the corresponded `Release` of the mentioned `Request`s, construct a
//   so called "acquisition proof", which indicates at least one honest
//   processor agrees the `Request` to get lock in the presence of at most f
//   faulty processors
// the timings are adjusted around this new `Ordered` and acquisition proof
// mechanisms, additionally, the `RequestOk` message is changed to be broadcast
// to every processor instead of directly reply to the `Request` sender, in
// order to enable processors to be able to keep track of the progress of the
// `Request`s of other processors in addition to itself's and send `Ordered`
// when necessary
pub mod verifiable {
    // finally decided to duplicate some code to above
    // 27 hours until ddl, should be forgivable

    use std::{cmp::Ordering, collections::HashMap};

    use serde::{de::DeserializeOwned, Deserialize, Serialize};
    use tracing::debug;

    use crate::{
        crypto::{
            peer::{Crypto, Verifiable},
            DigestHash,
        },
        event::{erased::OnEvent, OnTimer, SendEvent, Timer},
        net::{deserialize, events::Recv, Addr, All, SendMessage},
        worker::Submit,
    };

    use super::{events, Clock, Clocked};

    #[derive(Debug, Hash, Serialize, Deserialize)]
    pub struct Ordered<C> {
        clock: C,
        after: Vec<(C, u8)>,
        id: u8,
    }

    #[derive(Debug, derive_more::Deref, derive_more::DerefMut)]
    pub struct Processor<CN, N, U, C> {
        num_faulty: usize,
        #[deref]
        #[deref_mut]
        inner: super::Processor<CN, U, C>,
        last_ordered: C,
        proof: HashMap<u8, Verifiable<Ordered<C>>>,
        net: N,
    }

    impl<CN, N, U, C> Processor<CN, N, U, C> {
        pub fn new(
            id: u8,
            num_processor: usize,
            num_faulty: usize,
            clock_zero: impl Fn(u8) -> C,
            causal_net: CN,
            net: N,
            upcall: U,
        ) -> Self {
            let inner = super::Processor::new(id, num_processor, &clock_zero, causal_net, upcall);
            Self {
                inner,
                num_faulty,
                net,
                last_ordered: clock_zero(id),
                proof: Default::default(),
            }
        }
    }

    impl<CN: SendMessage<All, super::Message>, N, U, C> OnEvent<events::Request>
        for Processor<CN, N, U, C>
    {
        fn on_event(
            &mut self,
            events::Request: events::Request,
            timer: &mut impl Timer,
        ) -> anyhow::Result<()> {
            self.inner.on_event(events::Request, timer)
        }
    }

    impl<
            CN: SendMessage<All, super::Message>,
            N: SendMessage<u8, Ordered<C>>,
            U: SendEvent<events::RequestOk>,
            C: Clock,
        > OnEvent<Recv<Clocked<super::Message, C>>> for Processor<CN, N, U, C>
    {
        fn on_event(
            &mut self,
            Recv(message): Recv<Clocked<super::Message, C>>,
            _: &mut impl Timer,
        ) -> anyhow::Result<()> {
            debug!("{:?}", message.inner);
            self.handle_clocked(message, |_| All)?;
            self.check_requested()
        }
    }

    impl<CN, N: SendMessage<u8, Ordered<C>>, U: SendEvent<events::RequestOk>, C: Clock>
        Processor<CN, N, U, C>
    {
        fn check_requested(&mut self) -> anyhow::Result<()> {
            // println!("check requested");
            for (clock, id) in &self.inner.requests {
                // println!("check requested {id}");
                if clock.arbitrary_cmp(&self.last_ordered).is_le() {
                    // println!("skip ordered clock");
                    continue;
                }
                if self.latests.iter().all(|other_clock| {
                    matches!(
                        other_clock.partial_cmp(clock),
                        Some(Ordering::Greater | Ordering::Equal)
                    )
                }) {
                    let ordered = Ordered {
                        clock: clock.clone(),
                        after: self.requests.clone(), // TODO trim the later requests
                        id: self.id,
                    };
                    // println!("ordered");
                    self.net.send(*id, ordered)?;
                    self.last_ordered = clock.clone()
                } else {
                    break;
                }
            }
            if self.requesting {
                if let Some((clock, id)) = self.requests.first() {
                    if *id == self.id
                        && self
                            .proof
                            .values()
                            .filter(|message| {
                                // probably not Greater, but who cares
                                matches!(message.clock.partial_cmp(clock), Some(Ordering::Equal))
                            })
                            .count()
                            > self.num_faulty
                    {
                        self.requesting = false;
                        self.upcall.send(events::RequestOk)?
                    }
                }
            }
            Ok(())
        }
    }

    impl<CN: SendMessage<All, super::Message>, N, U, C> OnEvent<events::Release>
        for Processor<CN, N, U, C>
    {
        fn on_event(
            &mut self,
            events::Release: events::Release,
            timer: &mut impl Timer,
        ) -> anyhow::Result<()> {
            self.inner.on_event(events::Release, timer)
        }
    }

    impl<CN, N: SendMessage<u8, Ordered<C>>, U: SendEvent<events::RequestOk>, C: Clock>
        OnEvent<Recv<Verifiable<Ordered<C>>>> for Processor<CN, N, U, C>
    {
        fn on_event(
            &mut self,
            Recv(ordered): Recv<Verifiable<Ordered<C>>>,
            _: &mut impl Timer,
        ) -> anyhow::Result<()> {
            // println!("recv ordered");
            if let Some(other_ordered) = self.proof.get(&ordered.id) {
                if matches!(
                    other_ordered.clock.partial_cmp(&ordered.clock),
                    Some(Ordering::Greater | Ordering::Equal)
                ) {
                    // println!("discard earlier ordered");
                    return Ok(());
                }
            }
            self.proof.insert(ordered.id, ordered);
            self.check_requested()
        }
    }

    impl<CN, N, U, C> OnTimer for Processor<CN, N, U, C> {
        fn on_timer(
            &mut self,
            timer_id: crate::event::TimerId,
            timer: &mut impl Timer,
        ) -> anyhow::Result<()> {
            self.inner.on_timer(timer_id, timer)
        }
    }

    #[derive(Debug, Serialize, Deserialize, derive_more::From)]
    pub enum Message<C> {
        Clocked(Clocked<super::Message, C>),
        Ordered(Verifiable<Ordered<C>>),
    }

    pub type MessageNet<N, C> = crate::net::MessageNet<N, Message<C>>;

    pub fn on_buf<C: DeserializeOwned>(
        buf: &[u8],
        clocked_sender: &mut impl super::SendRecvEvent<C>,
        sender: &mut impl SendEvent<Recv<Verifiable<Ordered<C>>>>,
    ) -> anyhow::Result<()> {
        match deserialize(buf)? {
            Message::Clocked(message) => clocked_sender.send(Recv(message)),
            Message::Ordered(message) => sender.send(Recv(message)),
        }
    }

    pub struct SignOrdered<CW, E> {
        crypto_worker: CW,
        _m: std::marker::PhantomData<E>,
    }

    impl<CW, E> SignOrdered<CW, E> {
        pub fn new(crypto_worker: CW) -> Self {
            Self {
                crypto_worker,
                _m: Default::default(),
            }
        }
    }

    impl<CW: Submit<Crypto, E>, E: SendMessage<A, Verifiable<Ordered<C>>>, A: Addr, C>
        SendMessage<A, Ordered<C>> for SignOrdered<CW, E>
    where
        Ordered<C>: DigestHash + Send + Sync + 'static,
    {
        fn send(&mut self, dest: A, message: Ordered<C>) -> anyhow::Result<()> {
            self.crypto_worker.submit(Box::new(move |crypto, net| {
                net.send(dest, crypto.sign(message))
            }))
        }
    }
}

// cSpell:words lamport deque upcall blackbox
// cSpell:ignore commun
