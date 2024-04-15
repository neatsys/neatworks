// a construction that enables connection reusing
// the client side of a connection informs its server address to the connected
// server upon establishing connection, so if later a message need to be
// delivered in the opposite direction, it can go through the existing
// connection
// a few design choice has been explored, and here i note the rationale for
// current tradeoffs
// there were several attempts of proactively throttling/evicting connections
// at this dispatching layer. those did not work well. on the one hand, we don't
// have so much information that is useful for congestion control from our
// inputs i.e. `SendMessage<...>` calls. on the other hand, we are not the only
// consumer of underlying network resources, as others may be e.g. bulk service,
// and we have no idea of how much resource should be preserved for them. in
// conclusion, it's not a good idea to integrate resource management here
// the only thing remains to matter is about resource leaking. there is probably
// no more port leaking going on here. another thing is that if we close too
// many TCP connections in a short interval, there could be more TIME_WAIT
// connections than the amount kernel would like to track i.e. 32768, and the
// unexpected faster recycling may cause data corruption. since this dispatcher
// does not decide the timing of closing connections (the underlying
// `impl Protocol` does), and (again) we don't know how frequent others are
// closing connections, we are not doing anything specific to this
// the dispatcher comes with inherit overhead: each outgoing message must go
// through two channels, the first one from `DispatchNet` to `Dispatch` and the
// second one from `Dispatch` to the corresponded `write_task`. the first
// queuing is necessary for maintaining a global view of all connections in the
// `Dispatch`. consider the fact that kernel already always maintains a
// connection table (and yet another queuing layer), i generally don't satisfy
// with this solution
use std::{collections::HashMap, time::Duration};

use derive_where::derive_where;

use rand::{thread_rng, Rng};
use tracing::{debug, warn};

use crate::event::{
    OnEventRichTimer as OnEvent, RichTimer as Timer, SendEvent, SendEventOnce, TimerId,
};

use super::{Addr, Buf, IterAddr, SendMessage};

#[derive_where(Debug; E, P, P::Sender, A)]
pub struct Dispatch<E, P: Protocol<A, B>, A, B, F> {
    protocol: P,
    connections: HashMap<A, Connection<P::Sender, B>>,
    seq: u32,
    close_sender: E,
    #[derive_where(skip)]
    on_buf: F,
}

#[derive_where(Debug; E)]
enum Connection<E, B> {
    BackingOff(TimerId, #[derive_where(skip)] Vec<B>),
    BackedOff(E, u32),
}

impl<E, P: Protocol<A, B>, A, B, F> Dispatch<E, P, A, B, F> {
    pub fn new(protocol: P, on_buf: F, close_sender: E) -> anyhow::Result<Self> {
        Ok(Self {
            protocol,
            close_sender,
            on_buf,
            connections: Default::default(),
            seq: 0,
        })
    }
}

// go with typed event for this state machine because it (1) owns a sender that
// (2) sends to itself and (3) must be `Clone` at the same time
// this is exactly "the horror" of type erasure
#[derive(derive_more::From)]
pub enum Event<P: Protocol<A, B>, A, B> {
    Incoming(Incoming<P::Incoming>),
    Outgoing(Outgoing<A, B>),
    OutgoingBackoff(OutgoingBackoff<A>),
    Closed(Closed<A>),
}

pub struct Closed<A>(A, u32);

pub struct CloseGuard<E, A>(E, Option<A>, u32);

impl<E: SendEventOnce<Closed<A>>, A: Addr> CloseGuard<E, A> {
    pub fn close(self, addr: A) -> anyhow::Result<()> {
        if let Some(also_addr) = self.1 {
            anyhow::ensure!(addr == also_addr)
        }
        self.0.send_once(Closed(addr, self.2))
    }
}

pub trait Protocol<A, B> {
    fn local_addr(&self) -> Option<A>;

    type Sender: SendEvent<B>;

    fn connect<E: SendEventOnce<Closed<A>> + Send + 'static>(
        &self,
        remote: A,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        close_guard: CloseGuard<E, A>,
    ) -> Self::Sender;

    type Incoming;

    fn accept<E: SendEventOnce<Closed<A>> + Send + 'static>(
        connection: Self::Incoming,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        close_guard: CloseGuard<E, A>,
    ) -> Option<(A, Self::Sender)>;
}

pub struct Outgoing<A, B>(A, B);

#[derive(Clone)]
pub struct Net<E, A>(pub E, std::marker::PhantomData<A>);
// mark address type so the following implementations not conflict

impl<E, A> From<E> for Net<E, A> {
    fn from(value: E) -> Self {
        Self(value, Default::default())
    }
}

impl<E: SendEvent<Outgoing<A, B>>, A, B> SendMessage<A, B> for Net<E, A> {
    fn send(&mut self, dest: A, message: B) -> anyhow::Result<()> {
        self.0.send(Outgoing(dest, message))
    }
}

impl<E: SendEvent<Outgoing<A, B>>, A, B: Buf> SendMessage<IterAddr<'_, A>, B> for Net<E, A> {
    fn send(&mut self, dest: IterAddr<'_, A>, message: B) -> anyhow::Result<()> {
        for addr in dest.0 {
            SendMessage::send(self, addr, message.clone())?
        }
        Ok(())
    }
}

impl<
        E: SendEventOnce<Closed<A>> + Clone + Send + 'static,
        P: Protocol<A, B>,
        A: Addr + Ord,
        B: Buf,
        F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    > OnEvent for Dispatch<E, P, A, B, F>
{
    type Event = Event<P, A, B>;

    fn on_event(
        &mut self,
        event: Self::Event,
        timer: &mut impl Timer<Event = Event<P, A, B>>,
    ) -> anyhow::Result<()> {
        match event {
            Event::Outgoing(event) => self.handle_outgoing(event, timer),
            Event::OutgoingBackoff(event) => self.handle_outgoing_backoff(event, timer),
            Event::Incoming(event) => self.handle_incoming(event, timer),
            Event::Closed(event) => self.handle_closed(event, timer),
        }
    }
}

#[derive(Clone)]
pub struct OutgoingBackoff<A>(A);

impl<E, P: Protocol<A, B>, A: Addr + Ord, B: Buf, F> Dispatch<E, P, A, B, F> {
    fn handle_outgoing(
        &mut self,
        Outgoing(remote, buf): Outgoing<A, B>,
        timer: &mut impl Timer<Event = Event<P, A, B>>,
    ) -> anyhow::Result<()> {
        // currently it seems easier to tolerate loopback here than to require user to not do so
        // anyhow::ensure!(Some(remote.clone()) != self.protocol.local_addr());
        // when i say "tolerate" i mean it just works, not sure whether that is always the case, so
        // always keep an eye on loopback messages when things go unexpected
        if let Some(connection) = self.connections.get_mut(&remote) {
            match connection {
                Connection::BackingOff(_, pending) => {
                    pending.push(buf);
                    return Ok(());
                }
                Connection::BackedOff(sender, _) => {
                    let Err(err) = sender.send(buf.clone()) else {
                        return Ok(()); // the fastest path is here
                    };
                    warn!(">=> {remote:?} connection discontinued: {err}");
                    self.connections.remove(&remote);
                    // in an ideal world the SendError will return the buf back to us, and we can
                    // directly reuse that in below, saving a `clone` above especially for fast path
                    // but that's not happening as noted in `event::session` module, so far we have
                    // to settle on either this cloning solution or directly give up this message
                    // cloning should be fine since `Buf` is expected to be trivially cloned, but
                    // still will consider that alternative if performance is much affected
                }
            }
        }
        // instead of directly connecting remote, we first go through a random backoff, for two
        // purposes:
        // * a better than nothing throttling against burst of outgoing messages, which is common
        //   during system startup
        // * certain upper layer protocol may contain workload pattern that two servers accidentally
        //   send to each other almost at the same time. without this backoff it's more likely to
        //   result in two half-closed connections
        // (two half-closed connections is not something so bad that we must avoid, just trying to
        // make the normal case as simply/straightforward as possible, reducing maintenance
        // overhead. also QUIC connection is pretty computational expensive)
        // should we skip backoff if we just removed the old connection?
        let outgoing = OutgoingBackoff(remote.clone());
        let timer_id = timer.set(
            thread_rng().gen_range(Duration::ZERO..Duration::from_millis(100)),
            // + if Some(remote.clone()) >= self.protocol.local_addr() {
            //     Duration::ZERO // lower address always connects first
            // } else {
            //     Duration::from_millis(100)
            // },
            move || Event::OutgoingBackoff(outgoing.clone()),
        )?;
        // not saying that duplicated connections can be completely removed through this mechanism.
        // say the lower address happens to send `Outgoing` right at 300ms after the higher address
        // do so, there still a chance to have simultaneous connecting
        // just that this kind of workload is expected to be much rare than both end connect at the
        // same time
        self.connections
            .insert(remote, Connection::BackingOff(timer_id, vec![buf]));
        // TODO make the random backoff optional, shortcut to `handle_outgoing_backoff` from here
        // if user opt-out
        Ok(())
    }
}

impl<
        E: SendEventOnce<Closed<A>> + Clone + Send + 'static,
        P: Protocol<A, B>,
        A: Addr,
        B: Buf,
        F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    > Dispatch<E, P, A, B, F>
{
    fn handle_outgoing_backoff(
        &mut self,
        OutgoingBackoff(remote): OutgoingBackoff<A>,
        timer: &mut impl Timer,
    ) -> anyhow::Result<()> {
        let Some(Connection::BackingOff(timer_id, pending)) = self.connections.remove(&remote)
        else {
            anyhow::bail!("missing backing off connection")
        };
        timer.unset(timer_id)?;
        self.seq += 1;
        let close_guard = CloseGuard(self.close_sender.clone(), Some(remote.clone()), self.seq);
        let mut sender = self
            .protocol
            .connect(remote.clone(), self.on_buf.clone(), close_guard);
        for buf in pending {
            if sender.send(buf).is_err() {
                warn!(">=> {remote:?} new outgoing connection immediately fail");
                // we don't try again in such case since the remote is probably never reachable
                // anymore
                // not sure whether this should be considered as a fatal error though. if this is
                // happening, will it happen for every following outgoing connection regardless
                // remote address?
                return Ok(());
            }
        }
        self.connections
            .insert(remote, Connection::BackedOff(sender, self.seq));
        Ok(())
    }
}

pub struct Incoming<T>(pub T);

impl<
        E: SendEventOnce<Closed<A>> + Clone + Send + 'static,
        P: Protocol<A, B>,
        A: Addr,
        B: Buf,
        F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    > Dispatch<E, P, A, B, F>
{
    fn handle_incoming(
        &mut self,
        Incoming(event): Incoming<P::Incoming>,
        timer: &mut impl Timer,
    ) -> anyhow::Result<()> {
        self.seq += 1;
        let close_guard = CloseGuard(self.close_sender.clone(), None, self.seq);
        let Some((remote, mut sender)) = P::accept(event, self.on_buf.clone(), close_guard) else {
            return Ok(());
        };

        if matches!(
            self.connections.get(&remote),
            Some(Connection::BackedOff(..))
        ) {
            // always prefer to keep the connection created locally
            // the connection in `self.connections` may not be created locally, but the incoming
            // connection is probably created remotely
            // for loopback connection, dropping the incoming sender so that the `on_buf` we passed
            // to `connect` will never be called, only the `on_buf` above will receive messages, so
            // it seems to just work
            if Some(remote.clone()) != self.protocol.local_addr() {
                warn!(
                    "{:?} <=< {remote:?} incoming connection from connected address",
                    self.protocol.local_addr()
                );
            }
            return Ok(());
        }
        if let Some(Connection::BackingOff(timer_id, pending)) = self.connections.remove(&remote) {
            timer.unset(timer_id)?;
            for buf in pending {
                if sender.send(buf).is_err() {
                    warn!(
                        "{:?} <=< {remote:?} new incoming connection fail to send pending message",
                        self.protocol.local_addr()
                    );
                    // we are assuming if an incoming connection doesn't work, an outgoing one (that
                    // will be set up after the backoff) probably won't work either, so we don't
                    // bother to try
                    return Ok(());
                }
            }
        }
        self.connections
            .insert(remote, Connection::BackedOff(sender, self.seq));
        Ok(())
    }
}

impl<E, P: Protocol<A, B>, A: Addr, B, F> Dispatch<E, P, A, B, F> {
    fn handle_closed(
        &mut self,
        Closed(addr, seq): Closed<A>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some(Connection::BackedOff(_, other_seq)) = self.connections.get(&addr) {
            if *other_seq == seq {
                debug!(">=> {addr:?} outgoing connection closed");
                self.connections.remove(&addr);
            }
        }
        // otherwise it could be like
        // 1. underlying `Protocol` decides to close a connection, thus send `Closed` and exit
        // 2. dispatcher handles an `Outgoing` and realize that the sender is not sendable anymore,
        //    so `connect` again and replace the old one (after backoff expired)
        // 3. dispatcher handles the `Closed` and finds sequence number mismatches
        // something we have to deal with in a weakly synchronized system
        Ok(())
    }
}
