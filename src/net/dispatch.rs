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
use std::collections::{hash_map::Entry, HashMap};

use derive_where::derive_where;

use tracing::{debug, warn};

use crate::event::{
    erased, OnEventRichTimer as OnEvent, RichTimer as Timer, SendEvent, SendEventOnce,
    UnreachableTimer,
};

use super::{Addr, Buf, IterAddr, SendMessage};

#[derive_where(Debug; E, P, P::Sender, A)]
pub struct Dispatch<E, P: Protocol<A, B>, A, B, F> {
    protocol: P,
    connections: HashMap<A, Connection<P::Sender>>,
    seq: u32,
    close_sender: E,
    #[derive_where(skip)]
    on_buf: F,
}

#[derive(Debug)]
struct Connection<E> {
    sender: E,
    seq: u32,
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
// i.e. "the horror" of type erasure
#[derive(derive_more::From)]
pub enum Event<P: Protocol<A, B>, A, B> {
    Incoming(Incoming<P::Incoming>),
    Outgoing(Outgoing<A, B>),
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
        A: Addr,
        B: Buf,
        F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    > OnEvent for Dispatch<E, P, A, B, F>
{
    type Event = Event<P, A, B>;

    fn on_event(&mut self, event: Self::Event, timer: &mut impl Timer) -> anyhow::Result<()> {
        match event {
            Event::Outgoing(event) => self.handle_outgoing(event, timer),
            Event::Incoming(event) => erased::OnEvent::on_event(self, event, &mut UnreachableTimer),
            Event::Closed(event) => self.handle_closed(event, timer),
        }
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
    fn handle_outgoing(
        &mut self,
        Outgoing(remote, buf): Outgoing<A, B>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some(connection) = self.connections.get_mut(&remote) {
            match connection.sender.send(buf.clone()) {
                Ok(()) => return Ok(()),
                Err(err) => {
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
        self.seq += 1;
        let close_guard = CloseGuard(self.close_sender.clone(), Some(remote.clone()), self.seq);
        let mut sender = self
            .protocol
            .connect(remote.clone(), self.on_buf.clone(), close_guard);
        if sender.send(buf).is_err() {
            warn!(">=> {remote:?} new connection immediately fail")
            // we don't try again in such case since the remote is probably never reachable anymore
            // not sure whether this should be considered as a fatal error. if this is happening,
            // will it happen for every following outgoing connection?
        } else {
            self.connections.insert(
                remote,
                Connection {
                    sender,
                    seq: self.seq,
                },
            );
        }
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
    > erased::OnEvent<Incoming<P::Incoming>> for Dispatch<E, P, A, B, F>
{
    fn on_event(
        &mut self,
        Incoming(event): Incoming<P::Incoming>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        self.seq += 1;
        let close_guard = CloseGuard(self.close_sender.clone(), None, self.seq);
        let Some((remote, sender)) = P::accept(event, self.on_buf.clone(), close_guard) else {
            return Ok(());
        };
        // always prefer to keep the connection created locally
        // the connection in `self.connections` may not be created locally, but the incoming
        // connection is definitely created remotely
        if let Entry::Vacant(entry) = self.connections.entry(remote.clone()) {
            entry.insert(Connection {
                sender,
                seq: self.seq,
            });
        } else {
            warn!("<<< {remote:?} incoming connection from connected address")
        }
        Ok(())
    }
}

impl<E, P: Protocol<A, B>, A: Addr, B, F> Dispatch<E, P, A, B, F> {
    fn handle_closed(
        &mut self,
        Closed(addr, seq): Closed<A>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some(connection) = self.connections.get(&addr) {
            if connection.seq == seq {
                debug!(">>> {addr:?} outgoing connection closed");
                self.connections.remove(&addr);
            }
        }
        Ok(())
    }
}
