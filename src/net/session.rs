use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    mem::replace,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use derive_where::derive_where;

use tracing::{info, warn};

use crate::event::{
    erased::{events::Init, OnEvent},
    OnTimer, SendEvent, Timer,
};

use super::{Buf, IterAddr, SendMessage};

#[derive(Debug, Clone)]
pub struct Udp(pub Arc<tokio::net::UdpSocket>);

impl Udp {
    pub async fn recv_session(
        &self,
        mut on_buf: impl FnMut(&[u8]) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut buf = vec![0; 1 << 16];
        loop {
            let (len, _) = self.0.recv_from(&mut buf).await?;
            on_buf(&buf[..len])?
        }
    }
}

impl<B: Buf> SendMessage<SocketAddr, B> for Udp {
    fn send(&mut self, dest: SocketAddr, buf: B) -> anyhow::Result<()> {
        let socket = self.0.clone();
        // a broken error propagation here. no observation to the failure of `send_to`
        // by definition `SendMessage` is one-way (i.e. no complete notification) unreliable net
        // interface, so this is fine, just kindly note the fact
        // alternatively, collect sending tasks into a `JoinSet`
        // however that cannot be owned by `impl OnEvent`, which does not have a chance to poll
        // so not an ideal alternation and not conducted for now
        tokio::spawn(async move {
            if let Err(err) = socket.send_to(buf.as_ref(), dest).await {
                warn!("{:?} >>> {dest} {err}", socket.local_addr())
            }
        });
        Ok(())
    }
}

impl<B: Buf> SendMessage<IterAddr<'_, SocketAddr>, B> for Udp {
    fn send(&mut self, dest: IterAddr<'_, SocketAddr>, buf: B) -> anyhow::Result<()> {
        for addr in dest.0 {
            self.send(addr, buf.clone())?
        }
        Ok(())
    }
}

const MAX_BUF_LEN: usize = 1 << 20; // hard limit of single serialized message

// a construction that enables connection reusing
// the client side of a connection informs its server address to the connected
// server upon establishing connection, so if later a message need to be
// delivered in the opposite direction, it can go through the existing
// connection
// a few design choice has been explored, and here i note the rationale for
// current tradeoffs
// there's no aggressive throttling/eviction strategy built into this connection
// management. if the net is asked for concurrently sending to 10K addresses,
// then 10K connections will be established and kept inside connection table.
// this level is not the most proper (or even is the worst) place to do resource
// management. current `SendMessage<_, _>` interface is too simple for passing
// down any user intent (e.g. whether the sending address will be sent again in
// near future), while it is not "low-level" enough to assume exclusive access
// to system network resource. it's hard to do anything useful when there's e.g.
// bulk service out there taking up unknown yet significant amount of network.
// as the result, i just give up
// nevertheless, under current design there's unlikely port leaking, and i
// haven't seen warning from connection workers for a while. i guess the only
// remaining concern is that there's too many TIME_WAIT connections out there
// exceeding the amount kernel would like to keep i.e. 32768, so there may be
// unexpected fast recycling which causes corruption. i don't expect that to
// happen with noticeable probability, and switching to QUIC should workaround
// the issue quiet nicely
// if there's no active reclaiming, how should we decide when to reclaim a
// connection? unlike us, libp2p actually expose a more complex connection based
// interface to upper layer, and a connection is closed when application does
// not refer to it anymore, so we cannot mirror its behavior into our stateless
// interface. does application itself know when it should keep a connection open
// and when it should release? in kademlia it might know, but in entropy the
// message pattern may be more in a unidirectional oneshot style. not sure
// whether that's inherent difference among protocols or it just libp2p's
// implementation conforms its network interface
// anyway, we have to define our own reclaiming policy. tentatively a connection
// is reclaimed if it has no outgoing traffic for one whole second. the
// rationale is that
// * network communication is usually ping-pong style. if we haven't sent to
//   some address for one whole second, then it probably is not sending to us
//   for a while either
// * even if it is an unidirectional traffic, in current implementation
//   reclaiming the outgoing traffic worker does not affect the incoming
//   processing
// * one second is a seemingly short but actually reasonable timeout for session
//   ending. a session is a period that each side of the connection response to
//   each other's latest message almost immediately, and all exchanged messages
//   are around the same topic/context. we guarantee to never interrupt a
//   session, but we don't try to predict when the next session will come
//
// this solution comes with inherit overhead: each outgoing message must go
// through two channels, the first one from `DispatchNet` to `Dispatch` and the
// second one from `Dispatch` to the corresponded `write_task`. the first
// queuing is necessary for maintaining a global view of all connections in the
// `Dispatch`. the performance is not comparable to bare metal udp net
#[derive_where(Debug; P, P::Sender)]
pub struct Dispatch<P: Protocol<B>, B, F> {
    protocol: P,
    connections: HashMap<SocketAddr, Connection<P, B>>,
    #[derive_where(skip)]
    on_buf: F,
}

#[derive_where(Debug; P::Sender)]
struct Connection<P: Protocol<B>, B> {
    sender: P::Sender,
    using: bool,
}

impl<P: Protocol<B>, B, F> Dispatch<P, B, F> {
    pub fn new(protocol: P, on_buf: F) -> anyhow::Result<Self> {
        Ok(Self {
            protocol,
            connections: HashMap::new(),
            on_buf,
        })
    }
}

impl<P: Protocol<B>, B, F> OnEvent<Init> for Dispatch<P, B, F> {
    fn on_event(&mut self, Init: Init, timer: &mut impl Timer) -> anyhow::Result<()> {
        timer.set(Duration::from_secs(1))?;
        Ok(())
    }
}

pub trait Protocol<B> {
    type Sender: SendEvent<B>;

    fn connect(
        &self,
        remote: SocketAddr,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    ) -> Self::Sender;

    type Incoming;

    fn accept(
        connection: Self::Incoming,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    ) -> Option<(SocketAddr, Self::Sender)>;
}

pub struct Outgoing<B>(SocketAddr, B);

#[derive(Clone)]
pub struct DispatchNet<E>(pub E);

impl<E: SendEvent<Outgoing<B>>, B> SendMessage<SocketAddr, B> for DispatchNet<E> {
    fn send(&mut self, dest: SocketAddr, message: B) -> anyhow::Result<()> {
        self.0.send(Outgoing(dest, message))
    }
}

impl<E: SendEvent<Outgoing<B>>, B: Buf> SendMessage<IterAddr<'_, SocketAddr>, B>
    for DispatchNet<E>
{
    fn send(&mut self, dest: IterAddr<'_, SocketAddr>, message: B) -> anyhow::Result<()> {
        for addr in dest.0 {
            SendMessage::send(self, addr, message.clone())?
        }
        Ok(())
    }
}

impl<P: Protocol<B>, B: Buf, F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static>
    OnEvent<Outgoing<B>> for Dispatch<P, B, F>
{
    fn on_event(
        &mut self,
        Outgoing(remote, buf): Outgoing<B>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some(connection) = self.connections.get_mut(&remote) {
            match connection.sender.send(buf.clone()) {
                Ok(()) => {
                    connection.using = true;
                    return Ok(());
                }
                Err(err) => {
                    warn!(">=> {remote} connection discontinued: {err}");
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
        let mut sender = self.protocol.connect(remote, self.on_buf.clone());
        if sender.send(buf).is_err() {
            warn!(">=> {remote} new connection immediately fail")
            // we don't try again in such case since the remote is probably never reachable anymore
            // not sure whether this should be considered as a fatal error. if this is happening,
            // will it happen for every following outgoing connection?
        } else {
            self.connections.insert(
                remote,
                Connection {
                    sender,
                    using: true,
                },
            );
        }
        Ok(())
    }
}

pub struct Incoming<T>(T);

impl<P: Protocol<B>, B: Buf, F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static>
    OnEvent<Incoming<P::Incoming>> for Dispatch<P, B, F>
{
    fn on_event(
        &mut self,
        Incoming(event): Incoming<P::Incoming>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        let Some((remote, sender)) = P::accept(event, self.on_buf.clone()) else {
            return Ok(());
        };
        // always prefer to keep the connection created locally
        // the connection in `self.connections` may not be created locally, but the incoming
        // connection is definitely created remotely
        if let Entry::Vacant(entry) = self.connections.entry(remote) {
            entry.insert(Connection {
                sender,
                using: true,
            });
        } else {
            info!("<<< {remote} skip inserting incoming connection")
        }
        Ok(())
    }
}

impl<P: Protocol<B>, B, F> OnTimer for Dispatch<P, B, F> {
    fn on_timer(&mut self, _: crate::event::TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        if self.connections.is_empty() {
            return Ok(());
        }
        self.connections
            .retain(|_, connection| replace(&mut connection.using, false));
        info!("retaining {} connections", self.connections.len());
        Ok(())
    }
}

pub mod quic;
pub mod tcp;

pub use quic::Quic;
pub use tcp::Tcp;

// cSpell:words quic bincode rustls libp2p kademlia oneshot rcgen unreplicated
// cSpell:words neatworks
// cSpell:ignore nodelay reuseaddr
