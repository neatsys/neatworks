use std::{
    collections::HashMap, fmt::Debug, io::ErrorKind, mem::replace, net::SocketAddr, sync::Arc,
    time::Duration,
};

use bincode::Options;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
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

const TCP_MAX_BUF_LEN: usize = 1 << 20;

const TCP_PREAMBLE_LEN: usize = 16;

// a construction that enables connection reusing
// the client side of a connection informs its server address to the connected
// server with preamble, so if later a message need to be delivered in the
// opposite direction, it can go through the existing connection
// a few design choice has been explored, and here i note the rationale for
// current tradeoffs
// there's no aggressive throttling/eviction strategy built into this connection
// management. if the net is asked for concurrently sending to 10K addresses,
// then 10K connections will be established and kept inside connection table.
// this level is not the most, or even is the worst proper place to do resource
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
// happen with noticeable probablity, and switching to QUIC should workaround
// the issue quiet nicely
// if there's no active reclaiming, how should we decide when to relaim a
// connection? unlike us, libp2p actually expose a more complex connection based
// interface to upper layer, and a connection is closed when application does
// not refer to it anymore, so we cannot mirror its behavior into our stateless
// inteface. does application itself know when it should keep a connection open
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
// this solution comes with inherint overhead: each outgoing message must go
// through two channels, the first one for getting into `TcpControl` and the
// second one for dispatching into corresponding `write_task`. the first queuing
// is necessary for keeping all mutation to connection cache inside
// `TcpControl`. the performance is not comparable to udp net
// `simplex::Tcp` provides a stateless `impl SendMessage` which initiate an
// ephemeral tcp connection for every message. this results in a setup closer to
// udp, but the performance will be even worse, and it cannot accept incoming
// connections anymore. you can use a second `TcpControl` that wrapped as
//   Inline(&mut control, &mut UnreachableTimer)
// to `impl SendEvent<Incoming>` and pass into `tcp_accept_session` for incoming
// connections. check unreplicated benchmark for demonstration. the simplex
// variant is compatible with the default duplex one i.e. it is ok to have
// messages sent by simplex tcp net to be received with a duplex one, and vice
// versa
//
// TODO consider generalize this connection over underlying transportation
// protocols to be reused e.g. for QUIC
#[derive(Debug)]
pub struct TcpControl<B, F> {
    connections: HashMap<SocketAddr, Connection<B>>,
    on_buf: F,
    preamble: bytes::Bytes,
}

#[derive(Debug)]
struct Connection<B> {
    sender: UnboundedSender<B>,
    using: bool,
}

type Preamble = Option<SocketAddr>;

impl<B, F> TcpControl<B, F> {
    pub fn new(on_buf: F, addr: impl Into<Option<SocketAddr>>) -> anyhow::Result<Self> {
        let addr = addr.into();
        let mut preamble = bincode::options().serialize(&addr)?;
        assert!(preamble.len() < TCP_PREAMBLE_LEN);
        preamble.resize(TCP_PREAMBLE_LEN, Default::default());
        Ok(Self {
            connections: HashMap::new(),
            on_buf,
            preamble: preamble.into(),
        })
    }
}

impl<B, F> OnEvent<Init> for TcpControl<B, F> {
    fn on_event(&mut self, Init: Init, timer: &mut impl Timer) -> anyhow::Result<()> {
        timer.set(Duration::from_secs(1))?;
        Ok(())
    }
}

impl<B, F: FnMut(&[u8]) -> anyhow::Result<()>> TcpControl<B, F> {
    async fn read_task(
        mut stream: OwnedReadHalf,
        mut on_buf: F,
        remote: impl Into<Option<SocketAddr>>,
    ) {
        let remote = remote.into();
        if let Err(err) = async {
            loop {
                let len = match stream.read_u64().await {
                    Ok(len) => len as _,
                    Err(err) if matches!(err.kind(), ErrorKind::UnexpectedEof) => break Ok(()),
                    Err(err) => Err(err)?,
                };
                if len > TCP_MAX_BUF_LEN {
                    anyhow::bail!("invalid buffer length {len}")
                }
                let mut buf = vec![0; len];
                stream.read_exact(&mut buf).await?;
                on_buf(&buf)?
            }
        }
        .await
        {
            warn!(
                "{:?} (remote {remote:?}) >>> {:?} {err}",
                stream.peer_addr(),
                stream.local_addr()
            );
        }
    }
}

impl<B: Buf, F> TcpControl<B, F> {
    async fn write_task(
        mut stream: OwnedWriteHalf,
        mut receiver: UnboundedReceiver<B>,
        remote: SocketAddr,
    ) {
        while let Some(buf) = receiver.recv().await {
            if let Err(err) = async {
                stream.write_u64(buf.as_ref().len() as _).await?;
                stream.write_all(buf.as_ref()).await?;
                stream.flush().await
            }
            .await
            {
                warn!(
                    "{:?} >=> {:?} (remote {remote}) {err}",
                    stream.local_addr(),
                    stream.peer_addr()
                );
                break;
            }
        }
    }
}

pub struct Outgoing<B>(SocketAddr, B);

pub struct Incoming(Preamble, TcpStream);

impl<B: Buf, F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static> OnEvent<Outgoing<B>>
    for TcpControl<B, F>
{
    fn on_event(
        &mut self,
        Outgoing(remote, mut buf): Outgoing<B>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some(connection) = self.connections.get_mut(&remote) {
            match connection.sender.send(buf) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    self.connections.remove(&remote);
                    buf = err.0
                }
            }
        }
        let (sender, receiver) = unbounded_channel::<B>();
        let preamble = self.preamble.clone();
        let on_buf = self.on_buf.clone();
        tokio::spawn(async move {
            let task = async {
                let mut stream = TcpStream::connect(remote).await?;
                stream.set_nodelay(true)?;
                stream.write_all(&preamble).await?;
                anyhow::Result::<_>::Ok(stream)
            };
            let stream = match task.await {
                Ok(stream) => stream,
                Err(err) => {
                    warn!(">=> {remote} {err}");
                    return;
                }
            };
            let (read, write) = stream.into_split();
            tokio::spawn(Self::read_task(read, on_buf, remote));
            tokio::spawn(Self::write_task(write, receiver, remote));
        });
        if sender.send(buf).is_err() {
            warn!(">=> {remote} new connection immediately fail")
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

impl<B: Buf, F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static> OnEvent<Incoming>
    for TcpControl<B, F>
{
    fn on_event(
        &mut self,
        Incoming(preamble, stream): Incoming,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        let (read, write) = stream.into_split();
        tokio::spawn(Self::read_task(read, self.on_buf.clone(), preamble));
        if let Some(remote) = preamble {
            let (sender, receiver) = unbounded_channel::<B>();
            tokio::spawn(Self::write_task(write, receiver, remote));
            let replaced = self.connections.insert(
                remote,
                Connection {
                    sender,
                    using: false,
                },
            );
            if replaced.is_some() {
                warn!("<<< {remote} replacing previous connection")
            }
        } else {
            // write.forget()
        }
        Ok(())
    }
}

impl<B, F> OnTimer for TcpControl<B, F> {
    fn on_timer(&mut self, _: crate::event::TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        self.connections.retain(|_, connection| {
            replace(&mut connection.using, false) && !connection.sender.is_closed()
        });
        info!("retaining {} connections", self.connections.len());
        Ok(())
    }
}

#[derive(Clone)]
pub struct Tcp<E>(pub E);

impl<E: SendEvent<Outgoing<B>>, B> SendMessage<SocketAddr, B> for Tcp<E> {
    fn send(&mut self, dest: SocketAddr, message: B) -> anyhow::Result<()> {
        self.0.send(Outgoing(dest, message))
    }
}

impl<E: SendEvent<Outgoing<B>>, B: Buf> SendMessage<IterAddr<'_, SocketAddr>, B> for Tcp<E> {
    fn send(&mut self, dest: IterAddr<'_, SocketAddr>, message: B) -> anyhow::Result<()> {
        for addr in dest.0 {
            SendMessage::send(self, addr, message.clone())?
        }
        Ok(())
    }
}

pub async fn tcp_accept_session(
    listener: TcpListener,
    mut sender: impl SendEvent<Incoming>,
) -> anyhow::Result<()> {
    loop {
        let (mut stream, peer_addr) = listener.accept().await?;
        let task = async {
            stream.set_nodelay(true)?;
            let mut preamble = vec![0; TCP_PREAMBLE_LEN];
            stream.read_exact(&mut preamble).await?;
            anyhow::Result::<_>::Ok(
                bincode::options()
                    .allow_trailing_bytes()
                    .deserialize(&preamble)?,
            )
        };
        let preamble = match task.await {
            Ok(preamble) => preamble,
            Err(err) => {
                warn!("{peer_addr} {err}");
                continue;
            }
        };
        // println!("{peer_addr} -> {remote}");
        sender.send(Incoming(preamble, stream))?
    }
}

pub mod simplex {
    use std::net::SocketAddr;

    use bincode::Options;
    use tokio::{io::AsyncWriteExt, net::TcpStream};
    use tracing::warn;

    use crate::net::{Buf, IterAddr, SendMessage};

    use super::{Preamble, TCP_PREAMBLE_LEN};

    #[allow(clippy::type_complexity)]
    pub struct Tcp;

    impl<B: Buf> SendMessage<SocketAddr, B> for Tcp {
        fn send(&mut self, dest: SocketAddr, message: B) -> anyhow::Result<()> {
            tokio::spawn(async move {
                if let Err(err) = async {
                    let mut stream = TcpStream::connect(dest).await?;
                    let mut preamble = bincode::options().serialize(&Preamble::None)?;
                    preamble.resize(TCP_PREAMBLE_LEN, Default::default());
                    stream.write_all(&preamble).await?;
                    stream.write_u64(message.as_ref().len() as _).await?;
                    stream.write_all(message.as_ref()).await?;
                    stream.flush().await?;
                    anyhow::Result::<_>::Ok(())
                }
                .await
                {
                    warn!("simplex >>> {dest} {err}")
                }
            });
            Ok(())
        }
    }

    impl<B: Buf> SendMessage<IterAddr<'_, SocketAddr>, B> for Tcp {
        fn send(&mut self, dest: IterAddr<'_, SocketAddr>, message: B) -> anyhow::Result<()> {
            for addr in dest.0 {
                self.send(addr, message.clone())?
            }
            Ok(())
        }
    }

    // currently very similar to the approach commented above
    // just save for future case

    // use std::io::ErrorKind;

    // use bincode::Options as _;
    // use tokio::{io::AsyncReadExt as _, net::TcpListener};
    // use tracing::warn;

    // use super::{Preamble, TCP_MAX_BUF_LEN, TCP_PREAMBLE_LEN};

    // pub async fn tcp_accept_session(
    //     listener: TcpListener,
    //     on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    // ) -> anyhow::Result<()> {
    //     let local_addr = listener.local_addr()?;
    //     loop {
    //         let (mut stream, peer_addr) = listener.accept().await?;
    //         let mut on_buf = on_buf.clone();
    //         tokio::spawn(async move {
    //             let mut preamble = None;
    //             if let Err(err) = async {
    //                 let mut buf = [0; TCP_PREAMBLE_LEN];
    //                 stream.read_exact(&mut buf).await?;
    //                 preamble = bincode::options()
    //                     .allow_trailing_bytes()
    //                     .deserialize::<Preamble>(&buf)?;
    //                 loop {
    //                     let len = match stream.read_u64().await {
    //                         Ok(len) => len as _,
    //                         Err(err) if matches!(err.kind(), ErrorKind::UnexpectedEof) => {
    //                             break anyhow::Result::<_>::Ok(())
    //                         }
    //                         Err(err) => Err(err)?,
    //                     };
    //                     if len > TCP_MAX_BUF_LEN {
    //                         anyhow::bail!("invalid buffer length {len}")
    //                     }
    //                     let mut buf = vec![0; len];
    //                     stream.read_exact(&mut buf).await?;
    //                     on_buf(&buf)?
    //                 }
    //             }
    //             .await
    //             {
    //                 warn!("{peer_addr} (remote {preamble:?}) >>> {local_addr} {err}")
    //             }
    //         });
    //     }
    // }
}
