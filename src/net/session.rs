use std::{
    collections::VecDeque, fmt::Debug, io::ErrorKind, net::SocketAddr, sync::Arc, time::Duration,
};

use bincode::Options;
use lru::LruCache;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::Instant,
};
use tracing::{info, warn};

use crate::event::{erased::OnEvent, OnTimer, SendEvent, Timer};

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
        tokio::spawn(async move { socket.send_to(buf.as_ref(), dest).await.unwrap() });
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

const TCP_MAX_CONNECTION_NUM: usize = 1024;

const TCP_MAX_BUF_LEN: usize = 1 << 20;

const TCP_PREAMBLE_LEN: usize = 16;

// a construction that enables connection reusing and thottling
// the client side of a connection informs its server address to the connected
// server with preamble, so if later a message need to be delivered in the
// opposite direction, it can go through the existing connection
//
// this is not a solution optimized for performance: each outgoing message must
// go through two channels, the first one for getting into `TcpControl` and the
// second one for dispatching into corresponding `write_task`. the first queuing
// is necessary for keeping all mutation to connection cache inside
// `TcpControl`. after all, the highest priority goal is to ensure bounded
// OS resource (mostly number of occupied ephemeral ports) usage, which is
// essential for p2p stress test. also this strategy is also taken by libp2p,
// makes it more fair comparison between protocol in this codebase and in libp2p
//
// for better performance, consider `simplex::Tcp` which inlines the
// `TcpControl` into `impl SendMessage` and saves the first queuing. the
// tradeoff is that it cannot accept incoming connections anymore, you can use
// a second `TcpControl` that wrapped as
//   Inline(&mut control, &mut UnreachableTimer)
// to `impl SendEvent<Incoming>` and pass into `tcp_accept_session` for incoming
// connections. check unreplicated benchmark for demonstration
//
// TODO consider generalize this connection over underlying transportation
// protocols to be reused e.g. for QUIC
#[derive(Debug)]
pub struct TcpControl<B, F> {
    // cached connections based on the last *outgoing* traffic
    // the incoming messages does not prompt a connection in this cache. if an incoming connection
    // is not being reused for egressing for a while, it may get evicted from this cache even if the
    // connection is still actively receiving messages
    // this does not affect the incoming traffic. even if the connection is evicted, only the
    // `write_task` exits (due to the dropped egress sender). `read_task` will still be alive and
    // forward incoming messages by calling `on_buf`
    // if afterward there's outgoing messages to the remote address, a new connection will be
    // created and pushed into the cache, the connection will be accepted by remote as an incoming
    // stream, which will be unconditionally `put` into the cache, replace the previous egress
    // sender and cause further outgoing traffic (on remote side, incoming traffic on local side)
    // migrate to the new connection. as the result, (eventually) there's at most one connection
    // between each pair of addresses
    // even if i find a way to easily promote connections on incoming messages, this strategy is
    // still necessary to eliminate duplicated connections between same pair of addresses
    connections: LruCache<SocketAddr, Connection<B>>,
    on_buf: F,
    preamble: bytes::Bytes,
    evict_instants: VecDeque<Instant>,
}

#[derive(Debug)]
struct Connection<B> {
    sender: UnboundedSender<B>,
    // used_at: Instant,
}

type Preamble = Option<SocketAddr>;

impl<B, F> TcpControl<B, F> {
    pub fn new(on_buf: F, addr: impl Into<Option<SocketAddr>>) -> anyhow::Result<Self> {
        let addr = addr.into();
        let mut preamble = bincode::options().serialize(&addr)?;
        assert!(preamble.len() < TCP_PREAMBLE_LEN);
        preamble.resize(TCP_PREAMBLE_LEN, Default::default());
        Ok(Self {
            connections: LruCache::new(TCP_MAX_CONNECTION_NUM.try_into().unwrap()),
            on_buf,
            preamble: preamble.into(),
            evict_instants: Default::default(),
        })
    }
}

impl<B, F: FnMut(&[u8]) -> anyhow::Result<()>> TcpControl<B, F> {
    async fn read_task(
        mut stream: OwnedReadHalf,
        mut on_buf: F,
        remote: impl Into<Option<SocketAddr>>,
    ) {
        let remote = remote.into();
        loop {
            let len = match stream.read_u64().await {
                Ok(len) => len as _,
                Err(err) => {
                    if !matches!(err.kind(), ErrorKind::UnexpectedEof) {
                        warn!(
                            "{:?} (remote {remote:?}) >>> {:?} {err}",
                            stream.peer_addr(),
                            stream.local_addr()
                        )
                    }
                    break;
                }
            };
            if let Err(err) = async {
                if len > TCP_MAX_BUF_LEN {
                    anyhow::bail!("invalid buffer length {len}")
                }
                let mut buf = vec![0; len];
                stream.read_exact(&mut buf).await?;
                on_buf(&buf)?;
                Ok(())
            }
            .await
            {
                warn!(
                    "{:?} (remote {remote:?}) >>> {:?} {err}",
                    stream.peer_addr(),
                    stream.local_addr()
                );
                break;
            }
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
                    self.connections.pop(&remote);
                    buf = err.0
                }
            }
        }
        while self.connections.len() >= TCP_MAX_CONNECTION_NUM {
            // 60s = default TIME_WAIT
            // one eviction = at most one active shutdown = at most one TIME_WAIT
            // if we keep evicting when this condition holds, the number of TIME_WAIT connections
            // is potentially unbounded

            // for unknown reason, there seems to be at most 32768 connections in TIME_WAIT
            // so we should better keep the number bounded by 32768, not just bounded
            // notice that bulk service also generates TIME_WAIT connections

            // notice that there will be large amount of connection get into TIME_WAIT when the
            // `TcpControl` is dropped, potentially more than 32768
            // so to be safe entropy evaluation runs must have more than 1min interval
            if self.evict_instants.len() == 32768.min(TCP_MAX_CONNECTION_NUM) {
                if self.evict_instants.front().unwrap().elapsed() < Duration::from_secs(60) {
                    warn!("explicit drop egress message due to reaching maximum concurrent connection number");
                    return Ok(());
                }
                self.evict_instants.pop_front();
            }
            self.evict_instants.push_back(Instant::now());
            let (remote, _) = self.connections.pop_lru().unwrap();
            info!("evicting connection to {remote}")
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
            self.connections.push(remote, Connection { sender });
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
            let replaced = self.connections.put(remote, Connection { sender });
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
        unreachable!()
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

pub mod simplex {
    use std::net::SocketAddr;

    use tracing::warn;

    use crate::{
        event::{erased::Inline, UnreachableTimer},
        net::{Buf, IterAddr, SendMessage},
    };

    #[allow(clippy::type_complexity)]
    pub struct Tcp<B>(super::TcpControl<B, fn(&[u8]) -> anyhow::Result<()>>);

    impl<B> Tcp<B> {
        pub fn new() -> anyhow::Result<Self> {
            let on_buf = (|_: &_| {
                warn!("ignore ingress message of simplex connection");
                Ok(())
            }) as _;
            Ok(Self(super::TcpControl::new(on_buf, None)?))
        }
    }

    impl<B: Buf> SendMessage<SocketAddr, B> for Tcp<B> {
        fn send(&mut self, dest: SocketAddr, message: B) -> anyhow::Result<()> {
            SendMessage::send(
                &mut super::Tcp(Inline(&mut self.0, &mut UnreachableTimer)),
                dest,
                message,
            )
        }
    }

    impl<B: Buf> SendMessage<IterAddr<'_, SocketAddr>, B> for Tcp<B> {
        fn send(&mut self, dest: IterAddr<'_, SocketAddr>, message: B) -> anyhow::Result<()> {
            SendMessage::send(
                &mut super::Tcp(Inline(&mut self.0, &mut UnreachableTimer)),
                dest,
                message,
            )
        }
    }

    // impl<B, F> super::TcpControl<B, F> {
    //     pub fn new_accept(on_buf: F) -> Inline<'??
    // }
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
        let remote = match task.await {
            Ok(remote) => remote,
            Err(err) => {
                warn!("{peer_addr} {err}");
                continue;
            }
        };
        // println!("{peer_addr} -> {remote}");
        sender.send(Incoming(remote, stream))?
    }
}
