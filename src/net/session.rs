use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    io::ErrorKind,
    mem::replace,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use bincode::Options;
use rustls::RootCertStore;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tracing::{info, warn, Instrument};

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

const MAX_BUF_LEN: usize = 1 << 20;

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
#[derive(Debug)]
pub struct Dispatch<P, B, F> {
    protocol: P,
    connections: HashMap<SocketAddr, Connection<B>>,
    on_buf: F,
}

#[derive(Debug)]
struct Connection<B> {
    sender: UnboundedSender<B>,
    using: bool,
}

impl<P, B, F> Dispatch<P, B, F> {
    pub fn new(protocol: P, on_buf: F) -> anyhow::Result<Self> {
        Ok(Self {
            protocol,
            connections: HashMap::new(),
            on_buf,
        })
    }
}

impl<P, B, F> OnEvent<Init> for Dispatch<P, B, F> {
    fn on_event(&mut self, Init: Init, timer: &mut impl Timer) -> anyhow::Result<()> {
        timer.set(Duration::from_secs(1))?;
        Ok(())
    }
}

pub trait Protocol {
    fn connect<B: Buf>(
        &self,
        remote: SocketAddr,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        receiver: UnboundedReceiver<B>,
    );

    type Incoming;

    fn accept<B: Buf>(
        connection: Self::Incoming,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        receiver: UnboundedReceiver<B>,
    ) -> Option<SocketAddr>;
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

impl<P: Protocol, B: Buf, F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static>
    OnEvent<Outgoing<B>> for Dispatch<P, B, F>
{
    fn on_event(
        &mut self,
        Outgoing(remote, mut buf): Outgoing<B>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some(connection) = self.connections.get_mut(&remote) {
            match connection.sender.send(buf) {
                Ok(()) => {
                    connection.using = true;
                    return Ok(());
                }
                Err(err) => {
                    warn!(">=> {remote} reconnecting: {err}");
                    self.connections.remove(&remote);
                    buf = err.0
                }
            }
        }
        let (sender, receiver) = unbounded_channel();
        self.protocol.connect(remote, self.on_buf.clone(), receiver);
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

pub struct Incoming<T>(T);

impl<P: Protocol, B: Buf, F: FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static>
    OnEvent<Incoming<P::Incoming>> for Dispatch<P, B, F>
{
    fn on_event(
        &mut self,
        Incoming(event): Incoming<P::Incoming>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        let (sender, receiver) = unbounded_channel();
        if let Some(remote) = P::accept(event, self.on_buf.clone(), receiver) {
            // let replaced = self.connections.insert(
            //     remote,
            //     Connection {
            //         sender,
            //         using: true,
            //     },
            // );
            // if replaced.is_some() {
            //     warn!("<<< {remote} replacing previous connection")
            // }
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
        }
        Ok(())
    }
}

impl<P, B, F> OnTimer for Dispatch<P, B, F> {
    fn on_timer(&mut self, _: crate::event::TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        if self.connections.is_empty() {
            return Ok(());
        }
        self.connections.retain(|_, connection| {
            replace(&mut connection.using, false) && !connection.sender.is_closed()
        });
        info!("retaining {} connections", self.connections.len());
        Ok(())
    }
}

pub struct Tcp(bytes::Bytes);

type TcpPreamble = Option<SocketAddr>;

const TCP_PREAMBLE_LEN: usize = 16;

impl Tcp {
    pub fn new(addr: impl Into<Option<SocketAddr>>) -> anyhow::Result<Self> {
        let addr = addr.into();
        let mut preamble = bincode::options().serialize(&addr)?;
        assert!(preamble.len() < TCP_PREAMBLE_LEN);
        preamble.resize(TCP_PREAMBLE_LEN, Default::default());
        Ok(Self(preamble.into()))
    }

    async fn read_task(
        mut stream: OwnedReadHalf,
        mut on_buf: impl FnMut(&[u8]) -> anyhow::Result<()>,
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
                if len > MAX_BUF_LEN {
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

    async fn write_task<B: Buf>(
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

impl Protocol for Tcp {
    fn connect<B: Buf>(
        &self,
        remote: SocketAddr,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        receiver: UnboundedReceiver<B>,
    ) {
        let preamble = self.0.clone();
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
    }

    type Incoming = (TcpPreamble, TcpStream);

    fn accept<B: Buf>(
        (preamble, stream): Self::Incoming,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        receiver: UnboundedReceiver<B>,
    ) -> Option<SocketAddr> {
        let (read, write) = stream.into_split();
        tokio::spawn(Tcp::read_task(read, on_buf, preamble));
        if let Some(remote) = preamble {
            tokio::spawn(Tcp::write_task(write, receiver, remote));
        } else {
            // write.forget()
        }
        preamble
    }
}

pub async fn tcp_accept_session(
    listener: TcpListener,
    mut sender: impl SendEvent<Incoming<(TcpPreamble, TcpStream)>>,
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
        sender.send(Incoming((preamble, stream)))?
    }
}

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
pub mod simplex {
    use std::net::SocketAddr;

    use bincode::Options;
    use tokio::io::AsyncWriteExt;
    use tracing::warn;

    use crate::net::{Buf, IterAddr, SendMessage};

    use super::{TcpPreamble, TCP_PREAMBLE_LEN};

    pub struct Tcp;

    impl<B: Buf> SendMessage<SocketAddr, B> for Tcp {
        fn send(&mut self, dest: SocketAddr, message: B) -> anyhow::Result<()> {
            tokio::spawn(async move {
                if let Err(err) = async {
                    // have to enable REUSEADDR otherwise port number exhausted after sending to
                    // same `dest` 28K messages within 1min
                    // let mut stream = TcpStream::connect(dest).await?;
                    let socket = tokio::net::TcpSocket::new_v4()?;
                    socket.set_reuseaddr(true)?;
                    let mut stream = socket.connect(dest).await?;
                    let mut preamble = bincode::options().serialize(&TcpPreamble::None)?;
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
}

#[derive(Debug, Clone)]
pub struct Quic(pub quinn::Endpoint);

fn quic_config() -> anyhow::Result<(quinn::ServerConfig, quinn::ClientConfig)> {
    let issuer_key = rcgen::KeyPair::from_pem(include_str!("../key.pem"))?;
    let issuer = rcgen::Certificate::generate_self_signed(
        rcgen::CertificateParams::from_ca_cert_pem(include_str!("../cert.pem"))?,
        &issuer_key,
    )?;
    let priv_key = rcgen::KeyPair::generate()?;
    let cert = rcgen::Certificate::generate(
        rcgen::CertificateParams::new(vec!["neatworks.quic".into()])?,
        &priv_key,
        &issuer,
        &issuer_key,
    )?;
    let priv_key = rustls::PrivateKey(priv_key.serialize_der());
    let cert_chain = vec![rustls::Certificate(cert.der().to_vec())];

    let mut server_config = quinn::ServerConfig::with_single_cert(cert_chain, priv_key)?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    // transport_config.max_concurrent_uni_streams(0_u8.into());
    transport_config.max_concurrent_uni_streams(4096u32.into());
    transport_config.max_idle_timeout(Some(Duration::from_millis(2500).try_into()?));

    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(&[issuer.der()]);
    let client_config = quinn::ClientConfig::with_root_certificates(roots);
    Ok((server_config, client_config))
}

impl Quic {
    pub fn new(addr: SocketAddr) -> anyhow::Result<Self> {
        let (server_config, client_config) = quic_config()?;
        let mut endpoint = quinn::Endpoint::server(server_config, addr)?;
        endpoint.set_default_client_config(client_config);
        Ok(Self(endpoint))
    }

    async fn read_task(
        connection: quinn::Connection,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    ) {
        let remote_addr = connection.remote_address();
        loop {
            let mut stream = match connection.accept_uni().await {
                Ok(stream) => stream,
                // TODO
                Err(
                    quinn::ConnectionError::ConnectionClosed(_)
                    | quinn::ConnectionError::LocallyClosed
                    | quinn::ConnectionError::TimedOut,
                ) => break,
                Err(err) => {
                    warn!("<<< {remote_addr} {err}");
                    break;
                }
            };
            let mut on_buf = on_buf.clone();
            tokio::spawn(async move {
                if let Err(err) = async {
                    // determine incomplete stream?
                    on_buf(&stream.read_to_end(MAX_BUF_LEN).await?)?;
                    anyhow::Result::<_>::Ok(())
                }
                .await
                {
                    warn!("<<< {remote_addr} {err}")
                }
            });
        }
    }

    async fn write_task<B: Buf>(connection: quinn::Connection, mut receiver: UnboundedReceiver<B>) {
        while let Some(buf) = receiver.recv().await {
            let connection = connection.clone();
            tokio::spawn(async move {
                if let Err(err) = async {
                    connection.open_uni().await?.write_all(buf.as_ref()).await?;
                    anyhow::Result::<_>::Ok(())
                }
                .await
                {
                    warn!(">>> {} {err}", connection.remote_address())
                }
            });
        }
        // connection.close(Default::default(), Default::default())
    }
}

impl Protocol for Quic {
    fn connect<B: Buf>(
        &self,
        remote: SocketAddr,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        receiver: UnboundedReceiver<B>,
    ) {
        let endpoint = self.0.clone();
        // tracing::debug!("{:?} connect {remote}", endpoint.local_addr());
        tokio::spawn(async move {
            let task = async {
                let span = tracing::debug_span!("connecting", local = ?endpoint.local_addr(), remote = ?remote).entered();
                let connecting = endpoint.connect(remote, "neatworks.quic")?;
                drop(span);
                anyhow::Result::<_>::Ok(
                    connecting
                        .instrument(tracing::debug_span!("connect", local = ?endpoint.local_addr(), remote = ?remote))
                        .await?,
                )
            };
            let connection = match task.await {
                Ok(connection) => connection,
                Err(err) => {
                    warn!(">>> {remote} {err}");
                    return;
                }
            };
            tokio::spawn(Self::read_task(connection.clone(), on_buf));
            tokio::spawn(Self::write_task(connection, receiver));
        });
    }

    type Incoming = quinn::Connection;

    fn accept<B: Buf>(
        connection: Self::Incoming,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        receiver: UnboundedReceiver<B>,
    ) -> Option<SocketAddr> {
        let remote = connection.remote_address();
        tokio::spawn(Self::read_task(connection.clone(), on_buf));
        tokio::spawn(Self::write_task(connection, receiver));
        // tracing::debug!("{remote}");
        if remote.ip().is_unspecified() {
            None
        } else {
            Some(remote)
        }
    }
}

pub async fn quic_accept_session(
    Quic(endpoint): Quic,
    sender: impl SendEvent<Incoming<quinn::Connection>> + Clone + Send + 'static,
) -> anyhow::Result<()> {
    while let Some(conn) = endpoint.accept().await {
        let remote_addr = conn.remote_address();
        tracing::debug!(
            "remote {remote_addr} >>> {:?} accept",
            endpoint.local_addr()
        );
        let mut sender = sender.clone();
        tokio::spawn(async move {
            if let Err(err) = async {
                sender.send(Incoming(conn.await?))?;
                anyhow::Result::<_>::Ok(())
            }
            .await
            {
                warn!("<<< {remote_addr} {err}")
            }
        });
    }
    Ok(())
}
