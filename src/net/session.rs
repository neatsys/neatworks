use std::{
    collections::HashMap, fmt::Debug, io::ErrorKind, mem::replace, net::SocketAddr, sync::Arc,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinSet,
};
use tracing::warn;

use crate::event::{
    erased::{OnEventRichTimer, RichTimer},
    SendEvent, TimerId,
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

// `impl SendMessage` stub that forward messages to `TcpControl`
#[derive(Debug, Clone)]
pub struct Tcp<E>(pub E);

impl<E: SendEvent<(SocketAddr, B)>, B> SendMessage<SocketAddr, B> for Tcp<E> {
    fn send(&mut self, dest: SocketAddr, message: B) -> anyhow::Result<()> {
        self.0.send((dest, message))
    }
}

impl<E: SendEvent<(SocketAddr, B)>, B: Buf> SendMessage<IterAddr<'_, SocketAddr>, B> for Tcp<E> {
    fn send(
        &mut self,
        IterAddr(addrs): IterAddr<'_, SocketAddr>,
        message: B,
    ) -> anyhow::Result<()> {
        for addr in addrs {
            self.send(addr, message.clone())?
        }
        Ok(())
    }
}

// TODO consider generialize into a universal contruction for supporting oneshot
// unidirectional message with connections, best effort reusing established
// connections
// (that said the contruction should guarantee to reuse for fixed communication
// pattern e.g. replication group, gossip protocol when without reconfiguration,
// etc)
// is there a second connection-based user for the universal contruction? maybe
// TLS, QUIC, i don't know
#[derive(Debug)]
pub struct TcpControl<B> {
    // TODO replace timeout-based cleanup with LRU + throttle
    connections: HashMap<SocketAddr, Connection<B>>,
}

#[derive(Debug)]
struct Connection<B> {
    sender: UnboundedSender<B>,
    in_use: bool,
    timer: TimerId,
}

impl<B> Default for TcpControl<B> {
    fn default() -> Self {
        Self {
            connections: Default::default(),
        }
    }
}

impl<B> TcpControl<B> {
    pub fn new() -> Self {
        Self::default()
    }
}

const MAX_TCP_BUF_LEN: usize = 1 << 20;

impl<B: Buf> OnEventRichTimer<(SocketAddr, B)> for TcpControl<B> {
    fn on_event(
        &mut self,
        (dest, buf): (SocketAddr, B),
        timer: &mut impl RichTimer<Self>,
    ) -> anyhow::Result<()> {
        if buf.as_ref().len() >= MAX_TCP_BUF_LEN {
            anyhow::bail!("TCP buf too large: {}", buf.as_ref().len())
        }

        let buf = if let Some(connection) = self.connections.get_mut(&dest) {
            connection.in_use = true;
            match UnboundedSender::send(&connection.sender, buf) {
                Ok(()) => return Ok(()),
                // fail to reuse connection, fallback to slow path
                Err(err) => err.0,
            }
        } else {
            buf
        };

        let (sender, mut receiver) = unbounded_channel::<B>();
        tokio::spawn(async move {
            if let Err(err) = async {
                let mut stream = TcpStream::connect(dest).await?;
                stream.set_nodelay(true)?;
                // let socket = TcpSocket::new_v4()?;
                // socket.set_reuseaddr(true).unwrap();
                // let mut stream = socket.connect(dest).await?;
                while let Some(buf) = receiver.recv().await {
                    stream.write_u64(buf.as_ref().len() as _).await?;
                    stream.write_all(buf.as_ref()).await?;
                    stream.flush().await?;
                }
                Result::<_, anyhow::Error>::Ok(())
            }
            .await
            {
                warn!("{dest} {err}")
            }
        });
        sender.send(buf).map_err(|err| anyhow::anyhow!(err))?;
        self.connections.insert(
            dest,
            Connection {
                sender,
                in_use: false,
                timer: timer.set(Duration::from_secs(10), CheckIdleConnection(dest))?,
            },
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct CheckIdleConnection(SocketAddr);

impl<B> OnEventRichTimer<CheckIdleConnection> for TcpControl<B> {
    fn on_event(
        &mut self,
        CheckIdleConnection(dest): CheckIdleConnection,
        timer: &mut impl RichTimer<Self>,
    ) -> anyhow::Result<()> {
        let connection = self
            .connections
            .get_mut(&dest)
            .ok_or(anyhow::anyhow!("connection missing"))?;
        if !replace(&mut connection.in_use, false) {
            let connection = self.connections.remove(&dest).unwrap();
            timer.unset(connection.timer)?
        }
        Ok(())
    }
}

pub async fn tcp_listen_session(
    listener: TcpListener,
    mut on_buf: impl FnMut(&[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut stream_sessions = JoinSet::<anyhow::Result<_>>::new();
    let (sender, mut receiver) = unbounded_channel();
    loop {
        enum Select {
            Accept((TcpStream, SocketAddr)),
            Recv(Vec<u8>),
            JoinNext(()),
        }
        match tokio::select! {
            accept = listener.accept() => Select::Accept(accept?),
            recv = receiver.recv() => Select::Recv(recv.unwrap()),
            Some(result) = stream_sessions.join_next() => Select::JoinNext(result??),
        } {
            Select::Accept((mut stream, _)) => {
                stream.set_nodelay(true)?;
                let sender = sender.clone();
                stream_sessions.spawn(async move {
                    loop {
                        let len = match stream.read_u64().await {
                            Ok(len) => len,
                            Err(err) => {
                                if err.kind() != ErrorKind::UnexpectedEof {
                                    warn!("{:?} {err}", stream.peer_addr());
                                }
                                return Ok(());
                            }
                        };
                        if len as usize >= MAX_TCP_BUF_LEN {
                            warn!("{:?} invalid buffer length: {len}", stream.peer_addr());
                            return Ok(());
                        }
                        let mut buf = vec![0; len as _];
                        if let Err(err) = stream.read_exact(&mut buf).await {
                            warn!("{:?} {err}", stream.peer_addr());
                            return Ok(());
                        }
                        sender.send(buf).map_err(|err| anyhow::anyhow!(err))?
                    }
                });
            }
            Select::Recv(buf) => on_buf(&buf)?,
            Select::JoinNext(()) => {}
        }
    }
}
