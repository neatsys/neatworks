pub mod kademlia;

use std::{
    collections::HashMap, fmt::Debug, hash::Hash, net::SocketAddr, sync::Arc, time::Duration,
};

use bincode::Options as _;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinSet,
};

use crate::event::{
    erased::{OnEvent, Timer},
    SendEvent,
};

pub trait Addr:
    Send + Sync + Clone + Eq + Hash + Debug + Serialize + DeserializeOwned + 'static
{
}
impl<T: Send + Sync + Clone + Eq + Hash + Debug + Serialize + DeserializeOwned + 'static> Addr
    for T
{
}

pub trait Buf: AsRef<[u8]> + Send + Sync + Clone + 'static {}
impl<T: AsRef<[u8]> + Send + Sync + Clone + 'static> Buf for T {}

// terms about nets that used in this codebase
// raw net: implementation of `SendMessage<_, impl Buf>`
// message net: implement `SendMessage<_, M>` for some structural message type
// `M`, potentially through wrapping some raw net and adding serialization upon
// it (and such implementation usually becomes a type alias of `MessageNet`
// below)
// socket net: implementation of `SendMessage<SocketAddr, _>`
// the implementation that wraps a socket net and translates other address types
// into `SocketAddr` can be called as routing net, i guess

// the `A` parameter used to be an associated type, and later get lifted to
// contravariant position to enable implementations to send to multiple address
// types
// there used to be a dediated trait for raw net, but later get merged into this
// univeral trait
// these result in the `Addr` and `Buf` traits are not directly mentioned in
// sending trait anymore. nevertheless, constrait type parameters with them
// manually when necessary

// it's obvious that `M` is not required to `impl Buf` for all cases, but `A`
// is neither required to `impl Addr` for all cases
// only the "public" `A`s, e.g. the ones that are sent as part of network
// messages, are required to `impl Addr`, because e.g. network messages demand
// it
// perhaps `Addr` is not the best name for the trait
pub trait SendMessage<A, M> {
    // an alternative choice is to accept `message: &M` to avoid overhead of moving large messages,
    // since structural messages needs to be serialized before sent, which usually can be done
    // against borrowed messages
    // the reasons against that choice is that, firstly this `SendMessage` is a general propose
    // network abstraction, covering the implementations that does not serialize messages e.g.
    // sending through in memory channels, thus demands message ownership
    // secondly, even only considering serializing implementations, the implementation may serialize
    // the message with certain extra fields e.g. tagging the type `M`. although these fields can be
    // filled into byte buffer, we would prefer to instead performing transformation on the original
    // message before serialization to do the trick, to e.g. enjoy type safety. certain
    // transformation e.g. `Into` is easy to use and requires owned messages, so the trait follows
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()>;
}

// i have realized that this will always be used as
// `IterAddr<&mut dyn Iterator<Item = A>>`, so change the definition to make it
// more usable
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub struct IterAddr<I>(pub I);

// this is usable enough, just still feels not very decent, and performance will
// be limited, certainly
// it may seem like a default implementation of SendMessage<IterAddr<A>, M>
// should be provided as long as SendMessage<A, M> and M is Clone. but in this
// codebase "address" has been abused and certain address iterator e.g.
// IterAddr<AllReplica> does not really make sense
pub struct IterAddr<'a, A>(pub &'a mut (dyn Iterator<Item = A> + Send + Sync));
// TODO better name
pub trait SendMessageToEach<A, M>: for<'a> SendMessage<IterAddr<'a, A>, M> {}
impl<T: for<'a> SendMessage<IterAddr<'a, A>, M>, A, M> SendMessageToEach<A, M> for T {}
pub trait SendMessageToEachExt<A, M>: SendMessageToEach<A, M> {
    fn send_to_each(
        &mut self,
        mut addrs: impl Iterator<Item = A> + Send + Sync,
        message: M,
    ) -> anyhow::Result<()> {
        SendMessage::send(self, IterAddr(&mut addrs), message)
    }
}
impl<T: ?Sized + SendMessageToEach<A, M>, A, M> SendMessageToEachExt<A, M> for T {}

pub mod events {
    #[derive(Debug, Clone)]
    pub struct Recv<M>(pub M);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SendAddr<T>(pub T);

#[derive(Debug)]
pub struct Auto<A>(std::marker::PhantomData<A>); // TODO better name

impl<T: SendEvent<M>, M> SendMessage<SendAddr<T>, M> for Auto<SendAddr<T>> {
    fn send(&mut self, mut dest: SendAddr<T>, message: M) -> anyhow::Result<()> {
        dest.0.send(message)
    }
}

#[derive(Debug)]
pub struct MessageNet<T, M>(pub T, std::marker::PhantomData<M>);

impl<T, M> MessageNet<T, M> {
    pub fn new(raw_net: T) -> Self {
        Self(raw_net, Default::default())
    }
}

impl<T, M> From<T> for MessageNet<T, M> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Clone, M> Clone for MessageNet<T, M> {
    fn clone(&self) -> Self {
        Self::new(self.0.clone())
    }
}

impl<T: SendMessage<A, Bytes>, A, M: Into<N>, N: Serialize> SendMessage<A, M> for MessageNet<T, N> {
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        // the `dest` may be an IterAddr, use Bytes to reduce cloning overhead
        let buf = Bytes::from(bincode::options().serialize(&message.into())?);
        self.0.send(dest, buf)
    }
}

pub fn deserialize<M: DeserializeOwned>(buf: &[u8]) -> anyhow::Result<M> {
    bincode::options()
        .allow_trailing_bytes()
        .deserialize(buf)
        .map_err(Into::into)
}

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
        // a broken error propagation here. nothing can observe the failure of `send_to`
        // by definition `SendMessage` is one-way (i.e. no complete notification) unreliable net
        // interface, so this is fine, just kindly note the fact
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

#[derive(Debug)]
pub struct TcpControl<B> {
    connections: HashMap<SocketAddr, (UnboundedSender<B>, bool)>,
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

impl<B: Buf> OnEvent<(SocketAddr, B)> for TcpControl<B> {
    fn on_event(
        &mut self,
        (dest, buf): (SocketAddr, B),
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if buf.as_ref().len() >= MAX_TCP_BUF_LEN {
            anyhow::bail!("TCP buf too large: {}", buf.as_ref().len())
        }
        let mut set_timer = false;
        let (sender, in_use) = self.connections.entry(dest).or_insert_with(|| {
            let (sender, mut receiver) = unbounded_channel::<B>();
            tokio::spawn(async move {
                // let mut stream = TcpStream::connect(dest).await.unwrap();
                let socket = TcpSocket::new_v4().unwrap();
                socket.set_reuseaddr(true).unwrap();
                let mut stream = socket.connect(dest).await.unwrap();
                while let Some(buf) = receiver.recv().await {
                    stream.write_u64(buf.as_ref().len() as _).await.unwrap();
                    stream.write_all(buf.as_ref()).await.unwrap();
                    stream.flush().await.unwrap()
                }
            });
            set_timer = true;
            (sender, false)
        });
        if set_timer {
            timer.set(Duration::from_secs(10), IdleConnection(dest))?;
        }
        *in_use = true;
        sender
            .send(buf)
            .map_err(|_| anyhow::anyhow!("connection closed"))
    }
}

struct IdleConnection(SocketAddr);

impl<B> OnEvent<IdleConnection> for TcpControl<B> {
    fn on_event(
        &mut self,
        IdleConnection(dest): IdleConnection,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let (_, in_use) = self
            .connections
            .get_mut(&dest)
            .ok_or(anyhow::anyhow!("connection missing"))?;
        if *in_use {
            *in_use = false;
            timer.set(Duration::from_secs(10), IdleConnection(dest))?;
        } else {
            self.connections.remove(&dest).unwrap();
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
                let sender = sender.clone();
                stream_sessions.spawn(async move {
                    while let Ok(len) = stream.read_u64().await {
                        if len as usize >= MAX_TCP_BUF_LEN {
                            eprintln!(
                                "Closing connection to {:?} for too large buf: {len}",
                                stream.peer_addr()
                            );
                            break;
                        }
                        let mut buf = vec![0; len as _];
                        stream.read_exact(&mut buf).await?;
                        sender
                            .send(buf)
                            .map_err(|_| anyhow::anyhow!("channel closed"))?
                    }
                    Ok(())
                });
            }
            Select::Recv(buf) => on_buf(&buf)?,
            Select::JoinNext(()) => {}
        }
    }
}
