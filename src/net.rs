pub mod kademlia;

use std::{fmt::Debug, hash::Hash, net::SocketAddr, sync::Arc};

use bincode::Options as _;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::event::SendEvent;

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
        tokio::spawn(async move { socket.send_to(buf.as_ref(), dest).await.unwrap() });
        Ok(())
    }
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

impl<T: SendMessage<A, Bytes>, A, M: Into<N>, N: Serialize> SendMessage<A, M> for MessageNet<T, N> {
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        let buf = Bytes::from(bincode::options().serialize(&message.into())?);
        self.0.send(dest, buf)
    }
}
