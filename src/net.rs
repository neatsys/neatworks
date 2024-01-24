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

pub trait SendMessage<A, M> {
    fn send(&self, dest: A, message: M) -> anyhow::Result<()>;
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
    fn send(&self, dest: SocketAddr, buf: B) -> anyhow::Result<()> {
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
    fn send(&self, dest: SendAddr<T>, message: M) -> anyhow::Result<()> {
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
    fn send(&self, dest: A, message: M) -> anyhow::Result<()> {
        let buf = Bytes::from(bincode::options().serialize(&message.into())?);
        self.0.send(dest, buf)
    }
}
