use std::{hash::Hash, net::SocketAddr, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};

pub trait Addr: Send + Sync + Clone + Eq + Hash + Serialize + DeserializeOwned {}
impl<T: Send + Sync + Clone + Eq + Hash + Serialize + DeserializeOwned> Addr for T {}

pub trait SendMessage<M> {
    type Addr: Addr;

    fn send(&self, dest: Self::Addr, message: &M) -> anyhow::Result<()>;
}

pub trait SendBuf {
    type Addr: Addr;

    fn send(&self, dest: Self::Addr, buf: Vec<u8>) -> anyhow::Result<()>;
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

impl SendBuf for Udp {
    type Addr = SocketAddr;

    fn send(&self, dest: Self::Addr, buf: Vec<u8>) -> anyhow::Result<()> {
        let socket = self.0.clone();
        tokio::spawn(async move { socket.send_to(&buf, dest).await.unwrap() });
        Ok(())
    }
}
