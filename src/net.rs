use std::{net::SocketAddr, sync::Arc};

pub trait SendBuf<A> {
    fn send(&self, dest: A, buf: Vec<u8>) -> anyhow::Result<()>;
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

impl SendBuf<SocketAddr> for Udp {
    fn send(&self, dest: SocketAddr, buf: Vec<u8>) -> anyhow::Result<()> {
        let socket = self.0.clone();
        tokio::spawn(async move { socket.send_to(&buf, dest).await.unwrap() });
        Ok(())
    }
}
