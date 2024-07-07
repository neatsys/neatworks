use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use tokio::{net::UdpSocket, spawn};

use crate::{event::SendEvent, net::events::Cast};

impl SendEvent<Cast<SocketAddr, Bytes>> for Arc<UdpSocket> {
    fn send(&mut self, Cast(remote, message): Cast<SocketAddr, Bytes>) -> anyhow::Result<()> {
        let socket = self.clone();
        spawn(async move {
            if socket.send_to(&message, remote).await.is_err() {
                // TODO log
            }
        });
        Ok(())
    }
}

pub async fn run(
    socket: &UdpSocket,
    mut on_buf: impl FnMut(&[u8]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut buf = vec![0; 64 << 10];
    loop {
        let (len, _) = socket.recv_from(&mut buf).await?;
        on_buf(&buf[..len])?
    }
}
