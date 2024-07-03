use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use tokio::{net::UdpSocket, spawn};

use crate::{
    event::SendEvent,
    net::events::{Recv, Send},
};

impl SendEvent<Send<SocketAddr, Bytes>> for Arc<UdpSocket> {
    fn send(&mut self, Send(remote, message): Send<SocketAddr, Bytes>) -> anyhow::Result<()> {
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
    mut sender: impl SendEvent<Recv<Bytes>>,
) -> anyhow::Result<()> {
    let mut buf = vec![0; 64 << 10];
    loop {
        let (len, _) = socket.recv_from(&mut buf).await?;
        sender.send(Recv(Bytes::copy_from_slice(&buf[..len])))?
    }
}
