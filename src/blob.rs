use std::net::{IpAddr, SocketAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedReceiver,
    task::JoinSet,
};

use crate::{event::SendEvent, net::SendMessage};

#[derive(Debug)]
pub enum Event<A, M> {
    Send(A, M, Vec<u8>),
    IngressServe(M, SocketAddr),
}

pub async fn session<A, M: Send + 'static>(
    ip: IpAddr,
    mut events: UnboundedReceiver<Event<A, M>>,
    mut net: impl SendMessage<A, (M, SocketAddr)>,
    mut upcall: impl SendEvent<(M, Vec<u8>)>,
) -> anyhow::Result<()> {
    let mut bind_tasks = JoinSet::<anyhow::Result<_>>::new();
    let mut send_tasks = JoinSet::<anyhow::Result<_>>::new();
    let mut recv_tasks = JoinSet::<anyhow::Result<_>>::new();
    let mut pending_send = Vec::new();
    loop {
        enum Select<A, M> {
            Recv(Event<A, M>),
            JoinNextBind(TcpListener),
            JoinNextSend(()),
            JoinNextRecv((M, Vec<u8>)),
        }
        match tokio::select! {
            event = events.recv() => Select::Recv(event.ok_or(anyhow::anyhow!("channel closed"))?),
            Some(result) = bind_tasks.join_next() => Select::JoinNextBind(result??),
            Some(result) = send_tasks.join_next() => Select::JoinNextSend(result??),
            Some(result) = recv_tasks.join_next() => Select::JoinNextRecv(result??),
        } {
            Select::Recv(Event::Send(dest, message, buf)) => {
                pending_send.push((dest, message, buf));
                bind_tasks.spawn(async move { Ok(TcpListener::bind((ip, 0)).await?) });
            }
            Select::JoinNextBind(listener) => {
                let (dest, message, buf) = pending_send.pop().unwrap();
                // it's possible that the message arrives before listener start accepting
                // send inside spawned task requires clone and send `net`
                // i don't want that, and spurious error like this should be fine
                net.send(dest, (message, listener.local_addr()?))?;
                send_tasks.spawn(async move {
                    let (mut stream, _) = listener.accept().await?;
                    stream.write_all(&buf).await?;
                    Ok(())
                });
            }
            Select::JoinNextSend(()) => {}
            Select::Recv(Event::IngressServe(message, blob_addr)) => {
                recv_tasks.spawn(async move {
                    let mut stream = TcpStream::connect(blob_addr).await?;
                    let mut buf = Vec::new();
                    stream.read_to_end(&mut buf).await?;
                    Ok((message, buf))
                });
            }
            Select::JoinNextRecv((message, buf)) => upcall.send((message, buf))?,
        }
    }
}
