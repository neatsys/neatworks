use std::{
    fmt::Debug,
    net::{IpAddr, SocketAddr},
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedReceiver,
    task::JoinSet,
};

use crate::{
    event::SendEvent,
    net::{events::Recv, SendMessage},
};

#[derive(Clone)]
pub struct Transfer<A, M>(pub A, pub M, pub Vec<u8>);

impl<A: Debug, M: Debug> Debug for Transfer<A, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transfer")
            .field("dest", &self.0)
            .field("message", &self.1)
            .field("data", &format!("<{} bytes>", self.2.len()))
            .finish()
    }
}

// is it desirable to impl SendMessage<A, (M, Vec<u8>)> for
// `impl SendEvent<Transfer<A, M>>`?
// probably involve newtype so iyada

#[derive(Debug, derive_more::From)]
pub enum Event<A, M> {
    Transfer(Transfer<A, M>),
    RecvServe(Recv<Serve<M>>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Serve<M>(M, SocketAddr);

#[derive(Clone)]
pub struct RecvBlob<M>(pub M, pub Vec<u8>);

impl<M: Debug> Debug for RecvBlob<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvBlob")
            .field("message", &self.0)
            .field("data", &format!("<{} bytes>", self.1.len()))
            .finish()
    }
}

pub async fn session<A, M: Send + 'static>(
    ip: IpAddr,
    mut events: UnboundedReceiver<Event<A, M>>,
    mut net: impl SendMessage<A, Serve<M>>,
    mut upcall: impl SendEvent<RecvBlob<M>>,
) -> anyhow::Result<()> {
    let mut bind_tasks = JoinSet::<anyhow::Result<_>>::new();
    let mut send_tasks = JoinSet::<anyhow::Result<_>>::new();
    let mut recv_tasks = JoinSet::<anyhow::Result<_>>::new();
    let mut pending_bind = Vec::new();
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
            Select::Recv(Event::Transfer(Transfer(dest, message, buf))) => {
                pending_bind.push((dest, message, buf));
                bind_tasks.spawn(async move { Ok(TcpListener::bind((ip, 0)).await?) });
            }
            Select::JoinNextBind(listener) => {
                let (dest, message, buf) = pending_bind.pop().unwrap();
                // it's possible that the message arrives before listener start accepting
                // send inside spawned task requires clone and send `net`
                // i don't want that, and spurious error like this should be fine
                net.send(dest, Serve(message, listener.local_addr()?))?;
                send_tasks.spawn(async move {
                    let (mut stream, _) = listener.accept().await?;
                    stream.write_all(&buf).await?;
                    Ok(())
                });
            }
            Select::JoinNextSend(()) => {}
            Select::Recv(Event::RecvServe(Recv(Serve(message, blob_addr)))) => {
                recv_tasks.spawn(async move {
                    let mut stream = TcpStream::connect(blob_addr).await?;
                    let mut buf = Vec::new();
                    stream.read_to_end(&mut buf).await?;
                    Ok((message, buf))
                });
            }
            Select::JoinNextRecv((message, buf)) => upcall.send(RecvBlob(message, buf))?,
        }
    }
}

pub trait SendRecvEvent<M>: SendEvent<Recv<Serve<M>>> {}
impl<T: SendEvent<Recv<Serve<M>>>, M> SendRecvEvent<M> for T {}

pub mod stream {
    use std::{
        fmt::Debug,
        future::Future,
        net::{IpAddr, SocketAddr},
        pin::Pin,
    };

    use anyhow::Context;
    use serde::{Deserialize, Serialize};
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::mpsc::UnboundedReceiver,
        task::JoinSet,
    };

    use crate::{
        event::SendEvent,
        net::{events::Recv, SendMessage},
    };

    pub struct Transfer<A, M>(pub A, pub M, pub OnTransfer);

    pub type OnTransfer = Box<
        dyn FnOnce(TcpStream) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + Sync>>
            + Send
            + Sync,
    >;

    impl<A: Debug, M: Debug> Debug for Transfer<A, M> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Transfer")
                .field("dest", &self.0)
                .field("message", &self.1)
                .finish_non_exhaustive()
        }
    }

    #[derive(Debug, derive_more::From)]
    pub enum Event<A, M> {
        Transfer(Transfer<A, M>),
        RecvServe(Recv<Serve<M>>),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Serve<M>(M, SocketAddr);

    pub struct RecvBlob<M>(pub M, pub TcpStream);

    impl<M: Debug> Debug for RecvBlob<M> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RecvBlob")
                .field("message", &self.0)
                .finish_non_exhaustive()
        }
    }

    pub async fn session<A, M: Send + 'static>(
        ip: IpAddr,
        mut events: UnboundedReceiver<Event<A, M>>,
        mut net: impl SendMessage<A, Serve<M>>,
        mut upcall: impl SendEvent<RecvBlob<M>>,
    ) -> anyhow::Result<()> {
        let mut bind_tasks = JoinSet::<anyhow::Result<_>>::new();
        let mut send_tasks = JoinSet::<anyhow::Result<_>>::new();
        let mut connect_tasks = JoinSet::<anyhow::Result<_>>::new();
        let mut pending_bind = Vec::new();
        loop {
            enum Select<A, M> {
                Recv(Event<A, M>),
                JoinNextBind(TcpListener),
                JoinNextSend(()),
                JoinNextConnect((M, TcpStream)),
            }
            match tokio::select! {
                event = events.recv() => Select::Recv(event.ok_or(anyhow::anyhow!("channel closed"))?),
                Some(result) = bind_tasks.join_next() => Select::JoinNextBind(result??),
                Some(result) = send_tasks.join_next() => Select::JoinNextSend(result??),
                Some(result) = connect_tasks.join_next() => Select::JoinNextConnect(result??),
            } {
                Select::Recv(Event::Transfer(Transfer(dest, message, buf))) => {
                    pending_bind.push((dest, message, buf));
                    // bind_tasks.spawn(async move { Ok(TcpListener::bind((ip, 0)).await?) });
                    bind_tasks.spawn(async move {
                        Ok(TcpListener::bind(SocketAddr::from(([0; 4], 0))).await?)
                    });
                }
                Select::JoinNextBind(listener) => {
                    let (dest, message, buf) = pending_bind.pop().unwrap();
                    // net.send(dest, Serve(message, listener.local_addr()?))?;
                    net.send(
                        dest,
                        Serve(message, (ip, listener.local_addr()?.port()).into()),
                    )?;
                    send_tasks.spawn(async move {
                        let (stream, _) = listener.accept().await?;
                        buf(stream).await
                    });
                }
                Select::JoinNextSend(()) => {}
                Select::Recv(Event::RecvServe(Recv(Serve(message, blob_addr)))) => {
                    connect_tasks.spawn(async move {
                        let stream = TcpStream::connect(blob_addr).await.context(blob_addr)?;
                        Ok((message, stream))
                    });
                }
                Select::JoinNextConnect((message, buf)) => upcall.send(RecvBlob(message, buf))?,
            }
        }
    }

    pub trait SendRecvEvent<M>: SendEvent<Recv<Serve<M>>> {}
    impl<T: SendEvent<Recv<Serve<M>>>, M> SendRecvEvent<M> for T {}
}
