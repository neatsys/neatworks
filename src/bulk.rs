// justification of dedicated bulk transfer service
//
// the network messaging abstraction in this codebse i.e. `SendMessage` is
// suitable for unreliably delivering short messages with negligible overhead
// (negligible for delivering single message, overall networking overhead may
// still be significant). this module, on the other hand, provides different
// semantic that is usally desired when sending binary large object
//
// firstly this service provides explicit cancel interface, which could be
// helpful to improve bandwidth and performance efficiency. notice that the
// cancellation results in a silent fail on the other side, without explicit
// notification, and the cancel itself is not reliable: a transfer that get
// cancelled too late may still be treated as finished on the other side. so
// it is a purely optimization shortcut
//
// secondly this service guarantees reliable transfer (in the absent of
// cancellation and sender/receiver crashing). `impl SendMessage` does not
// guarantee the message will be delivered under any conidition. this service,
// instead, ensures the transfer will always finish as long as both participants
// do not cancel the transfer, the underlying connection does not broken, and
// the `Serve` message is delivered. application may take this assertion to
// simplify logic
//
// without further consideration, this service can also be used to avoid head of
// line blocking. exclude large transfer from the ordinary connection can help
// to keep latency low and stable

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedReceiver,
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    event::SendEvent,
    net::{events::Recv, SendMessage},
};

pub struct Offer<A, M> {
    pub dest: A,
    pub message: M,
    pub buf: Bytes,
    pub cancel: Option<CancellationToken>,
}

pub struct Accept<N> {
    pub serve_internal: (SocketAddr, u32),
    pub expect_len: Option<usize>,
    pub into_recv_event: Box<dyn FnOnce(Vec<u8>) -> N + Send + Sync>,
    pub cancel: Option<CancellationToken>,
}

#[derive(derive_more::From)]
pub enum Event<A, M, N> {
    Offer(Offer<A, M>),
    Accept(Accept<N>),
    RecvServe(Serve<M>),
}

impl<A, M, N> From<Recv<Serve<M>>> for Event<A, M, N> {
    fn from(Recv(value): Recv<Serve<M>>) -> Self {
        Self::RecvServe(value)
    }
}

#[derive(derive_more::Deref)]
pub struct RecvOffer<M> {
    #[deref]
    pub inner: M,
    serve_internal: Option<(SocketAddr, u32)>,
}

pub trait Service<A, M, N>: SendEvent<Event<A, M, N>> {}
impl<T: SendEvent<Event<A, M, N>>, A, M, N> Service<A, M, N> for T {}

pub trait ServiceExt<A, M, N> {
    fn offer(
        &mut self,
        dest: A,
        message: M,
        buf: impl Into<Bytes>,
        cancel: impl Into<Option<CancellationToken>>,
    ) -> anyhow::Result<()>;

    fn accept(
        &mut self,
        recv_offer: &mut RecvOffer<M>,
        expect_len: impl Into<Option<usize>>,
        into_recv_event: impl FnOnce(Vec<u8>) -> N + Send + Sync + 'static,
        cancel: impl Into<Option<CancellationToken>>,
    ) -> anyhow::Result<()>;

    // implicitly reject by not calling `accept` also works, just with a definite penalty of
    // holding ephemeral server for timeout period
    // that said, this method is not used currently, since it probably cannot enable faster
    // releasing of ephemeral server with this implementation
    // revisit this if necessary
    fn reject(&mut self, recv_offer: &mut RecvOffer<M>) -> anyhow::Result<()> {
        let cancel = CancellationToken::new();
        self.accept(recv_offer, None, |_| unreachable!(), cancel.clone())?;
        cancel.cancel();
        Ok(())
    }
}

impl<T: Service<A, M, N> + ?Sized, A, M, N> ServiceExt<A, M, N> for T {
    fn offer(
        &mut self,
        dest: A,
        message: M,
        buf: impl Into<Bytes>,
        cancel: impl Into<Option<CancellationToken>>,
    ) -> anyhow::Result<()> {
        let offer = Offer {
            dest,
            message,
            buf: buf.into(),
            cancel: cancel.into(),
        };
        SendEvent::send(self, Event::Offer(offer))
    }

    fn accept(
        &mut self,
        recv_offer: &mut RecvOffer<M>,
        expect_len: impl Into<Option<usize>>,
        into_recv_event: impl FnOnce(Vec<u8>) -> N + Send + Sync + 'static,
        cancel: impl Into<Option<CancellationToken>>,
    ) -> anyhow::Result<()> {
        let accept = Accept {
            serve_internal: recv_offer
                .serve_internal
                .take()
                // TODO compile time check instead
                .ok_or(anyhow::anyhow!(
                    "the offer has already been accepted/rejected"
                ))?,
            expect_len: expect_len.into(),
            into_recv_event: Box::new(into_recv_event),
            cancel: cancel.into(),
        };
        SendEvent::send(self, Event::Accept(accept))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Serve<M>(M, SocketAddr, u32);

pub async fn session<A, M, N: Send + 'static>(
    ip: impl Into<Option<IpAddr>>,
    mut events: UnboundedReceiver<Event<A, M, N>>,
    mut net: impl SendMessage<A, Serve<M>>,
    mut upcall: impl SendEvent<RecvOffer<M>> + SendEvent<N>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind((ip.into().unwrap_or(IpAddr::from([0, 0, 0, 0])), 0)).await?;
    let addr = listener.local_addr()?;
    let mut id = 0;
    let mut pending_accept = HashMap::new();

    let mut send_tasks = JoinSet::new();
    let mut recv_tasks = JoinSet::new();
    let mut cancel_tasks = JoinSet::new();
    loop {
        enum Select<A, M, N> {
            Recv(Event<A, M, N>),
            Accept((TcpStream, SocketAddr)),
            JoinNextSend(()),
            JoinNextRecv(Option<N>),
            JoinNextCancel(u32),
        }
        match tokio::select! {
            event = events.recv() => Select::Recv(event.ok_or(anyhow::anyhow!("channel closed"))?),
            stream = listener.accept() => Select::Accept(stream?),
            Some(result) = send_tasks.join_next() => Select::JoinNextSend(result?),
            Some(result) = recv_tasks.join_next() => Select::JoinNextRecv(result?),
            Some(result) = cancel_tasks.join_next() => Select::JoinNextCancel(result?),
        } {
            Select::Recv(Event::Offer(offer)) => {
                id += 1;
                pending_accept.insert(id, (offer.buf, offer.cancel.clone()));
                cancel_tasks.spawn(async move {
                    if let Some(cancel) = offer.cancel {
                        cancel.cancelled().await
                    } else {
                        // default timeout to avoid `pending_accept` entry leaks
                        sleep(Duration::from_millis(2500)).await
                    }
                    id
                });
                net.send(offer.dest, Serve(offer.message, addr, id))?
            }
            Select::JoinNextCancel(id) => {
                pending_accept.remove(&id);
            }
            Select::Recv(Event::RecvServe(Serve(message, serve_addr, id))) => {
                upcall.send(RecvOffer {
                    inner: message,
                    serve_internal: Some((serve_addr, id)),
                })?
            }
            Select::Recv(Event::Accept(accept)) => {
                let (addr, id) = accept.serve_internal;
                recv_tasks.spawn(async move {
                    let task = async {
                        let mut stream = TcpStream::connect(addr).await?;
                        stream.write_u32(id).await?;
                        let mut buf = Vec::new();
                        if let Some(expect_len) = accept.expect_len {
                            buf.resize(expect_len, Default::default());
                            stream.read_exact(&mut buf).await?;
                        } else {
                            stream.read_to_end(&mut buf).await?;
                        }
                        anyhow::Result::<_>::Ok(buf)
                    };
                    let buf = if let Some(cancel) = accept.cancel {
                        tokio::select! {
                            result = task => result,
                            () = cancel.cancelled() => return None
                        }
                    } else {
                        task.await
                    };
                    match buf {
                        Ok(buf) => Some((accept.into_recv_event)(buf)),
                        Err(err) => {
                            info!("{err}");
                            None
                        }
                    }
                });
            }
            Select::JoinNextRecv(recv_event) => {
                if let Some(recv_event) = recv_event {
                    upcall.send(recv_event)?
                }
            }
            Select::Accept((mut stream, _)) => {
                // it is a little risky to blocking the main event loop, but base on the sender
                // logic this should either finish fast or fail fast, and we are not chasing
                // concurrency that hard
                let Ok(id) = stream.read_u32().await else {
                    continue;
                };
                let Some((buf, cancel)) = pending_accept.remove(&id) else {
                    continue; // drop stream to close connection
                };
                // technically this does not need to be joined
                // just as a good habbit and in case needed later
                send_tasks.spawn(async move {
                    let task = stream.write_all(&buf);
                    let result = if let Some(cancel) = cancel {
                        tokio::select! {
                            result = task => result,
                            () = cancel.cancelled() => Ok(())
                        }
                    } else {
                        task.await
                    };
                    if let Err(err) = result {
                        info!("{err}")
                    }
                });
            }
            Select::JoinNextSend(()) => {}
        }
    }
}
