// justification of dedicated blob transfer service
//
// the network messaging abstraction in this codebse i.e. `SendMessage` is
// suitable for delivering short messages with negligible overhead (negligible
// for delivering single message, overall networking overhead may still be
// significant). when sending messages/data that is large enough to break this
// expectation, use this module can provide more stable performance and
// additional functions
//
// for applications deployed with udp-based transportation, a reliable delivery
// service is demanded for any message that cannot be fit into several IP
// segments, or it would be unpractical to deliver the message eventually
// through resending. blob transfer service provides such reliability with
// ephemeral TCP servers and connections, and it's safe to consider blob
// transfer failures as fatal errors without hurting robustness
//
// although tcp-based deployment does not have the reliability concren above,
// if the transportation delivers all messages destinating same remote address
// sequentially (e.g. `net::tokio::Tcp`), blob messages may occupy the
// transmission channel and postpone later messages longer than expect. also,
// blob messages probably should be logged differently for diagnostic (e.g. we
// probably don't want to log blob's full content). as the result dedicated
// serivce (at least channels) for blob transfer is still desirable
//
// this blob service additionally supports cancellation on both sender and
// receiver side, which can be helpful to improve bandwidth and performance
// efficiency. in conclusion, for the following statements:
// * protocol doesn't want to keep in mind that the sending may fail
// * protocol can benefit from isolation between sending blob and ordinary
//   messages
// * protocol can make use of a cancellation interface
// if any of these is true, a blob transfer service instance can be deployed

use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::mpsc::UnboundedReceiver,
    task::{yield_now, JoinSet},
    time::timeout,
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
    pub serve_addr: SocketAddr,
    pub expect_len: Option<usize>,
    pub into_recv_event: Box<dyn FnOnce(Vec<u8>) -> N + Send + Sync>,
    pub cancel: Option<CancellationToken>,
}

#[derive(derive_more::From)]
pub enum Event<A, M, N> {
    Offer(Offer<A, M>),
    Accept(Accept<N>),
    RecvServe(Recv<Serve<M>>),
}

#[derive(derive_more::Deref)]
pub struct RecvOffer<M> {
    #[deref]
    pub inner: M,
    serve_addr: Option<SocketAddr>,
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
            serve_addr: recv_offer
                .serve_addr
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
pub struct Serve<M>(M, SocketAddr);

pub async fn session<A, M, N: Send + 'static>(
    ip: IpAddr,
    mut events: UnboundedReceiver<Event<A, M, N>>,
    mut net: impl SendMessage<A, Serve<M>>,
    mut upcall: impl SendEvent<RecvOffer<M>> + SendEvent<N>,
) -> anyhow::Result<()> {
    let mut bind_tasks = JoinSet::<anyhow::Result<_>>::new();
    let mut send_tasks = JoinSet::new();
    let mut recv_tasks = JoinSet::new();
    let mut pending_bind = Vec::new();
    loop {
        enum Select<A, M, N> {
            Recv(Event<A, M, N>),
            JoinNextBind(TcpListener),
            JoinNextSend(()),
            JoinNextRecv(Option<N>),
        }
        match tokio::select! {
            event = events.recv() => Select::Recv(event.ok_or(anyhow::anyhow!("channel closed"))?),
            Some(result) = bind_tasks.join_next() => Select::JoinNextBind(result??),
            Some(result) = send_tasks.join_next() => Select::JoinNextSend(result?),
            Some(result) = recv_tasks.join_next() => Select::JoinNextRecv(result?),
        } {
            Select::Recv(Event::Offer(offer)) => {
                pending_bind.push(offer);
                // comparing to this ad-hoc binding, a bounded size ephemeral server pool may be
                // more desired
                bind_tasks.spawn(async move { Ok(TcpListener::bind((ip, 0)).await?) });
            }
            Select::JoinNextBind(listener) => {
                let offer = pending_bind.pop().unwrap();
                let addr = listener.local_addr()?;
                send_tasks.spawn(async move {
                    let task = async {
                        // escaping path to prevent the ephemeral server leaks forever
                        // that could happen e.g. remote does not accept the transfer
                        let (mut stream, _) =
                            timeout(Duration::from_millis(2500), listener.accept()).await??;
                        Result::<_, anyhow::Error>::Ok(stream.write_all(&offer.buf).await?)
                    };
                    let result = if let Some(cancel) = offer.cancel {
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
                // it's possible that the message arrives before listener start accepting
                // send inside spawned task requires clone and send `net`
                // i don't want that, instead do best effort spurious error mitigation with
                // active yielding
                // notice that such spuerious error is not hard error, just effectively false
                // positive cancel the transfer
                yield_now().await;
                net.send(offer.dest, Serve(offer.message, addr))?;
            }
            Select::JoinNextSend(()) => {}
            Select::Recv(Event::RecvServe(Recv(Serve(message, serve_addr)))) => {
                upcall.send(RecvOffer {
                    inner: message,
                    serve_addr: Some(serve_addr),
                })?
            }
            Select::Recv(Event::Accept(accept)) => {
                recv_tasks.spawn(async move {
                    let task = async {
                        let mut stream = TcpStream::connect(accept.serve_addr).await?;
                        let mut buf = Vec::new();
                        if let Some(expect_len) = accept.expect_len {
                            buf.resize(expect_len, Default::default());
                            stream.read_exact(&mut buf).await?;
                        } else {
                            stream.read_to_end(&mut buf).await?;
                        }
                        Result::<_, anyhow::Error>::Ok(buf)
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
        }
    }
}
