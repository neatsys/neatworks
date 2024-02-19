use std::{collections::HashMap, fmt::Debug, net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    event::{OnEvent, SendEvent, Timer, TimerId},
    net::{deserialize, Addr, MessageNet, SendMessage},
    replication::{Invoke, Request},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Vec<u8>,
}

pub trait ToClientNet<A>: SendMessage<A, Reply> {}
impl<T: SendMessage<A, Reply>, A> ToClientNet<A> for T {}

pub trait ToReplicaNet<A>: SendMessage<u8, Request<A>> {}
impl<T: SendMessage<u8, Request<A>>, A> ToReplicaNet<A> for T {}

#[derive(Debug, Clone, derive_more::From)]
pub enum ClientEvent {
    Invoke(Vec<u8>),
    Ingress(Reply),
    ResendTimeout,
}

impl From<Invoke> for ClientEvent {
    fn from(Invoke(op): Invoke) -> Self {
        Self::Invoke(op)
    }
}

pub trait ClientUpcall: SendEvent<(u32, Vec<u8>)> {}
impl<T: SendEvent<(u32, Vec<u8>)>> ClientUpcall for T {}

#[derive(Debug)]
pub struct Client<N, U, A> {
    id: u32,
    addr: A,
    seq: u32,
    invoke: Option<ClientInvoke>,

    net: N,
    upcall: U,
}

#[derive(Debug)]
struct ClientInvoke {
    op: Vec<u8>,
    resend_timer: TimerId,
}

impl<N, U, A> Client<N, U, A> {
    pub fn new(id: u32, addr: A, net: N, upcall: U) -> Self {
        Self {
            id,
            addr,
            net,
            upcall,
            seq: 0,
            invoke: Default::default(),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<ClientEvent> for Client<N, U, A> {
    fn on_event(
        &mut self,
        event: ClientEvent,
        timer: &mut impl Timer<ClientEvent>,
    ) -> anyhow::Result<()> {
        match event {
            ClientEvent::Invoke(op) => self.on_invoke(op, |period| {
                timer.set(period, || ClientEvent::ResendTimeout)
            }),
            ClientEvent::ResendTimeout => self.on_resend_timeout(),
            ClientEvent::Ingress(reply) => self.on_ingress(reply, |timer_id| timer.unset(timer_id)),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> Client<N, U, A> {
    fn on_invoke(
        &mut self,
        op: Vec<u8>,
        mut set_resend_timer: impl FnMut(Duration) -> anyhow::Result<TimerId>,
    ) -> anyhow::Result<()> {
        if self.invoke.is_some() {
            anyhow::bail!("concurrent invocation")
        }
        self.seq += 1;
        let invoke = ClientInvoke {
            op,
            resend_timer: set_resend_timer(Duration::from_millis(1000))?,
        };
        self.invoke = Some(invoke);
        self.do_send()
    }

    fn on_resend_timeout(&mut self) -> anyhow::Result<()> {
        // TODO logging
        self.do_send()
    }

    fn on_ingress(
        &mut self,
        reply: Reply,
        mut unset_timer: impl FnMut(TimerId) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(invoke) = self.invoke.take() else {
            return Ok(());
        };
        unset_timer(invoke.resend_timer)?;
        self.upcall.send((self.id, reply.result))
    }

    fn do_send(&mut self) -> anyhow::Result<()> {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self.invoke.as_ref().unwrap().op.clone(),
        };
        self.net.send(0, request)
    }
}

#[derive(Debug)]
pub enum ReplicaEvent<A> {
    Ingress(Request<A>),
    Dummy, //
}

pub struct Replica<S, N, A> {
    replies: HashMap<u32, Reply>,
    _addr_marker: std::marker::PhantomData<A>,
    app: S,
    net: N,
}

impl<S: Debug, N, A> Debug for Replica<S, N, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("replies", &self.replies)
            .field("app", &self.app)
            .finish_non_exhaustive()
    }
}

impl<S, N, A> Replica<S, N, A> {
    pub fn new(app: S, net: N) -> Self {
        Self {
            app,
            net,
            _addr_marker: Default::default(),
            replies: Default::default(),
        }
    }
}

impl<S: App, N: ToClientNet<A>, A> OnEvent<ReplicaEvent<A>> for Replica<S, N, A> {
    fn on_event(
        &mut self,
        event: ReplicaEvent<A>,
        _: &mut impl Timer<ReplicaEvent<A>>,
    ) -> anyhow::Result<()> {
        match event {
            ReplicaEvent::Ingress(request) => self.on_ingress(request),
            ReplicaEvent::Dummy => unreachable!(),
        }
    }
}

impl<S: App, N: ToClientNet<A>, A> Replica<S, N, A> {
    fn on_ingress(&mut self, request: Request<A>) -> anyhow::Result<()> {
        match self.replies.get(&request.client_id) {
            Some(reply) if reply.seq > request.seq => return Ok(()),
            Some(reply) if reply.seq == request.seq => {
                return self.net.send(request.client_addr, reply.clone())
            }
            _ => {}
        }
        let reply = Reply {
            seq: request.seq,
            result: self.app.execute(&request.op)?,
        };
        self.replies.insert(request.client_id, reply.clone());
        self.net.send(request.client_addr, reply)
    }
}

pub type ToClientMessageNet<T> = MessageNet<T, Reply>;

pub fn to_client_on_buf(buf: &[u8], sender: &mut impl SendEvent<Reply>) -> anyhow::Result<()> {
    sender.send(deserialize(buf)?)
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, Request<A>>;

pub fn to_replica_on_buf(
    buf: &[u8],
    sender: &mut impl SendEvent<ReplicaEvent<SocketAddr>>,
) -> anyhow::Result<()> {
    sender.send(ReplicaEvent::Ingress(deserialize(buf)?))
}

pub mod erased {
    use std::net::SocketAddr;

    use crate::{
        app::App,
        event::{
            erased::{OnEvent, Timer},
            SendEvent,
        },
        net::{deserialize, events::Recv, Addr},
        replication::{Invoke, Request},
    };

    use super::{ClientUpcall, Reply, ToClientNet, ToReplicaNet};

    pub type Client<A> = super::Client<
        Box<dyn ToReplicaNet<A> + Send + Sync>,
        Box<dyn ClientUpcall + Send + Sync>,
        A,
    >;

    impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<Invoke> for super::Client<N, U, A> {
        fn on_event(
            &mut self,
            Invoke(op): Invoke,
            timer: &mut impl Timer<Self>,
        ) -> anyhow::Result<()> {
            self.on_invoke(op, |period| timer.set(period, Resend))
        }
    }

    #[derive(Debug, Clone)]
    struct Resend;

    impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<Resend> for super::Client<N, U, A> {
        fn on_event(&mut self, Resend: Resend, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
            // TODO logging
            self.do_send()
        }
    }

    impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<Recv<Reply>> for super::Client<N, U, A> {
        fn on_event(
            &mut self,
            Recv(reply): Recv<Reply>,
            timer: &mut impl Timer<Self>,
        ) -> anyhow::Result<()> {
            self.on_ingress(reply, |timer_id| timer.unset(timer_id))
        }
    }

    pub type Replica<S, A> = super::Replica<S, Box<dyn ToClientNet<A> + Send + Sync>, A>;

    impl<S: App, N: ToClientNet<A>, A> OnEvent<Recv<Request<A>>> for super::Replica<S, N, A> {
        fn on_event(
            &mut self,
            Recv(request): Recv<Request<A>>,
            _: &mut impl Timer<Self>,
        ) -> anyhow::Result<()> {
            self.on_ingress(request)
        }
    }

    pub fn to_client_on_buf(
        buf: &[u8],
        sender: &mut impl SendEvent<Recv<Reply>>,
    ) -> anyhow::Result<()> {
        sender.send(Recv(deserialize(buf)?))
    }

    pub fn to_replica_on_buf(
        buf: &[u8],
        sender: &mut impl SendEvent<Recv<Request<SocketAddr>>>,
    ) -> anyhow::Result<()> {
        sender.send(Recv(deserialize(buf)?))
    }
}
