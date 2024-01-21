use std::{collections::HashMap, fmt::Debug, net::SocketAddr, time::Duration};

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    event::{OnEvent, SendEvent, TimerEngine},
    net::{Addr, SendBuf, SendMessage},
    replication::Request,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Vec<u8>,
}

pub trait ToClientNet: SendMessage<Reply> {}
impl<T: SendMessage<Reply>> ToClientNet for T {}

pub trait ToReplicaNet<A>: SendMessage<Request<A>, Addr = u8> {}
impl<T: SendMessage<Request<A>, Addr = u8>, A> ToReplicaNet<A> for T {}

#[derive(Debug, Clone, derive_more::From)]
pub enum ClientEvent {
    Invoke(Vec<u8>),
    Ingress(Reply),
    ResendTimeout,
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
    resend_timer: u32,
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
        timer: TimerEngine<'_, ClientEvent>,
    ) -> anyhow::Result<()> {
        match event {
            ClientEvent::Invoke(op) => self.on_invoke(op, timer),
            ClientEvent::ResendTimeout => self.on_resend_timeout(),
            ClientEvent::Ingress(reply) => self.on_ingress(reply, timer),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> Client<N, U, A> {
    fn on_invoke(
        &mut self,
        op: Vec<u8>,
        mut timer: TimerEngine<'_, ClientEvent>,
    ) -> anyhow::Result<()> {
        if self.invoke.is_some() {
            anyhow::bail!("concurrent invocation")
        }
        self.seq += 1;
        let invoke = ClientInvoke {
            op,
            resend_timer: timer.set(Duration::from_millis(1000), ClientEvent::ResendTimeout),
        };
        self.invoke = Some(invoke);
        self.do_send()
    }

    fn on_resend_timeout(&self) -> anyhow::Result<()> {
        // TODO logging
        self.do_send()
    }

    fn on_ingress(
        &mut self,
        reply: Reply,
        mut timer: TimerEngine<'_, ClientEvent>,
    ) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(invoke) = self.invoke.take() else {
            return Ok(());
        };
        timer.unset(invoke.resend_timer)?;
        self.upcall.send((self.id, reply.result))
    }

    fn do_send(&self) -> anyhow::Result<()> {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self.invoke.as_ref().unwrap().op.clone(),
        };
        self.net.send(0, &request)
    }
}

pub type ReplicaEvent<A> = Request<A>;

pub struct Replica<S, N, A> {
    on_request: HashMap<u32, OnRequest<A, N>>,
    app: S,

    net: N,
}

type OnRequest<A, N> = Box<dyn Fn(&Request<A>, &N) -> anyhow::Result<bool> + Send + Sync>;

impl<S, N, A> Debug for Replica<S, N, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Replica {{ .. }}")
    }
}

impl<S, N, A> Replica<S, N, A> {
    pub fn new(app: S, net: N) -> Self {
        Self {
            app,
            net,
            on_request: Default::default(),
        }
    }
}

impl<S: App, N: ToClientNet> OnEvent<ReplicaEvent<N::Addr>> for Replica<S, N, N::Addr> {
    fn on_event(
        &mut self,
        event: ReplicaEvent<N::Addr>,
        _: TimerEngine<'_, ReplicaEvent<N::Addr>>,
    ) -> anyhow::Result<()> {
        self.on_ingress(event)
    }
}

impl<S: App, N: ToClientNet> Replica<S, N, N::Addr> {
    fn on_ingress(&mut self, request: Request<N::Addr>) -> anyhow::Result<()> {
        if let Some(on_request) = self.on_request.get(&request.client_id) {
            if on_request(&request, &self.net)? {
                return Ok(());
            }
        }
        let result = self.app.execute(&request.op)?;
        let seq = request.seq;
        let reply = Reply { seq, result };
        let on_request = move |request: &Request<N::Addr>, net: &N| {
            if request.seq < seq {
                return Ok(true);
            }
            if request.seq == seq {
                net.send(request.client_addr.clone(), &reply)?;
                Ok(true)
            } else {
                Ok(false)
            }
        };
        on_request(&request, &self.net)?;
        self.on_request
            .insert(request.client_id, Box::new(on_request));
        Ok(())
    }
}

#[derive(Debug)]
pub struct ToClientMessageNet<T>(pub T);

impl<T: SendBuf> SendMessage<Reply> for ToClientMessageNet<T> {
    type Addr = T::Addr;

    fn send(&self, dest: Self::Addr, message: &Reply) -> anyhow::Result<()> {
        let buf = bincode::options().serialize(message)?;
        self.0.send(dest, buf)
    }
}

pub fn to_client_on_buf(sender: &impl SendEvent<Reply>, buf: &[u8]) -> anyhow::Result<()> {
    let message = bincode::options().allow_trailing_bytes().deserialize(buf)?;
    sender.send(message)
}

#[derive(Debug)]
pub struct ToReplicaMessageNet<T>(pub T);

impl<T: SendBuf, A: Addr> SendMessage<Request<A>> for ToReplicaMessageNet<T> {
    type Addr = T::Addr;

    fn send(&self, dest: Self::Addr, message: &Request<A>) -> anyhow::Result<()> {
        let buf = bincode::options().serialize(message)?;
        self.0.send(dest, buf)
    }
}

pub fn to_replica_on_buf(
    sender: &impl SendEvent<Request<SocketAddr>>,
    buf: &[u8],
) -> anyhow::Result<()> {
    let message = bincode::options().allow_trailing_bytes().deserialize(buf)?;
    sender.send(message)
}
