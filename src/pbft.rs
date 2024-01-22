use std::{collections::HashMap, time::Duration};

use bincode::Options as _;
use serde::{Deserialize, Serialize};

use crate::{
    crypto::Signed,
    event::{OnEvent, SendEvent, Timer, TimerId},
    net::{Addr, MessageNet, SendMessage},
    replication::Request,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrePrepare {
    view_num: u32,
    op_num: u32,
    digest: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prepare {
    view_num: u32,
    op_num: u32,
    digest: [u8; 32],
    replica_id: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    view_num: u32,
    op_num: u32,
    digest: [u8; 32],
    replica_id: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Vec<u8>,
    view_num: u32,
    replica_id: u8,
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
    view_num: u32,
    num_replica: usize,
    num_faulty: usize,

    net: N,
    upcall: U,
}

#[derive(Debug)]
struct ClientInvoke {
    op: Vec<u8>,
    resend_timer: TimerId,
    replies: HashMap<u8, Reply>,
}

impl<N, U, A> Client<N, U, A> {
    pub fn new(id: u32, addr: A, net: N, upcall: U, num_replica: usize, num_faulty: usize) -> Self {
        Self {
            id,
            addr,
            net,
            upcall,
            num_replica,
            num_faulty,
            seq: 0,
            view_num: 0,
            invoke: Default::default(),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<ClientEvent> for Client<N, U, A> {
    fn on_event(
        &mut self,
        event: ClientEvent,
        timer: &mut dyn Timer<ClientEvent>,
    ) -> anyhow::Result<()> {
        match event {
            ClientEvent::Invoke(op) => self.on_invoke(op, timer),
            ClientEvent::ResendTimeout => self.on_resend_timeout(),
            ClientEvent::Ingress(reply) => self.on_ingress(reply, timer),
        }
    }
}

impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> Client<N, U, A> {
    fn on_invoke(&mut self, op: Vec<u8>, timer: &mut dyn Timer<ClientEvent>) -> anyhow::Result<()> {
        if self.invoke.is_some() {
            anyhow::bail!("concurrent invocation")
        }
        self.seq += 1;
        let invoke = ClientInvoke {
            op,
            resend_timer: timer.set(Duration::from_millis(1000), ClientEvent::ResendTimeout)?,
            replies: Default::default(),
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
        timer: &mut dyn Timer<ClientEvent>,
    ) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(invoke) = self.invoke.as_mut() else {
            return Ok(());
        };
        invoke.replies.insert(reply.replica_id, reply.clone());
        if invoke
            .replies
            .values()
            .filter(|inserted_reply| inserted_reply.result == reply.result)
            .count()
            == self.num_faulty + 1
        {
            self.view_num = reply.view_num;
            let invoke = self.invoke.take().unwrap();
            timer.unset(invoke.resend_timer)?;
            self.upcall.send((self.id, reply.result))
        } else {
            Ok(())
        }
    }

    fn do_send(&self) -> anyhow::Result<()> {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self.invoke.as_ref().unwrap().op.clone(),
        };
        // TODO broadcast on resend
        self.net
            .send((self.view_num as usize % self.num_replica) as _, &request)
    }
}

pub type ToClientMessageNet<T> = MessageNet<T, Reply>;

pub fn to_client_on_buf(sender: &impl SendEvent<Reply>, buf: &[u8]) -> anyhow::Result<()> {
    let message = bincode::options().allow_trailing_bytes().deserialize(buf)?;
    sender.send(message)
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum ToReplica<A> {
    Request(Request<A>),
    PrePrepare(Signed<PrePrepare>, Vec<Request<A>>),
    Prepare(Signed<Prepare>),
    Commit(Signed<Commit>),
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, ToReplica<A>>;

pub fn to_replica_on_buf<A: Addr>(
    sender: &(impl SendEvent<Request<A>>
          + SendEvent<(Signed<PrePrepare>, Vec<Request<A>>)>
          + SendEvent<Signed<Prepare>>
          + SendEvent<Signed<Commit>>),
    buf: &[u8],
) -> anyhow::Result<()> {
    match bincode::options().allow_trailing_bytes().deserialize(buf)? {
        ToReplica::Request(message) => sender.send(message),
        ToReplica::PrePrepare(message, requests) => sender.send((message, requests)),
        ToReplica::Prepare(message) => sender.send(message),
        ToReplica::Commit(message) => sender.send(message),
    }
}
