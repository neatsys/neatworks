use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    event::{
        semantic::{Invoke, InvokeOk},
        OnErasedEvent, ScheduleEvent, SendEvent, TimerId,
    },
    net::{
        events::{Recv, Send},
        Addr,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    seq: u32,
    op: Bytes,
    client_id: u32,
    client_addr: A,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientState<A> {
    id: u32,
    addr: A,
    seq: u32,
    outstanding: Option<Outstanding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Outstanding {
    op: Bytes,
    timer: TimerId,
}

pub mod client {
    #[derive(Debug)]
    pub struct Resend;
}

pub trait ClientContext<A> {
    type Net: SendEvent<Send<(), Request<A>>>;
    type Upcall: SendEvent<InvokeOk<Bytes>>;
    type Schedule: ScheduleEvent<client::Resend>;
    fn net(&mut self) -> &mut Self::Net;
    fn upcall(&mut self) -> &mut Self::Upcall;
    fn schedule(&mut self) -> &mut Self::Schedule;
}

impl<A: Addr, C: ClientContext<A>> OnErasedEvent<Invoke<Bytes>, C> for ClientState<A> {
    fn on_event(&mut self, Invoke(op): Invoke<Bytes>, context: &mut C) -> anyhow::Result<()> {
        self.seq += 1;
        let replaced = self.outstanding.replace(Outstanding {
            op,
            timer: context
                .schedule()
                .set(Duration::from_millis(100), client::Resend)?,
        });
        anyhow::ensure!(replaced.is_none());
        self.send_request(context)
    }
}

impl<A: Addr> ClientState<A> {
    fn send_request(&self, context: &mut impl ClientContext<A>) -> anyhow::Result<()> {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self
                .outstanding
                .as_ref()
                .expect("there is outstanding invocation")
                .op
                .clone(),
        };
        context.net().send(Send((), request))
    }
}

impl<A, C: ClientContext<A>> OnErasedEvent<Recv<Reply>, C> for ClientState<A> {
    fn on_event(&mut self, Recv(reply): Recv<Reply>, context: &mut C) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(outstanding) = self.outstanding.take() else {
            return Ok(());
        };
        context.schedule().unset(outstanding.timer)?;
        context.upcall().send(InvokeOk(reply.result))
    }
}

impl<A: Addr, C: ClientContext<A>> OnErasedEvent<client::Resend, C> for ClientState<A> {
    fn on_event(&mut self, client::Resend: client::Resend, context: &mut C) -> anyhow::Result<()> {
        // TODO log
        self.send_request(context)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ServerState {
    replies: BTreeMap<u32, Reply>,
}

impl ServerState {
    pub fn new() -> Self {
        Self::default()
    }
}

pub trait ServerContext<A> {
    type Net: SendEvent<Send<A, Reply>>;
    fn net(&mut self) -> &mut Self::Net;
}

impl<A, C: ServerContext<A>> OnErasedEvent<Recv<Request<A>>, C> for ServerState {
    fn on_event(&mut self, Recv(request): Recv<Request<A>>, context: &mut C) -> anyhow::Result<()> {
        match self.replies.get(&request.client_id) {
            Some(reply) if reply.seq > request.seq => return Ok(()),
            Some(reply) if reply.seq == request.seq => {
                return context.net().send(Send(request.client_addr, reply.clone()))
            }
            _ => {}
        }
        let reply = Reply {
            seq: request.seq,
            result: Default::default(), // TODO
        };
        self.replies.insert(request.client_id, reply.clone());
        context.net().send(Send(request.client_addr, reply))
    }
}

pub mod context {
    use super::*;

    pub struct Client<N, U, T> {
        pub net: N,
        pub upcall: U,
        pub schedule: T,
    }

    impl<N, U, T, A> ClientContext<A> for Client<N, U, T>
    where
        N: SendEvent<Send<(), Request<A>>>,
        U: SendEvent<InvokeOk<Bytes>>,
        T: ScheduleEvent<client::Resend>,
    {
        type Net = N;
        type Upcall = U;
        type Schedule = T;
        fn net(&mut self) -> &mut Self::Net {
            &mut self.net
        }
        fn upcall(&mut self) -> &mut Self::Upcall {
            &mut self.upcall
        }
        fn schedule(&mut self) -> &mut Self::Schedule {
            &mut self.schedule
        }
    }

    pub struct Server<N> {
        pub net: N,
    }

    impl<N, A> ServerContext<A> for Server<N>
    where
        N: SendEvent<Send<A, Reply>>,
    {
        type Net = N;
        fn net(&mut self) -> &mut Self::Net {
            &mut self.net
        }
    }
}
