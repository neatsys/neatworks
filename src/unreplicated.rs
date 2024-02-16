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
            resend_timer: timer.set(Duration::from_millis(1000), || ClientEvent::ResendTimeout)?,
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
        timer: &mut dyn Timer<ClientEvent>,
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

pub type ReplicaEvent<A> = Request<A>;

pub struct Replica<S, N, A> {
    on_request: HashMap<u32, OnRequest<A, N>>,
    app: S,

    net: N,
}

type OnRequest<A, N> = Box<dyn Fn(&Request<A>, &mut N) -> anyhow::Result<bool> + Send + Sync>;

impl<S: Debug, N, A> Debug for Replica<S, N, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("app", &self.app)
            .finish_non_exhaustive()
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

impl<S: App, N: ToClientNet<A>, A: Addr> OnEvent<ReplicaEvent<A>> for Replica<S, N, A> {
    fn on_event(
        &mut self,
        event: ReplicaEvent<A>,
        _: &mut dyn Timer<ReplicaEvent<A>>,
    ) -> anyhow::Result<()> {
        self.on_ingress(event)
    }
}

impl<S: App, N: ToClientNet<A>, A: Addr> Replica<S, N, A> {
    fn on_ingress(&mut self, request: Request<A>) -> anyhow::Result<()> {
        if let Some(on_request) = self.on_request.get(&request.client_id) {
            if on_request(&request, &mut self.net)? {
                return Ok(());
            }
        }
        let result = self.app.execute(&request.op)?;
        let seq = request.seq;
        let reply = Reply { seq, result };
        let on_request = move |request: &Request<A>, net: &mut N| {
            if request.seq < seq {
                return Ok(true);
            }
            if request.seq == seq {
                net.send(request.client_addr.clone(), reply.clone())?;
                Ok(true)
            } else {
                Ok(false)
            }
        };
        on_request(&request, &mut self.net)?;
        self.on_request
            .insert(request.client_id, Box::new(on_request));
        Ok(())
    }
}

pub type ToClientMessageNet<T> = MessageNet<T, Reply>;

pub fn to_client_on_buf(buf: &[u8], sender: &mut impl SendEvent<Reply>) -> anyhow::Result<()> {
    sender.send(deserialize(buf)?)
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, Request<A>>;

pub fn to_replica_on_buf(
    buf: &[u8],
    sender: &mut impl SendEvent<Request<SocketAddr>>,
) -> anyhow::Result<()> {
    sender.send(deserialize(buf)?)
}

pub mod erased {
    use std::{net::SocketAddr, time::Duration};

    use crate::{
        app::App,
        event::{
            erased::{OnEvent, Timer},
            SendEvent,
        },
        net::{deserialize, events::Recv, Addr},
        replication::{Invoke, Request},
    };

    use super::{ClientInvoke, ClientUpcall, Replica, Reply, ToClientNet, ToReplicaNet};

    pub type Client<A> = super::Client<
        Box<dyn ToReplicaNet<A> + Send + Sync>,
        Box<dyn ClientUpcall + Send + Sync>,
        A,
    >;

    // try reduce duplicated code as for replica
    // unlikely to be done due to incompatible timer

    impl<N: ToReplicaNet<A>, U: ClientUpcall, A: Addr> OnEvent<Invoke> for super::Client<N, U, A> {
        fn on_event(
            &mut self,
            Invoke(op): Invoke,
            timer: &mut impl Timer<Self>,
        ) -> anyhow::Result<()> {
            if self.invoke.is_some() {
                anyhow::bail!("concurrent invocation")
            }
            self.seq += 1;
            let invoke = ClientInvoke {
                op,
                resend_timer: timer.set(Duration::from_millis(1000), Resend)?,
            };
            self.invoke = Some(invoke);
            self.do_send()
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
            if reply.seq != self.seq {
                return Ok(());
            }
            let Some(invoke) = self.invoke.take() else {
                return Ok(());
            };
            timer.unset(invoke.resend_timer)?;
            self.upcall.send((self.id, reply.result))
        }
    }

    impl<S: App, N: ToClientNet<A>, A: Addr> OnEvent<Recv<Request<A>>> for Replica<S, N, A> {
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
