use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    codec::Payload,
    event::{OnErasedEvent, ScheduleEvent, SendEvent, TimerId},
    net::{
        events::{Cast, Recv},
        Addr,
    },
    workload::{
        events::{Invoke, InvokeOk},
        App,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    seq: u32,
    op: Payload,
    client_id: u32,
    client_addr: A,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Payload,
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
    op: Payload,
    timer: TimerId,
}

impl<A> ClientState<A> {
    pub fn new(id: u32, addr: A) -> Self {
        Self {
            id,
            addr,
            seq: 0,
            outstanding: Default::default(),
        }
    }
}

pub mod client {
    #[derive(Debug)]
    pub struct Resend;
}

pub trait ClientContext<A> {
    type Net: SendEvent<Cast<(), Request<A>>>;
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
            op: Payload(op),
            timer: context
                .schedule()
                .set(Duration::from_millis(100), || client::Resend)?,
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
        context.net().send(Cast((), request))
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
        let Payload(result) = reply.result;
        context.upcall().send(InvokeOk(result))
    }
}

impl<A: Addr, C: ClientContext<A>> OnErasedEvent<client::Resend, C> for ClientState<A> {
    fn on_event(&mut self, client::Resend: client::Resend, context: &mut C) -> anyhow::Result<()> {
        // TODO log
        self.send_request(context)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerState<S> {
    replies: BTreeMap<u32, Reply>,
    app: S,
}

impl<S> ServerState<S> {
    pub fn new(app: S) -> Self {
        Self {
            app,
            replies: Default::default(),
        }
    }
}

pub trait ServerContext<A> {
    type Net: SendEvent<Cast<A, Reply>>;
    fn net(&mut self) -> &mut Self::Net;
}

impl<S: App, A, C: ServerContext<A>> OnErasedEvent<Recv<Request<A>>, C> for ServerState<S> {
    fn on_event(&mut self, Recv(request): Recv<Request<A>>, context: &mut C) -> anyhow::Result<()> {
        match self.replies.get(&request.client_id) {
            Some(reply) if reply.seq > request.seq => return Ok(()),
            Some(reply) if reply.seq == request.seq => {
                return context.net().send(Cast(request.client_addr, reply.clone()))
            }
            _ => {}
        }
        let reply = Reply {
            seq: request.seq,
            result: Payload(self.app.execute(&request.op)?),
        };
        self.replies.insert(request.client_id, reply.clone());
        context.net().send(Cast(request.client_addr, reply))
    }
}

pub mod context {
    use std::marker::PhantomData;

    use super::*;

    pub struct Client<O: On<Self>, N, U, A> {
        pub net: N,
        pub upcall: U,
        pub schedule: O::Schedule,
        pub _m: PhantomData<A>,
    }

    pub trait On<C> {
        type Schedule: ScheduleEvent<client::Resend>;
    }

    impl<O: On<Self>, N, U, A> super::ClientContext<A> for Client<O, N, U, A>
    where
        N: SendEvent<Cast<(), Request<A>>>,
        U: SendEvent<InvokeOk<Bytes>>,
    {
        type Net = N;
        type Upcall = U;
        type Schedule = O::Schedule;
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
        N: SendEvent<Cast<A, Reply>>,
    {
        type Net = N;
        fn net(&mut self) -> &mut Self::Net {
            &mut self.net
        }
    }

    mod task {
        use crate::event::task::{erase::ScheduleState, Context};

        use super::*;

        impl<N, U, A: Addr> On<Client<Self, N, U, A>> for Context
        where
            N: SendEvent<Cast<(), Request<A>>>,
            U: SendEvent<InvokeOk<Bytes>>,
        {
            type Schedule = ScheduleState<ClientState<A>, Client<Self, N, U, A>>;
        }
    }
}

pub mod codec {
    use crate::codec::{bincode_decode, Encode};

    use super::*;

    pub fn client_encode<A: Addr, N>(net: N) -> Encode<Request<A>, N> {
        Encode::bincode(net)
    }

    pub fn client_decode<'a>(
        mut sender: impl SendEvent<Recv<Reply>> + 'a,
    ) -> impl FnMut(&[u8]) -> anyhow::Result<()> + 'a {
        move |buf| sender.send(Recv(bincode_decode(buf)?))
    }

    pub fn server_encode<N>(net: N) -> Encode<Reply, N> {
        Encode::bincode(net)
    }

    pub fn server_decode<'a, A: Addr>(
        mut sender: impl SendEvent<Recv<Request<A>>> + 'a,
    ) -> impl FnMut(&[u8]) -> anyhow::Result<()> + 'a {
        move |buf| sender.send(Recv(bincode_decode(buf)?))
    }
}

pub mod model {
    use derive_more::From;

    use crate::{
        event::combinators::Transient,
        model::{NetworkState, TimerState},
        workload::app::kvstore::KVStore,
    };

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub enum Addr {
        Client(u8),
        Server,
    }

    impl crate::net::Addr for Addr {}

    #[derive(Debug, From)]
    pub enum Message {
        Request(super::Request<Addr>),
        Reply(super::Reply),
    }

    #[derive(Debug)]
    pub enum Timer {
        ClientResend,
    }

    impl From<client::Resend> for Timer {
        fn from(client::Resend: client::Resend) -> Self {
            Self::ClientResend
        }
    }

    pub struct State {
        clients: Vec<ClientState<Addr>>,
        server: ServerState<KVStore>,
        client_contexts: Vec<ClientLocalContext>,
        network: NetworkState<Addr, Message>,
    }

    struct ClientLocalContext {
        upcall: Transient<InvokeOk<Bytes>>,
        schedule: TimerState<Timer>,
    }

    struct ClientContextCarrier;

    impl<'a> super::context::On<ClientContext<'a>> for ClientContextCarrier {
        type Schedule = &'a mut TimerState<Timer>;
    }

    type ClientContext<'a> = super::context::Client<
        ClientContextCarrier,
        &'a mut NetworkState<Addr, Message>,
        &'a mut Transient<InvokeOk<Bytes>>,
        Addr,
    >;

    impl SendEvent<Cast<(), Request<Addr>>> for NetworkState<Addr, Message> {
        fn send(&mut self, Cast((), message): Cast<(), Request<Addr>>) -> anyhow::Result<()> {
            self.send(Cast(Addr::Server, message))
        }
    }

    impl ServerContext<Addr> for NetworkState<Addr, Message> {
        type Net = Self;
        fn net(&mut self) -> &mut Self::Net {
            self
        }
    }
}
