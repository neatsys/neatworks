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
    #[derive(Debug, Clone)]
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
            N: SendEvent<Cast<(), Request<A>>> + 'static,
            U: SendEvent<InvokeOk<Bytes>> + 'static,
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
        model::{NetworkState, ScheduleState, State as _},
        workload::{
            app::{
                combinators::Typed,
                kvstore::{self, KVStore},
            },
            CloseLoop, Workload,
        },
    };

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub enum Addr {
        Client(u8),
        Server,
    }

    impl crate::net::Addr for Addr {}

    #[derive(Debug, Clone, From)]
    pub enum Message {
        Request(super::Request<Addr>),
        Reply(super::Reply),
    }

    #[derive(Debug, Clone)]
    pub enum Timer {
        ClientResend,
    }

    impl From<client::Resend> for Timer {
        fn from(client::Resend: client::Resend) -> Self {
            Self::ClientResend
        }
    }

    #[derive(Debug, Clone)]
    pub struct State<W> {
        pub clients: Vec<(ClientState<Addr>, ClientLocalContext<W>)>,
        server: ServerState<kvstore::App>,
        network: NetworkState<Addr, Message>,
    }

    #[derive(Debug, Clone)]
    pub struct ClientLocalContext<W> {
        pub upcall: CloseLoop<W, Option<Invoke<Bytes>>>,
        schedule: ScheduleState<Timer>,
    }

    struct ClientContextCarrier;

    impl<'a, W> super::context::On<ClientContext<'a, W>> for ClientContextCarrier {
        type Schedule = &'a mut ScheduleState<Timer>;
    }

    type ClientContext<'a, W> = super::context::Client<
        ClientContextCarrier,
        &'a mut NetworkState<Addr, Message>,
        &'a mut CloseLoop<W, Option<Invoke<Bytes>>>,
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

    #[derive(Debug)]
    pub enum Event {
        Message(Addr, Message),
        Timer(u8, TimerId, Timer),
    }

    impl<W: Workload<Op = Bytes, Result = Bytes>> SendEvent<Event> for State<W> {
        fn send(&mut self, event: Event) -> anyhow::Result<()> {
            match event {
                Event::Message(Addr::Client(index), _) | Event::Timer(index, ..) => {
                    let Some((client, context)) = self.clients.get_mut(index as usize) else {
                        anyhow::bail!("unexpected client index {index}")
                    };
                    let mut context = ClientContext {
                        net: &mut self.network,
                        upcall: &mut context.upcall,
                        schedule: &mut context.schedule,
                        _m: Default::default(),
                    };
                    match event {
                        Event::Message(_, Message::Reply(message)) => {
                            client.on_event(Recv(message), &mut context)
                        }
                        Event::Timer(_, id, Timer::ClientResend) => {
                            context.schedule.tick(id)?;
                            client.on_event(client::Resend, &mut context)
                        }
                        _ => anyhow::bail!("unexpected event {event:?}"),
                    }
                }
                Event::Message(Addr::Server, Message::Request(message)) => {
                    self.server.on_event(Recv(message), &mut self.network)
                }
                _ => anyhow::bail!("unexpected event {event:?}"),
            }
        }
    }

    impl<W: Workload<Op = Bytes, Result = Bytes>> crate::model::State for State<W> {
        type Event = Event;

        fn events(&self) -> Vec<Self::Event> {
            let mut events = Vec::new();
            for (index, (_, context)) in self.clients.iter().enumerate() {
                assert!(context.upcall.sender.is_none());
                context
                    .schedule
                    .generate_events(|id, event| events.push(Event::Timer(index as _, id, event)))
            }
            self.network
                .generate_events(|addr, message| events.push(Event::Message(addr, message)));
            events
        }

        fn fix(&mut self) -> anyhow::Result<()> {
            for (client, context) in &mut self.clients {
                if let Some(invoke) = context.upcall.sender.take() {
                    let mut context = ClientContext {
                        net: &mut self.network,
                        upcall: &mut context.upcall,
                        schedule: &mut context.schedule,
                        _m: Default::default(),
                    };
                    client.on_event(invoke, &mut context)?
                }
            }
            Ok(())
        }
    }

    impl<W> State<W> {
        pub fn new() -> Self {
            Self {
                server: ServerState::new(Typed::json(KVStore::new())),
                clients: Default::default(),
                network: NetworkState::new(),
            }
        }

        pub fn push_client(&mut self, workload: W) {
            let index = self.clients.len();
            let client = ClientState::new(index as _, Addr::Client(index as _));
            let context = ClientLocalContext {
                upcall: CloseLoop::new(workload, None),
                schedule: ScheduleState::new(),
            };
            self.clients.push((client, context));
        }
    }

    impl<W> Default for State<W> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<W: Workload<Op = Bytes, Result = Bytes>> State<W> {
        pub fn init(&mut self) -> anyhow::Result<()> {
            for (_, context) in &mut self.clients {
                context.upcall.init()?
            }
            self.fix()
        }
    }
}
