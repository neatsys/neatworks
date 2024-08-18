use std::collections::BTreeMap;

use bytes::Bytes;

use crate::{
    codec::Payload,
    event::{OnErasedEvent, ScheduleEvent, SendEvent, ActiveTimer},
    net::{combinators::All, events::Recv, Addr, SendMessage},
    workload::events::{Invoke, InvokeOk},
};

use super::{
    messages::{Reply, Request},
    PublicParameters,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct State<A> {
    id: u32,
    addr: A,
    config: PublicParameters,

    seq: u32,
    outstanding: Option<Outstanding>,
    view_num: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Outstanding {
    op: Payload,
    replies: BTreeMap<u8, Reply>,
    timer: ActiveTimer,
}

impl<A> State<A> {
    pub fn new(id: u32, addr: A, config: PublicParameters) -> Self {
        Self {
            id,
            addr,
            config,

            seq: 0,
            outstanding: Default::default(),
            view_num: 0,
        }
    }
}

pub mod events {
    #[derive(Debug, Clone)]
    pub struct Resend;
}

pub trait Context<A> {
    type Net: SendMessage<u8, Request<A>> + SendMessage<All, Request<A>>;
    type Upcall: SendEvent<InvokeOk<Bytes>>;
    type Schedule: ScheduleEvent<events::Resend>;
    fn net(&mut self) -> &mut Self::Net;
    fn upcall(&mut self) -> &mut Self::Upcall;
    fn schedule(&mut self) -> &mut Self::Schedule;
}

impl<A: Addr, C: Context<A>> OnErasedEvent<Invoke<Bytes>, C> for State<A> {
    fn on_event(&mut self, Invoke(op): Invoke<Bytes>, context: &mut C) -> anyhow::Result<()> {
        self.seq += 1;
        let replaced = self.outstanding.replace(Outstanding {
            op: Payload(op),
            timer: context
                .schedule()
                .set(self.config.client_resend_interval, events::Resend)?,
            replies: Default::default(),
        });
        anyhow::ensure!(replaced.is_none());
        self.send_request(
            (self.view_num as usize % self.config.num_replica) as u8,
            context,
        )
    }
}

impl<A: Addr, C: Context<A>> OnErasedEvent<events::Resend, C> for State<A> {
    fn on_event(&mut self, events::Resend: events::Resend, context: &mut C) -> anyhow::Result<()> {
        // warn!("Resend timeout on seq {}", self.seq);
        self.send_request(All, context)
    }
}

impl<A, C: Context<A>> OnErasedEvent<Recv<Reply>, C> for State<A> {
    fn on_event(&mut self, Recv(reply): Recv<Reply>, context: &mut C) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(invoke) = self.outstanding.as_mut() else {
            return Ok(());
        };
        invoke.replies.insert(reply.replica_id, reply.clone());
        // println!("{:?}", invoke.replies);
        if invoke
            .replies
            .values()
            .filter(|inserted_reply| inserted_reply.result == reply.result)
            .count()
            != self.config.num_faulty + 1
        {
            return Ok(());
        }
        // paper is not saying what does it mean by "what it believes is the current primary"
        // either taking min or max of the view numbers seems wrong, so i choose to design nothing
        self.view_num = reply.view_num;
        context
            .schedule()
            .unset(self.outstanding.take().unwrap().timer)?;
        let Payload(result) = reply.result;
        context.upcall().send(InvokeOk(result))
    }
}

impl<A: Addr> State<A> {
    fn send_request<B, C: Context<A>>(&mut self, dest: B, context: &mut C) -> anyhow::Result<()>
    where
        C::Net: SendMessage<B, Request<A>>,
    {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self.outstanding.as_ref().unwrap().op.clone(),
        };
        context.net().send(dest, request)
    }
}

pub mod context {
    use std::marker::PhantomData;

    use super::*;

    pub struct Context<O: On<Self>, N, U, A> {
        pub net: N,
        pub upcall: U,
        pub schedule: O::Schedule,
        pub _m: PhantomData<A>,
    }

    pub trait On<C> {
        type Schedule: ScheduleEvent<events::Resend>;
    }

    impl<O: On<Self>, N, U, A> super::Context<A> for Context<O, N, U, A>
    where
        N: SendMessage<u8, Request<A>> + SendMessage<All, Request<A>>,
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

    mod task {
        use crate::event::task::{erase::ScheduleState, ContextCarrier as Task};

        use super::*;

        impl<N, U, A: Addr> On<Context<Self, N, U, A>> for Task
        where
            N: SendMessage<u8, Request<A>> + SendMessage<All, Request<A>> + 'static,
            U: SendEvent<InvokeOk<Bytes>> + 'static,
        {
            type Schedule = ScheduleState<State<A>, Context<Self, N, U, A>>;
        }
    }
}
