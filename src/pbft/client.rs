use std::collections::BTreeMap;

use crate::{
    codec::Payload,
    event::{OnErasedEvent, ScheduleEvent, SendEvent, TimerId},
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
    timer: TimerId,
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
    #[derive(Debug)]
    pub struct Resend;
}

pub trait Context<A> {
    type Net: SendMessage<u8, Request<A>> + SendMessage<All, Request<A>>;
    type Upcall: SendEvent<InvokeOk<Payload>>;
    type Schedule: ScheduleEvent<events::Resend>;
    fn net(&mut self) -> &mut Self::Net;
    fn upcall(&mut self) -> &mut Self::Upcall;
    fn schedule(&mut self) -> &mut Self::Schedule;
}

impl<A: Addr, C: Context<A>> OnErasedEvent<Invoke<Payload>, C> for State<A> {
    fn on_event(&mut self, Invoke(op): Invoke<Payload>, context: &mut C) -> anyhow::Result<()> {
        self.seq += 1;
        let timer = context
            .schedule()
            .set(self.config.client_resend_interval, || events::Resend)?;
        let replaced = self.outstanding.replace(Outstanding {
            op,
            timer,
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
        context.upcall().send(InvokeOk(reply.result))
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
