use std::collections::BTreeSet;

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    app::KVStore,
    crypto::{
        events::{Signed, Verified},
        Crypto,
        CryptoFlavor::Plain,
        Verifiable,
    },
    event::{linear::Timer, SendEvent, TimerId, Transient},
    net::{IndexNet, SendMessage},
    worker::Worker,
    workload::{CloseLoop, Invoke, InvokeOk, Workload},
};

use super::{Commit, PrePrepare, Prepare, Reply, Request};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Addr {
    Client(u8),
    Replica(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From)]
pub enum Message {
    Request(Request<Addr>),
    PrePrepare(Verifiable<PrePrepare>, Vec<Request<Addr>>),
    Prepare(Verifiable<Prepare>),
    Commit(Verifiable<Commit>),
    Reply(Reply),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageEvent {
    dest: Addr,
    message: Message,
}

impl<M: Into<Message>> SendMessage<Addr, M> for Transient<MessageEvent> {
    fn send(&mut self, dest: Addr, message: M) -> anyhow::Result<()> {
        SendEvent::send(
            self,
            MessageEvent {
                dest,
                message: message.into(),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimerEvent {
    addr: Addr,
    timer_id: TimerId,
}

#[derive(Debug, Clone, derive_more::From)]
pub enum CryptoEvent {
    SignedPrePrepare(Signed<PrePrepare>, Vec<Request<Addr>>),
    VerifiedPrePrepare(Verified<PrePrepare>, Vec<Request<Addr>>),
    SignedPrepare(Signed<Prepare>),
    VerifiedPrepare(Verified<Prepare>),
    SignedCommit(Signed<Commit>),
    VerifiedCommit(Verified<Commit>),
}

type Client = super::Client<IndexNet<Transient<MessageEvent>, Addr>, Transient<InvokeOk>, Addr>;
type Replica = super::Replica<
    IndexNet<Transient<MessageEvent>, Addr>,
    Transient<MessageEvent>,
    Worker<Crypto, Transient<CryptoEvent>>,
    KVStore,
    Addr,
>;

#[derive_where(PartialEq, Eq, Hash; W::Attach)]
#[derive_where(Debug, Clone; W, W::Attach)]
pub struct State<W: Workload, const CHECK: bool = false> {
    pub clients: Vec<ClientState<W>>,
    pub replicas: Vec<ReplicaState>,
    message_events: BTreeSet<MessageEvent>,
}

#[derive_where(Debug, Clone; W, W::Attach)]
#[derive_where(PartialEq, Eq, Hash; W::Attach)]
pub struct ClientState<W: Workload> {
    pub state: Client,
    timer: Timer,
    pub close_loop: CloseLoop<W, Transient<Invoke>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaState {
    pub state: Replica,
    timer: Timer,
}

impl<W: Workload, const CHECK: bool> State<W, CHECK> {
    pub fn new(num_replica: usize, num_faulty: usize) -> Self {
        let addrs = (0..num_replica)
            .map(|i| Addr::Replica(i as _))
            .collect::<Vec<_>>();
        Self {
            clients: Default::default(),
            replicas: (0..num_replica)
                .map(|i| {
                    Replica::new(
                        i as _,
                        KVStore::new(),
                        IndexNet::new(Transient::default(), addrs.clone(), i),
                        Transient::default(),
                        Worker::Inline(
                            Crypto::new_hardcoded_replication(num_replica, i, Plain)
                                .expect("plain crypto always can be created"),
                            Transient::default(),
                        ),
                        num_replica,
                        num_faulty,
                    )
                })
                .map(|state| ReplicaState {
                    state,
                    timer: Timer::default(),
                })
                .collect(),
            message_events: Default::default(),
        }
    }

    pub fn push_client(&mut self, workload: W, num_replica: usize, num_faulty: usize) {
        let addrs = (0..num_replica)
            .map(|i| Addr::Replica(i as _))
            .collect::<Vec<_>>();
        let index = self.clients.len();
        self.clients.push(ClientState {
            state: Client::new(
                index as u32 + 1000,
                Addr::Client(index as _),
                IndexNet::new(Transient::default(), addrs, None),
                Transient::default(),
                num_replica,
                num_faulty,
            ),
            timer: Timer::default(),
            close_loop: CloseLoop::new(Transient::default(), workload),
        });
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    Message(MessageEvent),
    Timer(TimerEvent),
}

impl<W: Clone + Workload, const CHECK: bool> crate::search::State for State<W, CHECK>
where
    W::Attach: Clone,
{
    type Event = Event;

    fn events(&self) -> Vec<Self::Event> {
        let events = self
            .message_events
            .iter()
            .cloned()
            .map(Event::Message)
            .chain(self.clients.iter().enumerate().flat_map(|(index, client)| {
                client.timer.events().into_iter().map(move |timer_id| {
                    Event::Timer(TimerEvent {
                        timer_id,
                        addr: Addr::Client(index as _),
                    })
                })
            }))
            .chain(
                self.replicas
                    .iter()
                    .enumerate()
                    .flat_map(|(index, replica)| {
                        replica.timer.events().into_iter().map(move |timer_id| {
                            Event::Timer(TimerEvent {
                                timer_id,
                                addr: Addr::Replica(index as _),
                            })
                        })
                    }),
            );
        if CHECK {
            events.collect()
        } else {
            events.take(1).collect()
        }
    }

    fn step(&mut self, event: Self::Event) -> anyhow::Result<()> {
        // match event {
        //     Event::Message(event) => {
        //         if CHECK {
        //             self.message_events.remove(&event); // assert exist
        //         }
        //     }
        //     Event::Timer(TimerEvent {
        //         addr: Addr::Client(index),
        //         timer_id,
        //     }) => {
        //         let client = &mut self.clients[index as usize];
        //         // client.state
        //     }
        // }
        Ok(())
    }
}
