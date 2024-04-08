use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    mem::{replace, take},
};

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
    event::{
        any::{self, TryIntoTimerData},
        erased::{events::Init, OnEvent as _, OnEventRichTimer as _},
        linear, SendEvent, TimerId, Transient, UnreachableTimer,
    },
    net::{events::Recv, IndexNet, IterAddr, SendMessage},
    worker::Worker,
    workload::{CloseLoop, Invoke, InvokeOk, Workload},
};

use super::{Commit, CryptoWorker, PrePrepare, Prepare, Reply, Request, Resend};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum Addr {
    Client(u8),
    Replica(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From)]
enum Message {
    Request(Request<Addr>),
    PrePrepare(Verifiable<PrePrepare>, Vec<Request<Addr>>),
    Prepare(Verifiable<Prepare>),
    Commit(Verifiable<Commit>),
    Reply(Reply),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MessageEvent {
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

impl<M: Into<Message> + Clone> SendMessage<IterAddr<'_, Addr>, M> for Transient<MessageEvent> {
    fn send(&mut self, dest: IterAddr<'_, Addr>, message: M) -> anyhow::Result<()> {
        // special bookkeeping if useful
        for addr in dest.0 {
            SendMessage::send(self, addr, message.clone())?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TimerEvent {
    addr: Addr,
    timer_id: TimerId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TimerData {
    Resend,
    // replica timers
}

struct IntoTimerData;

impl TryIntoTimerData<TimerData> for IntoTimerData {
    fn try_into<M: 'static>(event: M) -> anyhow::Result<TimerData> {
        if (&event as &dyn Any).is::<Resend>() {
            Ok(TimerData::Resend)
        } else {
            Err(anyhow::anyhow!("unknown timer data"))
        }
    }
}

#[derive(Debug, Clone, derive_more::From)]
enum CryptoEvent {
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
    CryptoWorker<Worker<Crypto, Transient<CryptoEvent>>, Transient<CryptoEvent>>,
    KVStore,
    Addr,
>;

#[derive_where(PartialEq, Eq, Hash; W::Attach)]
#[derive_where(Debug, Clone; W, W::Attach)]
struct State<W: Workload, const CHECK: bool = false> {
    pub clients: Vec<ClientState<W>>,
    pub replicas: Vec<Replica>,
    message_events: BTreeSet<MessageEvent>,
    timers: BTreeMap<Addr, any::Timer<linear::Timer, IntoTimerData, TimerData>>,
}

#[derive_where(Debug, Clone; W, W::Attach)]
#[derive_where(PartialEq, Eq, Hash; W::Attach)]
struct ClientState<W: Workload> {
    pub state: Client,
    pub close_loop: CloseLoop<W, Transient<Invoke>>,
}

impl<W: Workload, const CHECK: bool> State<W, CHECK> {
    fn new(num_replica: usize, num_faulty: usize) -> Self {
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
                        CryptoWorker::from(Worker::Inline(
                            Crypto::new_hardcoded_replication(num_replica, i, Plain)
                                .expect("plain crypto always can be created"),
                            Transient::default(),
                        )),
                        num_replica,
                        num_faulty,
                    )
                })
                .collect(),
            timers: (0..num_replica)
                .map(|i| (Addr::Replica(i as _), Default::default()))
                .collect(),
            message_events: Default::default(),
        }
    }

    fn push_client(&mut self, workload: W, num_replica: usize, num_faulty: usize) {
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
            close_loop: CloseLoop::new(Transient::default(), workload),
        });
        self.timers
            .insert(Addr::Client(index as _), Default::default());
    }
}

#[derive(Debug, Clone)]
enum Event {
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
            .chain(self.timers.iter().flat_map(|(addr, timer)| {
                timer.events().into_iter().map(move |timer_id| {
                    Event::Timer(TimerEvent {
                        timer_id,
                        addr: *addr,
                    })
                })
            }));
        if CHECK {
            events.collect()
        } else {
            events.take(1).collect()
        }
    }

    fn step(&mut self, event: Self::Event) -> anyhow::Result<()> {
        match event {
            Event::Message(event) => {
                if !CHECK {
                    self.message_events.remove(&event); // assert exist
                }
                let timer = self
                    .timers
                    .get_mut(&event.dest)
                    .ok_or(anyhow::anyhow!("missing timer for {:?}", event.dest))?;
                match (event.dest, event.message) {
                    (Addr::Client(index), Message::Reply(reply)) => self.clients[index as usize]
                        .state
                        .on_event(Recv(reply), timer)?,
                    (Addr::Replica(index), message) => {
                        let replica = &mut self.replicas[index as usize];
                        match message {
                            Message::Request(request) => replica.on_event(Recv(request), timer)?,
                            Message::PrePrepare(pre_prepare, requests) => {
                                replica.on_event(Recv((pre_prepare, requests)), timer)?
                            }
                            Message::Prepare(prepare) => replica.on_event(Recv(prepare), timer)?,
                            Message::Commit(commit) => replica.on_event(Recv(commit), timer)?,
                            _ => anyhow::bail!("unimplemented"),
                        }
                    }
                    _ => anyhow::bail!("unimplemented"),
                }
            }
            Event::Timer(TimerEvent { addr, timer_id }) => {
                let timer = self
                    .timers
                    .get_mut(&addr)
                    .ok_or(anyhow::anyhow!("missing timer for {addr:?}"))?;
                let timer_data = timer.get_timer_data(&timer_id)?.clone();
                timer.step_timer(&timer_id)?;
                match (addr, timer_data) {
                    (Addr::Client(index), TimerData::Resend) => {
                        self.clients[index as usize].state.on_event(Resend, timer)?
                    }
                    _ => anyhow::bail!("unimplemented"),
                }
            }
        }
        self.flush()
    }
}

impl<W: Workload, const CHECK: bool> State<W, CHECK> {
    fn launch(&mut self) -> anyhow::Result<()> {
        for client in &mut self.clients {
            client.close_loop.on_event(Init, &mut UnreachableTimer)?
        }
        self.flush()
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let mut rerun = true;
        while replace(&mut rerun, false) {
            for (index, replica) in self.replicas.iter_mut().enumerate() {
                self.message_events.extend(replica.net.drain(..));
                self.message_events.extend(replica.client_net.drain(..));
                let Worker::Inline(_, events) = &mut replica.crypto_worker.0 else {
                    unreachable!()
                };
                let timer = self
                    .timers
                    .get_mut(&Addr::Replica(index as _))
                    .ok_or(anyhow::anyhow!("missing timer for replica {index}"))?;
                for event in take(&mut **events) {
                    rerun = true;
                    match event {
                        CryptoEvent::SignedPrePrepare(pre_prepare, requests) => {
                            replica.on_event((pre_prepare, requests), timer)?
                        }
                        CryptoEvent::VerifiedPrePrepare(pre_prepare, requests) => {
                            replica.on_event((pre_prepare, requests), timer)?
                        }
                        CryptoEvent::SignedPrepare(prepare) => replica.on_event(prepare, timer)?,
                        CryptoEvent::VerifiedPrepare(prepare) => {
                            replica.on_event(prepare, timer)?
                        }
                        CryptoEvent::SignedCommit(commit) => replica.on_event(commit, timer)?,
                        CryptoEvent::VerifiedCommit(commit) => replica.on_event(commit, timer)?,
                    }
                }
            }
            for (index, client) in self.clients.iter_mut().enumerate() {
                self.message_events.extend(client.state.net.drain(..));
                let timer = self
                    .timers
                    .get_mut(&Addr::Client(index as _))
                    .ok_or(anyhow::anyhow!("missing timer for client {index}"))?;
                for invoke in client.close_loop.sender.drain(..) {
                    rerun = true;
                    client.state.on_event(invoke, timer)?
                }
                for upcall in client.state.upcall.drain(..) {
                    rerun = true;
                    client.close_loop.on_event(upcall, &mut UnreachableTimer)?
                }
            }
        }
        Ok(())
    }
}

#[test]
fn no_client() -> anyhow::Result<()> {
    let num_replica = 4;
    let num_faulty = 1;

    struct Idle;
    impl Workload for Idle {
        type Attach = ();

        fn next_op(&mut self) -> anyhow::Result<Option<(crate::util::Payload, Self::Attach)>> {
            Ok(None)
        }

        fn on_result(&mut self, _: crate::util::Payload, _: Self::Attach) -> anyhow::Result<()> {
            Ok(())
        }
    }

    let mut state = State::<Idle>::new(num_replica, num_faulty);
    state.launch()
}
