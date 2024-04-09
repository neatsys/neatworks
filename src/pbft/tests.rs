use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    iter::Empty,
    mem::{replace, take},
};

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    app::{
        kvstore::{
            static_workload,
            Op::{Get, Put},
            Result::{GetResult, PutOk},
        },
        KVStore,
    },
    crypto::{
        events::{Signed, Verified},
        Crypto,
        CryptoFlavor::Plain,
        Verifiable,
    },
    event::{
        downcast::{self, DowncastEvent},
        erased::{events::Init, OnEvent as _, OnEventRichTimer as _},
        linear, SendEvent, TimerId, Transient, UnreachableTimer,
    },
    net::{events::Recv, IndexNet, IterAddr, SendMessage},
    search::State as _,
    util::Payload,
    worker::Worker,
    workload::{CloseLoop, Invoke, InvokeOk, Iter, Workload},
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

impl DowncastEvent for TimerData {
    fn try_from<M: 'static>(event: M) -> anyhow::Result<Self> {
        if (&event as &dyn Any).is::<Resend>() {
            Ok(Self::Resend)
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
#[derive_where(Debug, Clone; W, W::Attach, F)]
struct State<W: Workload, F, const CHECK: bool = false> {
    pub clients: Vec<ClientState<W>>,
    pub replicas: Vec<Replica>,
    message_events: BTreeSet<MessageEvent>,
    timers: BTreeMap<Addr, downcast::Timer<linear::Timer, TimerData>>,
    #[derive_where(skip(EqHashOrd))]
    filter: F,
}

#[derive_where(Debug, Clone; W, W::Attach)]
#[derive_where(PartialEq, Eq, Hash; W::Attach)]
struct ClientState<W: Workload> {
    pub state: Client,
    pub close_loop: CloseLoop<W, Transient<Invoke>>,
}

trait Filter {
    fn deliver_message(&self, source: Addr, event: &MessageEvent) -> bool;
    // TODO deliver timer
}

#[derive(Debug, Clone)]
struct AllowAll;

impl Filter for AllowAll {
    fn deliver_message(&self, _: Addr, _: &MessageEvent) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct Partition(Vec<Addr>);

impl Filter for Partition {
    fn deliver_message(&self, source: Addr, event: &MessageEvent) -> bool {
        self.0.contains(&source) && self.0.contains(&event.dest)
    }
}

impl<W: Workload, const CHECK: bool> State<W, AllowAll, CHECK> {
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
            filter: AllowAll,
        }
    }
}

impl<W: Workload, F, const CHECK: bool> State<W, F, CHECK> {
    fn push_client(&mut self, workload: W, num_replica: usize, num_faulty: usize) -> usize {
        let addrs = (0..num_replica)
            .map(|i| Addr::Replica(i as _))
            .collect::<Vec<_>>();
        let index = self.clients.len();
        let addr = Addr::Client(index as _);
        self.clients.push(ClientState {
            state: Client::new(
                index as u32 + 1000,
                addr,
                IndexNet::new(Transient::default(), addrs, None),
                Transient::default(),
                num_replica,
                num_faulty,
            ),
            close_loop: CloseLoop::new(Transient::default(), workload),
        });
        self.timers.insert(addr, Default::default());
        index
    }

    fn with_filter<G>(self, filter: G) -> State<W, G, CHECK> {
        State {
            clients: self.clients,
            replicas: self.replicas,
            message_events: self.message_events,
            timers: self.timers,
            filter,
        }
    }
}

#[derive(Debug, Clone)]
enum Event {
    Message(MessageEvent),
    Timer(TimerEvent),
}

impl<W: Clone + Workload, F: Filter + Clone, const CHECK: bool> crate::search::State
    for State<W, F, CHECK>
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

impl<W: Workload, F: Filter, const CHECK: bool> State<W, F, CHECK> {
    fn launch(&mut self) -> anyhow::Result<()> {
        for client in &mut self.clients {
            client.close_loop.on_event(Init, &mut UnreachableTimer)?
        }
        self.flush()
    }

    fn launch_client(&mut self, index: usize) -> anyhow::Result<()> {
        self.clients[index]
            .close_loop
            .on_event(Init, &mut UnreachableTimer)?;
        self.flush()
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let mut rerun = true;
        while replace(&mut rerun, false) {
            for (index, replica) in self.replicas.iter_mut().enumerate() {
                let addr = Addr::Replica(index as _);
                self.message_events.extend(
                    replica
                        .net
                        .drain(..)
                        .filter(|event| self.filter.deliver_message(addr, event)),
                );
                self.message_events.extend(replica.client_net.drain(..));
                let Worker::Inline(_, events) = &mut replica.crypto_worker.0 else {
                    unreachable!()
                };
                let timer = self
                    .timers
                    .get_mut(&addr)
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
                let addr = Addr::Client(index as _);
                self.message_events.extend(
                    client
                        .state
                        .net
                        .drain(..)
                        .filter(|event| self.filter.deliver_message(addr, event)),
                );
                let timer = self
                    .timers
                    .get_mut(&addr)
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
    let mut state = State::<Iter<Empty<Payload>>, _>::new(num_replica, num_faulty);
    state.launch()
}

// the following test naming are based on the adapted DSLabs lab 3 test cases
// those are not expected to change, hopefully forever

#[test]
fn t02_basic() -> anyhow::Result<()> {
    let num_replica = 4;
    let num_faulty = 1;
    let mut state = State::<_, _>::new(num_replica, num_faulty);
    let op = Put("hello".into(), "world".into());
    let result = PutOk;
    let workload = static_workload([(op.clone(), result)].into_iter())?;
    state.push_client(workload, num_replica, num_faulty);
    state.launch()?;

    while !state.clients.iter().all(|client| client.close_loop.done) {
        let Some(event) = state.events().pop() else {
            anyhow::bail!("stuck")
        };
        state.step(event)?
    }

    let mut num_prepared = 0;
    for replica in &state.replicas {
        let Some(entry) = replica.log.get(1) else {
            continue;
        };
        num_prepared += 1;
        anyhow::ensure!(entry.requests.first().unwrap().op == Payload(serde_json::to_vec(&op)?));
    }
    anyhow::ensure!(num_prepared >= num_replica - num_faulty);

    Ok(())
}

#[test]
fn t03_no_partition() -> anyhow::Result<()> {
    let num_replica = 7;
    let num_faulty = 2;
    let mut state = State::<_, _>::new(num_replica, num_faulty);

    for (op, result) in [
        (Put("foo".into(), "bar".into()), PutOk),
        (Put("foo".into(), "baz".into()), PutOk),
        (Get("foo".into()), GetResult("baz".into())),
    ] {
        let workload = static_workload([(op.clone(), result)].into_iter())?;
        let index = state.push_client(workload, num_replica, num_faulty);
        state.launch_client(index)?;

        while !state.clients[index].close_loop.done {
            let Some(event) = state.events().pop() else {
                anyhow::bail!("stuck")
            };
            state.step(event)?
        }
    }

    Ok(())
}

#[test]
fn t04_progress_in_majority() -> anyhow::Result<()> {
    let num_replica = 7;
    let num_faulty = 2;
    let mut state = State::<_, _>::new(num_replica, num_faulty).with_filter(Partition(vec![
        Addr::Client(0),
        Addr::Replica(0),
        Addr::Replica(1),
        Addr::Replica(2),
        Addr::Replica(3),
        Addr::Replica(4),
    ]));
    let op = Put("hello".into(), "world".into());
    let result = PutOk;
    let workload = static_workload([(op.clone(), result)].into_iter())?;
    state.push_client(workload, num_replica, num_faulty);
    state.launch()?;

    while !state.clients.iter().all(|client| client.close_loop.done) {
        let Some(event) = state.events().pop() else {
            anyhow::bail!("stuck")
        };
        state.step(event)?
    }

    anyhow::ensure!(state.replicas[5].log.get(1).is_none());
    anyhow::ensure!(state.replicas[6].log.get(1).is_none());
    Ok(())
}

// cSpell:words upcall kvstore
