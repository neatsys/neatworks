use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    iter::Empty,
    mem::{replace, take},
    time::Duration,
};

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    app::{
        kvstore::{
            Op::{Get, Put},
            Result::{GetResult, PutOk},
        },
        KVStore,
    },
    crypto::{
        events::{Signed, Verified},
        Crypto,
        CryptoFlavor::Plain,
    },
    event::{
        downcast::{self, DowncastEvent},
        erased::{events::Init, OnEvent as _, OnEventRichTimer, RichTimer},
        linear, SendEvent, TimerId, Transient, UnreachableTimer,
    },
    net::{events::Recv, IndexNet, IterAddr, Payload, SendMessage},
    pbft::CLIENT_RESEND_INTERVAL,
    worker::Worker,
    workload::{
        events::{Invoke, InvokeOk},
        Check, CloseLoop, Iter, Json, Workload,
    },
};

use super::{
    Commit, CryptoWorker, DoViewChange, NewView, PrePrepare, Prepare, ProgressPrepared,
    ProgressViewChange, Reply, Request, Resend, ToReplica, ViewChange,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum Addr {
    Client(u8),
    Replica(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::From)]
enum Message {
    #[from(forward)]
    ToReplica(ToReplica<Addr>),
    #[from]
    ToClient(Reply),
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
    at: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TimerData {
    Resend,
    ProgressPrepared(ProgressPrepared),
    DoViewChange(DoViewChange),
    ProgressViewChange(ProgressViewChange),
}

impl DowncastEvent for TimerData {
    fn try_from<M: 'static>(event: M) -> anyhow::Result<Self> {
        fn downcast_or<T: 'static>(
            wrap: impl Fn(T) -> TimerData,
            default: impl Fn(Box<dyn Any>) -> anyhow::Result<TimerData>,
        ) -> impl Fn(Box<dyn Any>) -> anyhow::Result<TimerData> {
            move |event| match event.downcast::<T>() {
                Ok(event) => Ok(wrap(*event)),
                Err(event) => default(event),
            }
        }

        let downcast = |_| Err(anyhow::format_err!("unknown timer data"));
        let downcast = downcast_or(|_: Resend| TimerData::Resend, downcast);
        let downcast = downcast_or(TimerData::DoViewChange, downcast);
        let downcast = downcast_or(TimerData::ProgressPrepared, downcast);
        let downcast = downcast_or(TimerData::ProgressViewChange, downcast);
        downcast(Box::new(event))
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
    SignedViewChange(Signed<ViewChange>),
    VerifiedViewChange(Verified<ViewChange>),
    SignedNewView(Signed<NewView>),
    VerifiedNewView(Verified<NewView>),
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
    fn deliver_timer(&self, source: Addr, event: &linear::TimerEvent) -> bool;
}

#[derive(Debug, Clone)]
struct AllowAll;

impl Filter for AllowAll {
    fn deliver_message(&self, _: Addr, _: &MessageEvent) -> bool {
        true
    }

    fn deliver_timer(&self, _: Addr, _: &linear::TimerEvent) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct WithPartition<F>(F, Vec<Addr>);

impl<F: Filter> Filter for WithPartition<F> {
    fn deliver_message(&self, source: Addr, event: &MessageEvent) -> bool {
        self.1.contains(&source)
            && self.1.contains(&event.dest)
            && self.0.deliver_message(source, event)
    }

    fn deliver_timer(&self, source: Addr, event: &linear::TimerEvent) -> bool {
        self.0.deliver_timer(source, event)
    }
}

#[derive(Debug, Clone)]
struct Deadline<F>(F, Duration);

impl<F: Filter> Filter for Deadline<F> {
    fn deliver_message(&self, source: Addr, event: &MessageEvent) -> bool {
        self.0.deliver_message(source, event)
    }

    fn deliver_timer(&self, source: Addr, event: &linear::TimerEvent) -> bool {
        event.at() < self.1 && self.0.deliver_timer(source, event)
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

    fn map_filter<G>(self, f: impl FnOnce(F) -> G) -> State<W, G, CHECK> {
        State {
            clients: self.clients,
            replicas: self.replicas,
            message_events: self.message_events,
            timers: self.timers,
            filter: f(self.filter),
        }
    }
}

#[derive(Debug, Clone)]
enum Event {
    Message(MessageEvent),
    Timer(TimerEvent),
}

impl<W: Workload, F: Filter, const CHECK: bool> State<W, F, CHECK> {
    fn timer_events(&self) -> impl Iterator<Item = TimerEvent> + '_ {
        self.timers.iter().flat_map(|(addr, timer)| {
            timer
                .events()
                .filter(|event| self.filter.deliver_timer(*addr, event))
                .map(|event| TimerEvent {
                    addr: *addr,
                    timer_id: event.id(),
                    at: if CHECK { Duration::ZERO } else { event.at() },
                })
        })
    }
}

impl<W: Workload<Op = Payload, Result = Payload>, F: Filter + Clone, const CHECK: bool>
    crate::search::State for State<W, F, CHECK>
{
    type Event = Event;

    fn events(&self) -> Vec<Self::Event> {
        let message_events = self.message_events.iter().cloned().map(Event::Message);
        if CHECK {
            message_events
                .chain(self.timer_events().map(Event::Timer))
                .collect()
        } else {
            message_events
                .chain(
                    self.timer_events()
                        .min_by_key(|event| event.at)
                        .map(Event::Timer),
                )
                .take(1)
                .collect()
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
                    .ok_or(anyhow::format_err!("missing timer for {:?}", event.dest))?;
                match (event.dest, event.message) {
                    (Addr::Client(index), Message::ToClient(message)) => self.clients
                        [index as usize]
                        .state
                        .on_event(Recv(message), timer)?,
                    (Addr::Replica(index), Message::ToReplica(message)) => {
                        struct Inline<'a, S, T>(&'a mut S, &'a mut T);
                        impl<S: OnEventRichTimer<M>, M, T: RichTimer<S>> SendEvent<M> for Inline<'_, S, T> {
                            fn send(&mut self, event: M) -> anyhow::Result<()> {
                                self.0.on_event(event, self.1)
                            }
                        }
                        message.send(&mut Inline(&mut self.replicas[index as usize], timer))?
                    }
                    _ => anyhow::bail!("unimplemented"),
                }
            }
            Event::Timer(TimerEvent {
                addr,
                timer_id,
                at: now,
            }) => {
                if !CHECK {
                    for timer in self.timers.values_mut() {
                        timer.advance_to(now)?
                    }
                }
                let timer = self
                    .timers
                    .get_mut(&addr)
                    .ok_or(anyhow::format_err!("missing timer for {addr:?}"))?;
                let timer_data = timer.get_timer_data(&timer_id)?.clone();
                timer.step_timer(&timer_id)?;
                match (addr, timer_data) {
                    (Addr::Client(index), TimerData::Resend) => {
                        self.clients[index as usize].state.on_event(Resend, timer)?
                    }
                    (Addr::Replica(index), event) => {
                        let replica = &mut self.replicas[index as usize];
                        match event {
                            TimerData::ProgressPrepared(event) => replica.on_event(event, timer)?,
                            TimerData::DoViewChange(event) => replica.on_event(event, timer)?,
                            TimerData::ProgressViewChange(event) => {
                                replica.on_event(event, timer)?
                            }
                            _ => anyhow::bail!("unimplemented"),
                        }
                    }
                    _ => anyhow::bail!("unimplemented"),
                }
            }
        }
        self.flush()
    }
}

impl<W: Workload<Op = Payload, Result = Payload>, F: Filter, const CHECK: bool> State<W, F, CHECK> {
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
                let Worker::Inline(_, events) = &mut replica.crypto_worker.0 else {
                    unreachable!()
                };
                let timer = self
                    .timers
                    .get_mut(&addr)
                    .ok_or(anyhow::format_err!("missing timer for replica {index}"))?;
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
                        CryptoEvent::SignedViewChange(view_change) => {
                            replica.on_event(view_change, timer)?
                        }
                        CryptoEvent::VerifiedViewChange(view_change) => {
                            replica.on_event(view_change, timer)?
                        }
                        CryptoEvent::SignedNewView(new_view) => {
                            replica.on_event(new_view, timer)?
                        }
                        CryptoEvent::VerifiedNewView(new_view) => {
                            replica.on_event(new_view, timer)?
                        }
                    }
                }
                self.message_events.extend(
                    replica
                        .net
                        .drain(..)
                        .filter(|event| self.filter.deliver_message(addr, event)),
                );
                self.message_events.extend(replica.client_net.drain(..))
            }
            for (index, client) in self.clients.iter_mut().enumerate() {
                let addr = Addr::Client(index as _);
                let timer = self
                    .timers
                    .get_mut(&addr)
                    .ok_or(anyhow::format_err!("missing timer for client {index}"))?;
                for invoke in client.close_loop.sender.drain(..) {
                    rerun = true;
                    client.state.on_event(invoke, timer)?
                }
                for upcall in client.state.upcall.drain(..) {
                    rerun = true;
                    client.close_loop.on_event(upcall, &mut UnreachableTimer)?
                }
                self.message_events.extend(
                    client
                        .state
                        .net
                        .drain(..)
                        .filter(|event| self.filter.deliver_message(addr, event)),
                )
            }
        }
        // TODO assert crypto
        assert!(self
            .replicas
            .iter()
            .all(|replica| replica.net.is_empty() && replica.client_net.is_empty()));
        assert!(self.clients.iter().all(|client| client.state.net.is_empty()
            && client.state.upcall.is_empty()
            && client.close_loop.sender.is_empty()));
        Ok(())
    }
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub struct ProgressExhausted;

pub trait SimulateState: crate::search::State {
    fn step_simulate(&mut self) -> anyhow::Result<()> {
        let Some(event) = self.events().pop() else {
            anyhow::bail!(ProgressExhausted)
        };
        self.step(event)
    }
}

impl<W: Workload<Op = Payload, Result = Payload> + Clone, F: Filter + Clone> SimulateState
    for State<W, F, false>
where
    W::Attach: Clone,
{
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
    let workload = Json(Check::new([(op.clone(), PutOk)].into_iter()));
    state.push_client(workload, num_replica, num_faulty);
    state.launch()?;

    while !state.clients.iter().all(|client| client.close_loop.done) {
        // use crate::search::State;
        // println!("{:?}", state.events());
        state.step_simulate()?
    }

    let mut num_prepared = 0;
    for replica in &state.replicas {
        let Some(entry) = replica.log.get(1) else {
            continue;
        };
        num_prepared += 1;
        anyhow::ensure!(entry.requests.first().unwrap().op == Payload(serde_json::to_vec(&op)?))
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
        let workload = Json(Check::new([(op, result)].into_iter()));
        let index = state.push_client(workload, num_replica, num_faulty);
        state.launch_client(index)?;

        while !state.clients[index].close_loop.done {
            state.step_simulate()?
        }
    }

    Ok(())
}

#[test]
fn t04_progress_in_majority() -> anyhow::Result<()> {
    let num_replica = 7;
    let num_faulty = 2;
    let mut state = State::<_, _>::new(num_replica, num_faulty).map_filter(|filter| {
        WithPartition(
            filter,
            vec![
                Addr::Client(0),
                Addr::Replica(0),
                Addr::Replica(1),
                Addr::Replica(2),
                Addr::Replica(3),
                Addr::Replica(4),
            ],
        )
    });
    let workload = Json(Check::new(
        [(Put("hello".into(), "world".into()), PutOk)].into_iter(),
    ));
    state.push_client(workload, num_replica, num_faulty);
    state.launch()?;

    while !state.clients.iter().all(|client| client.close_loop.done) {
        state.step_simulate()?
    }

    anyhow::ensure!(state.replicas[5].log.get(1).is_none());
    anyhow::ensure!(state.replicas[6].log.get(1).is_none());
    Ok(())
}

#[test]
fn t05_no_progress_in_minority() -> anyhow::Result<()> {
    let num_replica = 7;
    let num_faulty = 2;
    let mut state = State::<_, _>::new(num_replica, num_faulty)
        .map_filter(|filter| {
            WithPartition(
                filter,
                vec![
                    Addr::Client(0),
                    Addr::Replica(0),
                    Addr::Replica(1),
                    Addr::Replica(2),
                    Addr::Replica(3),
                ],
            )
        })
        .map_filter(|filter| Deadline(filter, CLIENT_RESEND_INTERVAL * 20));
    let workload = Json(Check::new(
        [(Put("hello".into(), "world".into()), PutOk)].into_iter(),
    ));
    state.push_client(workload, num_replica, num_faulty);
    state.launch()?;

    loop {
        match state.step_simulate() {
            Ok(()) => {}
            Err(err) if err.is::<ProgressExhausted>() => break,
            err => err?,
        }
    }
    anyhow::ensure!(!state.clients.iter().any(|client| client.close_loop.done));
    anyhow::ensure!(state.replicas.iter().all(|replica| replica.commit_num == 0));
    Ok(())
}

#[test]
fn t06_progress_after_heal() -> anyhow::Result<()> {
    let num_replica = 7;
    let num_faulty = 2;

    let mut state = State::<_, _>::new(num_replica, num_faulty)
        .map_filter(|filter| {
            WithPartition(
                filter,
                vec![
                    Addr::Client(0),
                    Addr::Replica(0),
                    Addr::Replica(1),
                    Addr::Replica(2),
                    Addr::Replica(3),
                ],
            )
        })
        .map_filter(|filter| Deadline(filter, CLIENT_RESEND_INTERVAL * 20));
    let workload = Json(Check::new(
        [(Put("hello".into(), "world".into()), PutOk)].into_iter(),
    ));
    let index = state.push_client(workload, num_replica, num_faulty);
    state.launch_client(index)?;

    loop {
        match state.step_simulate() {
            Ok(()) => {}
            Err(err) if err.is::<ProgressExhausted>() => break,
            err => err?,
        }
    }

    let mut state = state.map_filter(|_| AllowAll);
    while !state.clients[index].close_loop.done {
        use crate::search::State;
        println!("{:?}", state.events());
        state.step_simulate()?
    }

    let workload = Json(Check::new(
        [(Get("hello".into()), GetResult("world".into()))].into_iter(),
    ));
    let index = state.push_client(workload, num_replica, num_faulty);
    state.launch_client(index)?;
    while !state.clients[index].close_loop.done {
        state.step_simulate()?
    }

    Ok(())
}

#[test]
fn t07_server_switches_partitions() -> anyhow::Result<()> {
    let num_replica = 7;
    let num_faulty = 2;
    let mut state = State::<_, _>::new(num_replica, num_faulty);
    let workload = Json(Check::new(
        [(Put("hello".into(), "world".into()), PutOk)].into_iter(),
    ));
    let index = state.push_client(workload, num_replica, num_faulty);
    let mut state = state.map_filter(|filter| {
        WithPartition(
            filter,
            vec![
                Addr::Client(index as _),
                Addr::Replica(0),
                Addr::Replica(1),
                Addr::Replica(2),
                Addr::Replica(3),
                Addr::Replica(4),
            ],
        )
    });
    state.launch_client(index)?;

    while !state.clients[index].close_loop.done {
        use crate::search::State;
        println!("{:?}", state.events());
        state.step_simulate()?
    }

    let workload = Json(Check::new(
        [(Get("hello".into()), GetResult("world".into()))].into_iter(),
    ));
    let index = state.push_client(workload, num_replica, num_faulty);
    let mut state = state.map_filter(|_| AllowAll).map_filter(|filter| {
        WithPartition(
            filter,
            vec![
                Addr::Client(index as _),
                Addr::Replica(2),
                Addr::Replica(3),
                Addr::Replica(4),
                Addr::Replica(5),
                Addr::Replica(6),
            ],
        )
    });
    state.launch_client(index)?;

    while !state.clients[index].close_loop.done {
        use crate::search::State;
        println!("{:?}", state.events());
        state.step_simulate()?
    }

    Ok(())
}

// cSpell:words upcall kvstore pbft
