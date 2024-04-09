use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    crypto::{
        events::{Signed, Verified},
        Crypto, DigestHash as _, Verifiable, H256,
    },
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent, TimerId,
    },
    net::{deserialize, events::Recv, Addr, All, MessageNet, SendMessage},
    util::{Payload, Request},
    worker::{Submit, Work},
    workload::{Invoke, InvokeOk},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PrePrepare {
    view_num: u32,
    op_num: u32,
    digest: H256,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Prepare {
    view_num: u32,
    op_num: u32,
    digest: H256,
    replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Commit {
    view_num: u32,
    op_num: u32,
    digest: H256,
    replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Payload,
    view_num: u32,
    replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ViewChange {
    view_num: u32,
    log: Vec<(Verifiable<PrePrepare>, Quorum<Prepare>)>,
    replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NewView {
    view_num: u32,
    view_changes: Quorum<ViewChange>,
    pre_prepares: Vec<Verifiable<PrePrepare>>,
}

pub trait ToReplicaNet<A>:
    SendMessage<u8, Request<A>>
    + SendMessage<All, Request<A>>
    + SendMessage<All, (Verifiable<PrePrepare>, Vec<Request<A>>)>
    + SendMessage<All, Verifiable<Prepare>>
    + SendMessage<All, Verifiable<Commit>>
    + SendMessage<All, Verifiable<ViewChange>>
    + SendMessage<All, Verifiable<NewView>>
{
}
impl<
        T: SendMessage<u8, Request<A>>
            + SendMessage<All, Request<A>>
            + SendMessage<All, (Verifiable<PrePrepare>, Vec<Request<A>>)>
            + SendMessage<All, Verifiable<Prepare>>
            + SendMessage<All, Verifiable<Commit>>
            + SendMessage<All, Verifiable<ViewChange>>
            + SendMessage<All, Verifiable<NewView>>,
        A,
    > ToReplicaNet<A> for T
{
}

#[derive(Clone)]
#[derive_where(Debug, PartialEq, Eq, Hash; A)]
pub struct Client<N, U, A> {
    id: u32,
    addr: A,
    seq: u32,
    invoke: Option<ClientInvoke>,
    view_num: u32,
    num_replica: usize,
    num_faulty: usize,

    #[derive_where(skip)]
    net: N,
    #[derive_where(skip)]
    upcall: U,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClientInvoke {
    op: Payload,
    resend_timer: TimerId,
    replies: BTreeMap<u8, Reply>,
}

impl<N, U, A> Client<N, U, A> {
    pub fn new(id: u32, addr: A, net: N, upcall: U, num_replica: usize, num_faulty: usize) -> Self {
        Self {
            id,
            addr,
            net,
            upcall,
            num_replica,
            num_faulty,

            seq: 0,
            view_num: 0,
            invoke: Default::default(),
        }
    }
}

const CLIENT_RESEND_INTERVAL: Duration = Duration::from_millis(1000);

impl<N: ToReplicaNet<A>, U, A: Addr> OnEvent<Invoke> for Client<N, U, A> {
    fn on_event(&mut self, Invoke(op): Invoke, timer: &mut impl Timer<Self>) -> anyhow::Result<()> {
        anyhow::ensure!(self.invoke.is_none(), "concurrent invocation");
        self.seq += 1;
        let invoke = ClientInvoke {
            op,
            resend_timer: timer.set(CLIENT_RESEND_INTERVAL, Resend)?,
            replies: Default::default(),
        };
        self.invoke = Some(invoke);
        self.do_send((self.view_num as usize % self.num_replica) as u8)
    }
}

#[derive(Debug, Clone)]
struct Resend;

impl<N: ToReplicaNet<A>, U, A: Addr> OnEvent<Resend> for Client<N, U, A> {
    fn on_event(&mut self, Resend: Resend, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        println!("Resend timeout on seq {}", self.seq);
        self.do_send(All)
        // Ok(())
    }
}

impl<N, U: SendEvent<InvokeOk>, A> OnEvent<Recv<Reply>> for Client<N, U, A> {
    fn on_event(
        &mut self,
        Recv(reply): Recv<Reply>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if reply.seq != self.seq {
            return Ok(());
        }
        let Some(invoke) = self.invoke.as_mut() else {
            return Ok(());
        };
        invoke.replies.insert(reply.replica_id, reply.clone());
        if invoke
            .replies
            .values()
            .filter(|inserted_reply| inserted_reply.result == reply.result)
            .count()
            == self.num_faulty + 1
        {
            self.view_num = reply.view_num;
            let invoke = self.invoke.take().unwrap();
            timer.unset(invoke.resend_timer)?;
            self.upcall.send((self.id, reply.result))
        } else {
            Ok(())
        }
    }
}

impl<N, U, A: Addr> Client<N, U, A> {
    fn do_send<B>(&mut self, dest: B) -> anyhow::Result<()>
    where
        N: SendMessage<B, Request<A>>,
    {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self.invoke.as_ref().unwrap().op.clone(),
        };
        // either this or add `Send + Sync` in trait bound above. i choose this
        self.net.send(dest, request)
    }
}

pub trait ToClientNet<A>: SendMessage<A, Reply> {}
impl<T: SendMessage<A, Reply>, A> ToClientNet<A> for T {}

pub trait SendCryptoEvent<A>:
    SendEvent<(Signed<PrePrepare>, Vec<Request<A>>)>
    + SendEvent<(Verified<PrePrepare>, Vec<Request<A>>)>
    + SendEvent<Signed<Prepare>>
    + SendEvent<Verified<Prepare>>
    + SendEvent<Signed<Commit>>
    + SendEvent<Verified<Commit>>
    + SendEvent<Signed<ViewChange>>
    + SendEvent<Verified<ViewChange>>
    + SendEvent<Signed<NewView>>
    + SendEvent<Verified<NewView>>
{
}
impl<
        T: SendEvent<(Signed<PrePrepare>, Vec<Request<A>>)>
            + SendEvent<(Verified<PrePrepare>, Vec<Request<A>>)>
            + SendEvent<Signed<Prepare>>
            + SendEvent<Verified<Prepare>>
            + SendEvent<Signed<Commit>>
            + SendEvent<Verified<Commit>>
            + SendEvent<Signed<ViewChange>>
            + SendEvent<Verified<ViewChange>>
            + SendEvent<Signed<NewView>>
            + SendEvent<Verified<NewView>>,
        A,
    > SendCryptoEvent<A> for T
{
}

#[derive_where(Debug, Clone; W)]
pub struct CryptoWorker<W, E>(W, std::marker::PhantomData<E>);

impl<W, E> From<W> for CryptoWorker<W, E> {
    fn from(value: W) -> Self {
        Self(value, Default::default())
    }
}

impl<W: Submit<S, E>, S: 'static, E: SendCryptoEvent<A> + 'static, A: Addr>
    Submit<S, dyn SendCryptoEvent<A>> for CryptoWorker<W, E>
{
    fn submit(&mut self, work: Work<S, dyn SendCryptoEvent<A>>) -> anyhow::Result<()> {
        self.0
            .submit(Box::new(move |state, emit| work(state, emit)))
    }
}

type Quorum<M> = BTreeMap<u8, Verifiable<M>>;
type Quorums<K, M> = BTreeMap<K, Quorum<M>>;

#[derive(Clone)]
#[derive_where(Debug, PartialEq, Eq, Hash; S, A)]
pub struct Replica<N, CN, CW, S, A, M = (N, CN, CW, S, A)> {
    id: u8,
    num_replica: usize,
    num_faulty: usize,

    replies: BTreeMap<u32, (u32, Option<Reply>)>,
    requests: Vec<Request<A>>,
    view_num: u32,
    op_num: u32,
    log: Vec<LogEntry<A>>,
    prepare_quorums: Quorums<u32, Prepare>, // `K` = op number
    commit_quorums: Quorums<u32, Commit>,
    commit_num: u32,
    app: S,

    do_view_change_timer: Option<TimerId>,
    view_changes: Quorums<u32, ViewChange>, // `K` = view number

    // any op num presents in this maps -> there's ongoing verification submitted
    // entry presents but empty list -> no pending but one is verifying
    // no entry present -> no pending and not verifying
    pending_prepares: BTreeMap<u32, Vec<Verifiable<Prepare>>>,
    pending_commits: BTreeMap<u32, Vec<Verifiable<Commit>>>,

    #[derive_where(skip)]
    net: N,
    #[derive_where(skip)]
    client_net: CN, // C for client
    #[derive_where(skip)]
    crypto_worker: CW, // C for crypto

    _m: std::marker::PhantomData<M>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[derive_where(Default)]
struct LogEntry<A> {
    pre_prepare: Option<Verifiable<PrePrepare>>,
    requests: Vec<Request<A>>,
    prepares: Quorum<Prepare>,
    commits: Quorum<Commit>,
    timer_id: Option<TimerId>,
}

impl<N, CN, CW, S, A> Replica<N, CN, CW, S, A> {
    pub fn new(
        id: u8,
        app: S,
        net: N,
        client_net: CN,
        crypto_worker: CW,
        num_replica: usize,
        num_faulty: usize,
    ) -> Self {
        Self {
            id,
            app,
            net,
            client_net,
            crypto_worker,
            num_replica,
            num_faulty,

            replies: Default::default(),
            requests: Default::default(),
            view_num: 0,
            op_num: 0,
            log: Default::default(),
            prepare_quorums: Default::default(),
            commit_quorums: Default::default(),
            commit_num: 0,
            do_view_change_timer: Default::default(),
            view_changes: Default::default(),
            pending_prepares: Default::default(),
            pending_commits: Default::default(),

            _m: Default::default(),
        }
    }
}

impl<N, CN, CW, S, A, M> Replica<N, CN, CW, S, A, M> {
    fn is_primary(&self) -> bool {
        (self.view_num as usize % self.num_replica) == self.id as usize
    }

    fn view_change(&self) -> bool {
        self.view_num != 0
            && self
                .view_changes
                .get(&self.view_num)
                .map(|quorum| quorum.len() < self.num_replica - self.num_faulty)
                .unwrap_or(true)
    }

    const NUM_CONCURRENT_PRE_PREPARE: u32 = 1;
}

pub trait ReplicaCommon {
    type N: ToReplicaNet<Self::A>;
    type CN: ToClientNet<Self::A>;
    type CW: Submit<Crypto, dyn SendCryptoEvent<Self::A>>;
    type S: App;
    type A: Addr;
}
impl<N, CN, CW, S, A> ReplicaCommon for (N, CN, CW, S, A)
where
    N: ToReplicaNet<A>,
    CN: ToClientNet<A>,
    CW: Submit<Crypto, dyn SendCryptoEvent<A>>,
    S: App,
    A: Addr,
{
    type N = N;
    type CN = CN;
    type CW = CW;
    type S = S;
    type A = A;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DoViewChange(u32); // view number to enter

impl<M: ReplicaCommon> OnEvent<Recv<Request<M::A>>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Recv(request): Recv<Request<M::A>>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if self.view_change() {
            return Ok(());
        }
        match self.replies.get(&request.client_id) {
            Some((seq, _)) if *seq > request.seq => return Ok(()),
            Some((seq, reply)) if *seq == request.seq => {
                if let Some(reply) = reply {
                    self.client_net.send(request.client_addr, reply.clone())?
                }
                return Ok(());
            }
            _ => {}
        }
        if !self.is_primary() {
            self.net
                .send((self.view_num as usize % self.num_replica) as u8, request)?;
            if self.do_view_change_timer.is_none() {
                // i don't know what's a good delay to begin; tentatively choose this to
                // hopefully resolve the liveness issue before next client resending, while
                // leave enough time for primary to continue trying
                self.do_view_change_timer =
                    Some(timer.set(CLIENT_RESEND_INTERVAL / 2, DoViewChange(self.view_num + 1))?)
            }
            return Ok(());
        }
        self.replies.insert(request.client_id, (request.seq, None));
        self.requests.push(request);
        if self.op_num < self.commit_num + Self::NUM_CONCURRENT_PRE_PREPARE {
            self.close_batch()
        } else {
            Ok(())
        }
    }
}

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn close_batch(&mut self) -> anyhow::Result<()> {
        assert!(self.is_primary());
        assert!(!self.requests.is_empty());
        self.op_num += 1;
        let requests = self
            .requests
            .drain(..self.requests.len().min(100))
            .collect::<Vec<_>>();
        let view_num = self.view_num;
        let op_num = self.op_num;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            let pre_prepare = PrePrepare {
                view_num,
                op_num,
                digest: requests.sha256(),
            };
            sender.send((Signed(crypto.sign(pre_prepare)), requests))
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Progress(u32); // op number

impl<M: ReplicaCommon> OnEvent<(Signed<PrePrepare>, Vec<Request<M::A>>)>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        (Signed(pre_prepare), requests): (Signed<PrePrepare>, Vec<Request<M::A>>),
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num {
            return Ok(());
        }

        let op_num = pre_prepare.op_num as usize;
        if self.log.get(op_num).is_none() {
            self.log.resize_with(op_num + 1, Default::default)
        }
        let replaced = self.log[op_num].pre_prepare.replace(pre_prepare.clone());
        assert!(replaced.is_none());

        self.log[op_num].requests.clone_from(&requests);
        let timer_id = timer.set(CLIENT_RESEND_INTERVAL / 5, Progress(op_num as _))?;
        self.log[op_num].timer_id = Some(timer_id);

        self.net.send(All, (pre_prepare, requests))
    }
}

impl<M: ReplicaCommon> OnEvent<Progress> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Progress(op_num): Progress,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let entry = &self.log[op_num as usize];
        self.net.send(
            All,
            (
                entry
                    .pre_prepare
                    .clone()
                    .ok_or(anyhow::anyhow!("missing PrePrepare"))?,
                entry.requests.clone(),
            ),
        )
    }
}

impl<M: ReplicaCommon> OnEvent<Recv<(Verifiable<PrePrepare>, Vec<Request<M::A>>)>>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        Recv((pre_prepare, requests)): Recv<(Verifiable<PrePrepare>, Vec<Request<M::A>>)>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num || self.view_change() {
            if pre_prepare.view_num > self.view_num {
                todo!("state transfer to enter view")
            }
            return Ok(());
        }
        // this was for eliminating duplicated verification on prepared slots, however this breaks
        // liveness when primary resending PrePrepare
        // the duplicated verification should only happen on slow path, which is acceptable
        // if let Some(entry) = self.log.get(pre_prepare.op_num as usize) {
        //     if entry.pre_prepare.is_some() {
        //         return Ok(());
        //     }
        // }

        // a decent implementation probably should throttle here (as well as for prepares and
        // commits) in order to mitigate faulty proposals
        // omitted since (again) that only on slow path
        let replica_id = pre_prepare.view_num as usize % self.num_replica;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if requests.sha256() == pre_prepare.digest
                && crypto.verify(replica_id, &pre_prepare).is_ok()
            {
                sender.send((Verified(pre_prepare), requests))
            } else {
                Ok(())
            }
        }))
    }
}

impl<M: ReplicaCommon> OnEvent<(Verified<PrePrepare>, Vec<Request<M::A>>)>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        (Verified(pre_prepare), requests): (Verified<PrePrepare>, Vec<Request<M::A>>),
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num {
            return Ok(());
        }
        if self.log.get(pre_prepare.op_num as usize).is_none() {
            self.log
                .resize_with(pre_prepare.op_num as usize + 1, Default::default);
        }
        if let Some(prepared) = &self.log[pre_prepare.op_num as usize].pre_prepare {
            if **prepared != *pre_prepare {
                println!("! PrePrepare not identical to the prepared one");
                return Ok(());
            }
        }
        self.log[pre_prepare.op_num as usize].pre_prepare = Some(pre_prepare.clone());
        self.log[pre_prepare.op_num as usize].requests = requests;

        let prepare = Prepare {
            view_num: self.view_num,
            op_num: pre_prepare.op_num,
            digest: pre_prepare.digest,
            replica_id: self.id,
        };
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            sender.send(Signed(crypto.sign(prepare)))
        }))?;

        if let Some(prepare_quorum) = self.prepare_quorums.get_mut(&pre_prepare.op_num) {
            prepare_quorum.retain(|_, prepare| {
                prepare.view_num == pre_prepare.view_num && prepare.digest == pre_prepare.digest
            });
        }
        if let Some(commit_quorum) = self.commit_quorums.get_mut(&pre_prepare.op_num) {
            commit_quorum.retain(|_, commit| {
                commit.view_num == pre_prepare.view_num && commit.digest == pre_prepare.digest
            })
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> OnEvent<Signed<Prepare>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Signed(prepare): Signed<Prepare>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if prepare.view_num != self.view_num {
            return Ok(());
        }
        self.net.send(All, prepare.clone())?;
        if self.log[prepare.op_num as usize].prepares.is_empty() {
            self.insert_prepare(prepare)?
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> OnEvent<Recv<Verifiable<Prepare>>>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        Recv(prepare): Recv<Verifiable<Prepare>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(pending_prepares) = self.pending_prepares.get_mut(&prepare.op_num) {
            pending_prepares.push(prepare);
            return Ok(());
        }
        let op_num = prepare.op_num;
        if self.submit_prepare(prepare)? {
            // insert the dummy entry to indicate there's ongoing task
            self.pending_prepares.insert(op_num, Default::default());
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn submit_prepare(&mut self, prepare: Verifiable<Prepare>) -> anyhow::Result<bool> {
        if prepare.view_num != self.view_num || self.view_change() {
            if prepare.view_num > self.view_num {
                todo!("state transfer to enter view")
            }
            return Ok(false);
        }
        if let Some(entry) = self.log.get(prepare.op_num as usize) {
            if !entry.prepares.is_empty() {
                return Ok(false);
            }
            if let Some(pre_prepare) = &entry.pre_prepare {
                if prepare.digest != pre_prepare.digest {
                    return Ok(false);
                }
            }
        }
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if crypto.verify(prepare.replica_id, &prepare).is_ok() {
                sender.send(Verified(prepare))
            } else {
                Ok(())
            }
        }))?;
        Ok(true)
    }
}

impl<M: ReplicaCommon> OnEvent<Verified<Prepare>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Verified(prepare): Verified<Prepare>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if prepare.view_num != self.view_num {
            return Ok(());
        }
        let op_num = prepare.op_num;
        self.insert_prepare(prepare)?;
        loop {
            let Some(pending_prepares) = self.pending_prepares.get_mut(&op_num) else {
                break;
            };
            let Some(prepare) = pending_prepares.pop() else {
                // there's no pending task, remove the task list to indicate
                self.pending_prepares.remove(&op_num);
                break;
            };
            if self.submit_prepare(prepare)? {
                break;
            }
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn insert_prepare(&mut self, prepare: Verifiable<Prepare>) -> anyhow::Result<()> {
        let prepare_quorum = self.prepare_quorums.entry(prepare.op_num).or_default();
        prepare_quorum.insert(prepare.replica_id, prepare.clone());
        // println!(
        //     "{} PrePrepare {} Prepare {}",
        //     prepare.op_num,
        //     self.log.get(prepare.op_num as usize).is_some(),
        //     prepare_quorum.len()
        // );
        if prepare_quorum.len() + 1 < self.num_replica - self.num_faulty {
            return Ok(());
        }
        let Some(entry) = self.log.get_mut(prepare.op_num as usize) else {
            // haven't matched digest for now, postpone entering "prepared" until receiving
            // pre-prepare
            return Ok(());
        };
        assert!(entry.prepares.is_empty());
        entry.prepares = self.prepare_quorums.remove(&prepare.op_num).unwrap();

        let commit = Commit {
            view_num: self.view_num,
            op_num: prepare.op_num,
            digest: prepare.digest,
            replica_id: self.id,
        };
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            sender.send(Signed(crypto.sign(commit)))
        }))
    }
}

impl<M: ReplicaCommon> OnEvent<Signed<Commit>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Signed(commit): Signed<Commit>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if commit.view_num != self.view_num {
            return Ok(());
        }
        self.net.send(All, commit.clone())?;
        if self.log[commit.op_num as usize].commits.is_empty() {
            self.insert_commit(commit, timer)?
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> OnEvent<Recv<Verifiable<Commit>>>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        Recv(commit): Recv<Verifiable<Commit>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(pending_commits) = self.pending_commits.get_mut(&commit.op_num) {
            pending_commits.push(commit);
            return Ok(());
        }
        let op_num = commit.op_num;
        if self.submit_commit(commit)? {
            // insert the dummy entry to indicate there's ongoing task
            self.pending_commits.insert(op_num, Default::default());
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn submit_commit(&mut self, commit: Verifiable<Commit>) -> anyhow::Result<bool> {
        if commit.view_num != self.view_num || self.view_change() {
            if commit.view_num > self.view_num {
                todo!("state transfer to enter view")
            }
            return Ok(false);
        }
        if let Some(entry) = self.log.get(commit.op_num as usize) {
            if !entry.commits.is_empty() {
                return Ok(false);
            }
            if let Some(pre_prepare) = &entry.pre_prepare {
                if commit.digest != pre_prepare.digest {
                    return Ok(false);
                }
            }
        }
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if crypto.verify(commit.replica_id, &commit).is_ok() {
                sender.send(Verified(commit))
            } else {
                Ok(())
            }
        }))?;
        Ok(true)
    }
}

impl<M: ReplicaCommon> OnEvent<Verified<Commit>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Verified(commit): Verified<Commit>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if commit.view_num != self.view_num {
            return Ok(());
        }
        let op_num = commit.op_num;
        self.insert_commit(commit, timer)?;
        loop {
            let Some(pending_commits) = self.pending_commits.get_mut(&op_num) else {
                break;
            };
            let Some(commit) = pending_commits.pop() else {
                // there's no pending task, remove the task list to indicate
                self.pending_commits.remove(&op_num);
                break;
            };
            if self.submit_commit(commit)? {
                break;
            }
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn insert_commit(
        &mut self,
        commit: Verifiable<Commit>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let commit_quorum = self.commit_quorums.entry(commit.op_num).or_default();
        commit_quorum.insert(commit.replica_id, commit.clone());
        // println!(
        //     "[{}] {} PrePrepare {} Commit {}",
        //     self.id,
        //     commit.op_num,
        //     self.log.get(commit.op_num as usize).is_some(),
        //     commit_quorum.len()
        // );
        if commit_quorum.len() < self.num_replica - self.num_faulty {
            return Ok(());
        }
        let Some(entry) = self.log.get_mut(commit.op_num as usize) else {
            return Ok(());
        };
        assert!(entry.commits.is_empty());
        if entry.prepares.is_empty() {
            return Ok(());
        }
        entry.commits = self.commit_quorums.remove(&commit.op_num).unwrap();

        let is_primary = self.is_primary();
        while let Some(entry) = self.log.get_mut(self.commit_num as usize + 1) {
            if entry.commits.is_empty() {
                break;
            }
            self.commit_num += 1;
            // println!("[{}] Commit {}", self.id, self.commit_num);

            if is_primary {
                timer.unset(
                    entry
                        .timer_id
                        .take()
                        .ok_or(anyhow::anyhow!("missing progress timer"))?,
                )?
            }

            for request in &entry.requests {
                let result = Payload(self.app.execute(&request.op)?);
                let seq = request.seq;
                let reply = Reply {
                    seq,
                    result,
                    view_num: self.view_num,
                    replica_id: self.id,
                };

                if self
                    .replies
                    .get(&request.client_id)
                    .map(|(seq, _)| *seq <= request.seq)
                    .unwrap_or(true)
                {
                    self.replies
                        .insert(request.client_id, (request.seq, Some(reply.clone())));
                }
                self.client_net.send(request.client_addr.clone(), reply)?
            }
        }
        if self.is_primary() {
            while !self.requests.is_empty()
                && self.op_num <= self.commit_num + Self::NUM_CONCURRENT_PRE_PREPARE
            {
                self.close_batch()?
            }
        } else if let Some(timer_id) = self.do_view_change_timer.take() {
            timer.unset(timer_id)?
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> OnEvent<DoViewChange> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        DoViewChange(view_num): DoViewChange,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let log = self
            .log
            .iter()
            .filter_map(|entry| {
                if entry.prepares.is_empty() {
                    None
                } else {
                    Some((entry.pre_prepare.clone()?, entry.prepares.clone()))
                }
            })
            .collect();
        let view_change = ViewChange {
            view_num,
            log,
            replica_id: self.id,
        };
        self.crypto_worker.submit(Box::new(|crypto, sender| {
            sender.send(Signed(crypto.sign(view_change)))
        }))
    }
}

impl<M: ReplicaCommon> OnEvent<Signed<ViewChange>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Signed(view_change): Signed<ViewChange>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        self.net.send(All, view_change.clone())?;
        self.insert_view_change(view_change, timer)
    }
}

fn verify_view_change(
    crypto: &Crypto,
    view_change: &Verifiable<ViewChange>,
    num_replica: usize,
    num_faulty: usize,
) -> anyhow::Result<()> {
    crypto.verify(view_change.replica_id, view_change)?;
    for (pre_prepare, prepares) in &view_change.log {
        anyhow::ensure!(prepares.len() >= num_replica - num_faulty);
        crypto.verify(pre_prepare.view_num as usize % num_replica, pre_prepare)?;
        for prepare in prepares.values() {
            anyhow::ensure!(prepare.digest == pre_prepare.digest);
            crypto.verify(prepare.replica_id, prepare)?
        }
    }
    Ok(())
}

impl<M: ReplicaCommon> OnEvent<Recv<Verifiable<ViewChange>>>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        Recv(view_change): Recv<Verifiable<ViewChange>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        if view_change.view_num <= self.view_num {
            return Ok(());
        }
        let num_replica = self.num_replica;
        let num_faulty = self.num_faulty;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if verify_view_change(crypto, &view_change, num_replica, num_faulty).is_ok() {
                sender.send(Verified(view_change))
            } else {
                Ok(())
            }
        }))
    }
}

impl<M: ReplicaCommon> OnEvent<Verified<ViewChange>>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        Verified(view_change): Verified<ViewChange>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        if view_change.view_num > self.view_num {
            self.insert_view_change(view_change, timer)?
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn insert_view_change(
        &mut self,
        view_change: Verifiable<ViewChange>,
        timer: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let quorum = self.view_changes.entry(view_change.view_num).or_default();
        quorum.insert(view_change.replica_id, view_change.clone());
        if quorum.len() >= self.num_replica - self.num_faulty {
            assert!(self.view_num < view_change.view_num);
            self.view_num = view_change.view_num;
            if let Some(timer_id) = self.do_view_change_timer.take() {
                timer.unset(timer_id)?
            }
            let view_changes = quorum.clone();
            if self.is_primary() {
                let new_view = NewView {
                    view_num: self.view_num,
                    view_changes,
                    pre_prepares: Default::default(),
                };
                self.crypto_worker.submit(Box::new(move |crypto, sender| {
                    // TODO add PrePrepares
                    sender.send(Signed(crypto.sign(new_view)))
                }))?
            } else {
                // TODO set timer
            }
        }
        Ok(())
    }
}

impl<M: ReplicaCommon> OnEvent<Signed<NewView>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Signed(new_view): Signed<NewView>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        self.net.send(All, new_view)
        // TODO do enter view
    }
}

impl<M: ReplicaCommon> OnEvent<Recv<Verifiable<NewView>>>
    for Replica<M::N, M::CN, M::CW, M::S, M::A, M>
{
    fn on_event(
        &mut self,
        Recv(new_view): Recv<Verifiable<NewView>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        if self.view_num > new_view.view_num
            || self.view_num == new_view.view_num && !self.view_change()
        {
            return Ok(());
        }
        let num_replica = self.num_replica;
        let num_faulty = self.num_faulty;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            let do_verify = || {
                anyhow::ensure!(new_view.view_changes.len() >= num_replica - num_faulty);
                for view_change in new_view.view_changes.values() {
                    verify_view_change(crypto, view_change, num_replica, num_faulty)?
                }
                // TODO PrePrepare semantic
                anyhow::Result::<_>::Ok(())
            };
            if do_verify().is_ok() {
                sender.send(Verified(new_view))
            } else {
                Ok(())
            }
        }))
    }
}

impl<M: ReplicaCommon> OnEvent<Verified<NewView>> for Replica<M::N, M::CN, M::CW, M::S, M::A, M> {
    fn on_event(
        &mut self,
        Verified(new_view): Verified<NewView>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        if self.view_num > new_view.view_num
            || self.view_num == new_view.view_num && !self.view_change()
        {
            return Ok(());
        }
        let new_view = new_view.into_inner();
        self.view_changes
            .insert(new_view.view_num, new_view.view_changes);
        // TODO enter new view
        Ok(())
    }
}

pub type ToClientMessageNet<T> = MessageNet<T, Reply>;

pub fn to_client_on_buf(
    buf: &[u8],
    sender: &mut impl SendEvent<Recv<Reply>>,
) -> anyhow::Result<()> {
    sender.send(Recv(deserialize(buf)?))
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum ToReplica<A> {
    Request(Request<A>),
    PrePrepare(Verifiable<PrePrepare>, Vec<Request<A>>),
    Prepare(Verifiable<Prepare>),
    Commit(Verifiable<Commit>),
    ViewChange(Verifiable<ViewChange>),
    NewView(Verifiable<NewView>),
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, ToReplica<A>>;

pub trait SendReplicaRecvEvent<A>:
    SendEvent<Recv<Request<A>>>
    + SendEvent<Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>>
    + SendEvent<Recv<Verifiable<Prepare>>>
    + SendEvent<Recv<Verifiable<Commit>>>
    + SendEvent<Recv<Verifiable<ViewChange>>>
    + SendEvent<Recv<Verifiable<NewView>>>
{
}
impl<
        T: SendEvent<Recv<Request<A>>>
            + SendEvent<Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>>
            + SendEvent<Recv<Verifiable<Prepare>>>
            + SendEvent<Recv<Verifiable<Commit>>>
            + SendEvent<Recv<Verifiable<ViewChange>>>
            + SendEvent<Recv<Verifiable<NewView>>>,
        A,
    > SendReplicaRecvEvent<A> for T
{
}

pub fn to_replica_on_buf<A: Addr>(
    buf: &[u8],
    sender: &mut impl SendReplicaRecvEvent<A>,
) -> anyhow::Result<()> {
    match deserialize(buf)? {
        ToReplica::Request(message) => sender.send(Recv(message)),
        ToReplica::PrePrepare(message, requests) => sender.send(Recv((message, requests))),
        ToReplica::Prepare(message) => sender.send(Recv(message)),
        ToReplica::Commit(message) => sender.send(Recv(message)),
        ToReplica::ViewChange(message) => sender.send(Recv(message)),
        ToReplica::NewView(message) => sender.send(Recv(message)),
    }
}

#[cfg(test)]
mod tests;

// cSpell:words upcall
