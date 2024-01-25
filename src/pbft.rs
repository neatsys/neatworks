use std::{collections::HashMap, fmt::Debug, time::Duration};

use bincode::Options as _;
use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    crypto::{Crypto, DigestHash as _, Signed},
    event::{OnEvent, SendEvent, Timer, TimerId},
    net::{Addr, MessageNet, SendMessage},
    replication::{AllReplica, Request},
    worker::Worker,
};

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct PrePrepare {
    view_num: u32,
    op_num: u32,
    digest: [u8; 32],
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct Prepare {
    view_num: u32,
    op_num: u32,
    digest: [u8; 32],
    replica_id: u8,
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct Commit {
    view_num: u32,
    op_num: u32,
    digest: [u8; 32],
    replica_id: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    seq: u32,
    result: Vec<u8>,
    view_num: u32,
    replica_id: u8,
}

pub trait ToClientNet<A>: SendMessage<A, Reply> {}
impl<T: SendMessage<A, Reply>, A> ToClientNet<A> for T {}

pub trait ToReplicaNet<A>:
    SendMessage<u8, Request<A>>
    + SendMessage<AllReplica, Request<A>>
    + SendMessage<AllReplica, (Signed<PrePrepare>, Vec<Request<A>>)>
    + SendMessage<AllReplica, Signed<Prepare>>
    + SendMessage<AllReplica, Signed<Commit>>
{
}
impl<
        T: SendMessage<u8, Request<A>>
            + SendMessage<AllReplica, Request<A>>
            + SendMessage<AllReplica, (Signed<PrePrepare>, Vec<Request<A>>)>
            + SendMessage<AllReplica, Signed<Prepare>>
            + SendMessage<AllReplica, Signed<Commit>>,
        A,
    > ToReplicaNet<A> for T
{
}

#[derive(Debug, Clone, derive_more::From)]
pub enum ClientEvent {
    Invoke(Vec<u8>),
    Ingress(Reply),
    ResendTimeout,
}

pub trait ClientUpcall: SendEvent<(u32, Vec<u8>)> {}
impl<T: SendEvent<(u32, Vec<u8>)>> ClientUpcall for T {}

#[derive(Debug)]
pub struct Client<N, U, A> {
    id: u32,
    addr: A,
    seq: u32,
    invoke: Option<ClientInvoke>,
    view_num: u32,
    num_replica: usize,
    num_faulty: usize,

    net: N,
    upcall: U,
}

#[derive(Debug)]
struct ClientInvoke {
    op: Vec<u8>,
    resend_timer: TimerId,
    replies: HashMap<u8, Reply>,
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
            resend_timer: timer.set(Duration::from_millis(1000), ClientEvent::ResendTimeout)?,
            replies: Default::default(),
        };
        self.invoke = Some(invoke);
        self.do_send((self.view_num as usize % self.num_replica) as u8)
    }

    fn on_resend_timeout(&mut self) -> anyhow::Result<()> {
        // TODO logging
        self.do_send(AllReplica)
    }

    fn on_ingress(
        &mut self,
        reply: Reply,
        timer: &mut dyn Timer<ClientEvent>,
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
        self.net.send(dest, request)
    }
}

#[derive(Debug)]
pub enum ReplicaEvent<A> {
    IngressRequest(Request<A>),
    SignedPrePrepare(Signed<PrePrepare>, Vec<Request<A>>),
    IngressPrePrepare(Signed<PrePrepare>, Vec<Request<A>>),
    VerifiedPrePrepare(Signed<PrePrepare>, Vec<Request<A>>),
    SignedPrepare(Signed<Prepare>),
    IngressPrepare(Signed<Prepare>),
    VerifiedPrepare(Signed<Prepare>),
    SignedCommit(Signed<Commit>),
    IngressCommit(Signed<Commit>),
    VerifiedCommit(Signed<Commit>),
}

pub struct Replica<S, N, M, A> {
    id: u8,
    num_replica: usize,
    num_faulty: usize,

    on_request: HashMap<u32, OnRequest<A, M>>,
    requests: Vec<Request<A>>,
    view_num: u32,
    op_num: u32,
    log: Vec<LogEntry<A>>,
    prepare_quorums: HashMap<u32, HashMap<u8, Signed<Prepare>>>,
    commit_quorums: HashMap<u32, HashMap<u8, Signed<Commit>>>,
    commit_num: u32,
    app: S,
    // op number -> task
    on_verified_prepare_tasks: HashMap<u32, Vec<OnVerified<Self>>>,
    on_verified_commit_tasks: HashMap<u32, Vec<OnVerified<Self>>>,

    net: N,
    client_net: M,
    crypto_worker: Worker<Crypto<u8>, ReplicaEvent<A>>,
}

type OnRequest<A, N> = Box<dyn Fn(&Request<A>, &mut N) -> anyhow::Result<bool> + Send + Sync>;
type OnVerified<S> = Box<dyn FnOnce(&mut S) -> anyhow::Result<()> + Send + Sync>;

#[derive(Debug)]
struct LogEntry<A> {
    view_num: u32,
    pre_prepare: Option<Signed<PrePrepare>>,
    requests: Vec<Request<A>>,
    prepares: Vec<(u8, Signed<Prepare>)>,
    commits: Vec<(u8, Signed<Commit>)>,
}

impl<A> Default for LogEntry<A> {
    fn default() -> Self {
        Self {
            view_num: Default::default(),
            pre_prepare: Default::default(),
            requests: Default::default(),
            prepares: Default::default(),
            commits: Default::default(),
        }
    }
}

impl<S, N, M, A> Debug for Replica<S, N, M, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica").finish_non_exhaustive()
    }
}

impl<S, N, M, A> Replica<S, N, M, A> {
    pub fn new(
        id: u8,
        app: S,
        net: N,
        client_net: M,
        crypto_worker: Worker<Crypto<u8>, ReplicaEvent<A>>,
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
            on_request: Default::default(),
            requests: Default::default(),
            view_num: 0,
            op_num: 0,
            log: Default::default(),
            prepare_quorums: Default::default(),
            commit_quorums: Default::default(),
            commit_num: 0,
            on_verified_prepare_tasks: Default::default(),
            on_verified_commit_tasks: Default::default(),
        }
    }
}

impl<S: App + 'static, N: ToReplicaNet<A> + 'static, M: ToClientNet<A> + 'static, A: Addr>
    OnEvent<ReplicaEvent<A>> for Replica<S, N, M, A>
{
    fn on_event(
        &mut self,
        event: ReplicaEvent<A>,
        _timer: &mut dyn Timer<ReplicaEvent<A>>,
    ) -> anyhow::Result<()> {
        // println!("{event:?}");
        match event {
            ReplicaEvent::IngressRequest(request) => self.on_ingress_request(request),
            ReplicaEvent::SignedPrePrepare(pre_prepare, requests) => {
                self.on_signed_pre_prepare(pre_prepare, requests)
            }
            ReplicaEvent::IngressPrePrepare(pre_prepare, requests) => {
                self.on_ingress_pre_prepare(pre_prepare, requests)
            }
            ReplicaEvent::VerifiedPrePrepare(pre_prepare, requests) => {
                self.on_verified_pre_prepare(pre_prepare, requests)
            }
            ReplicaEvent::SignedPrepare(prepare) => self.on_signed_prepare(prepare),
            ReplicaEvent::IngressPrepare(prepare) => self.on_ingress_prepare(prepare),
            ReplicaEvent::VerifiedPrepare(prepare) => self.on_verified_prepare(prepare),
            ReplicaEvent::SignedCommit(commit) => self.on_signed_commit(commit),
            ReplicaEvent::IngressCommit(commit) => self.on_ingress_commit(commit),
            ReplicaEvent::VerifiedCommit(commit) => self.on_verified_commit(commit),
        }
    }
}

impl<S: App + 'static, N: ToReplicaNet<A> + 'static, M: ToClientNet<A> + 'static, A: Addr>
    Replica<S, N, M, A>
{
    fn is_primary(&self) -> bool {
        (self.id as usize % self.num_replica) == self.view_num as usize
    }

    const NUM_CONCURRENT_PRE_PREPARE: u32 = 1;

    fn on_ingress_request(&mut self, request: Request<A>) -> anyhow::Result<()> {
        if let Some(on_request) = self.on_request.get(&request.client_id) {
            if on_request(&request, &mut self.client_net)? {
                return Ok(());
            }
        }
        if !self.is_primary() {
            todo!("forward request")
        }
        // ignore resend of ongoing consensus
        self.on_request
            .insert(request.client_id, Box::new(|_, _| Ok(true)));
        self.requests.push(request);
        if self.op_num < self.commit_num + Self::NUM_CONCURRENT_PRE_PREPARE {
            self.close_batch()
        } else {
            Ok(())
        }
    }

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
            sender.send(ReplicaEvent::SignedPrePrepare(
                crypto.sign(pre_prepare),
                requests,
            ))
        }))
    }

    fn on_signed_pre_prepare(
        &mut self,
        pre_prepare: Signed<PrePrepare>,
        requests: Vec<Request<A>>,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num {
            return Ok(());
        }
        if self.log.get(pre_prepare.op_num as usize).is_none() {
            self.log
                .resize_with(pre_prepare.op_num as usize + 1, Default::default);
        }
        let replaced = self.log[pre_prepare.op_num as usize]
            .pre_prepare
            .replace(pre_prepare.clone());
        assert!(replaced.is_none());
        self.log[pre_prepare.op_num as usize].view_num = self.view_num;
        self.log[pre_prepare.op_num as usize].requests = requests.clone();
        self.net.send(AllReplica, (pre_prepare, requests))
    }

    fn on_ingress_pre_prepare(
        &mut self,
        pre_prepare: Signed<PrePrepare>,
        requests: Vec<Request<A>>,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num {
            if pre_prepare.view_num > self.view_num {
                todo!("state transfer to enter view")
            }
            return Ok(());
        }
        if let Some(entry) = self.log.get(pre_prepare.op_num as usize) {
            if entry.pre_prepare.is_some() {
                return Ok(());
            }
        }
        // a decent implementation probably should throttle here (as well as for prepares and
        // commits) in order to mitigate faulty proposals
        // omitted since it makes no difference in normal path
        let replica_id = (pre_prepare.view_num as usize % self.num_replica) as _;
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            if requests.sha256() == pre_prepare.digest
                && crypto.verify(&replica_id, &pre_prepare).is_ok()
            {
                sender.send(ReplicaEvent::VerifiedPrePrepare(pre_prepare, requests))
            } else {
                Ok(())
            }
        }))
    }

    fn on_verified_pre_prepare(
        &mut self,
        pre_prepare: Signed<PrePrepare>,
        requests: Vec<Request<A>>,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num {
            return Ok(());
        }
        if self.log.get(pre_prepare.op_num as usize).is_none() {
            self.log
                .resize_with(pre_prepare.op_num as usize + 1, Default::default);
        }
        if self.log[pre_prepare.op_num as usize].pre_prepare.is_some() {
            return Ok(());
        }
        let _ = self.log[pre_prepare.op_num as usize]
            .pre_prepare
            .insert(pre_prepare.clone());
        self.log[pre_prepare.op_num as usize].view_num = self.view_num;
        self.log[pre_prepare.op_num as usize].requests = requests;

        let prepare = Prepare {
            view_num: self.view_num,
            op_num: pre_prepare.op_num,
            digest: pre_prepare.digest,
            replica_id: self.id,
        };
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            sender.send(ReplicaEvent::SignedPrepare(crypto.sign(prepare)))
        }))?;

        if let Some(prepare_quorum) = self.prepare_quorums.get_mut(&pre_prepare.op_num) {
            prepare_quorum.retain(|_, prepare| prepare.digest == pre_prepare.digest);
        }
        if let Some(commit_quorum) = self.commit_quorums.get_mut(&pre_prepare.op_num) {
            commit_quorum.retain(|_, commit| commit.digest == pre_prepare.digest)
        }
        Ok(())
    }

    fn on_signed_prepare(&mut self, prepare: Signed<Prepare>) -> anyhow::Result<()> {
        if prepare.view_num != self.view_num {
            return Ok(());
        }
        self.net.send(AllReplica, prepare.clone())?;
        self.insert_prepare(prepare)?;
        Ok(())
    }

    fn on_ingress_prepare(&mut self, prepare: Signed<Prepare>) -> anyhow::Result<()> {
        let op_num = prepare.op_num;
        let do_verify = move |this: &mut Self| {
            if prepare.view_num != this.view_num {
                if prepare.view_num > this.view_num {
                    todo!("state transfer to enter view")
                }
                return Ok(());
            }
            if let Some(entry) = this.log.get(prepare.op_num as usize) {
                if !entry.prepares.is_empty() {
                    return Ok(());
                }
                if let Some(pre_prepare) = &entry.pre_prepare {
                    if prepare.digest != pre_prepare.digest {
                        return Ok(());
                    }
                }
            }
            this.crypto_worker.submit(Box::new(move |crypto, sender| {
                if crypto.verify(&prepare.replica_id, &prepare).is_ok() {
                    sender.send(ReplicaEvent::VerifiedPrepare(prepare))
                } else {
                    Ok(())
                }
            }))
        };
        if let Some(on_verified) = self.on_verified_prepare_tasks.get_mut(&op_num) {
            on_verified.push(Box::new(do_verify));
            Ok(())
        } else {
            // insert the dummy entry to indicate there's ongoing task
            self.on_verified_prepare_tasks
                .insert(op_num, Default::default());
            do_verify(self)
        }
    }

    fn on_verified_prepare(&mut self, prepare: Signed<Prepare>) -> anyhow::Result<()> {
        if prepare.view_num != self.view_num {
            return Ok(());
        }
        let op_num = prepare.op_num;
        self.insert_prepare(prepare)?;
        if let Some(on_verified) = self.on_verified_prepare_tasks.get_mut(&op_num) {
            if let Some(on_verified) = on_verified.pop() {
                on_verified(self)?;
            } else {
                // there's no pending task, remove the task list to indicate
                self.on_verified_prepare_tasks.remove(&op_num);
            }
        }
        Ok(())
    }

    fn insert_prepare(&mut self, prepare: Signed<Prepare>) -> anyhow::Result<()> {
        let prepare_quorum = self.prepare_quorums.entry(prepare.op_num).or_default();
        prepare_quorum.insert(prepare.replica_id, prepare.clone());
        let Some(entry) = self.log.get_mut(prepare.op_num as usize) else {
            // cannot match digest for now, postpone entering "prepared" until receiving pre-prepare
            return Ok(());
        };
        if prepare_quorum.len() + 1 < self.num_replica - self.num_faulty {
            return Ok(());
        }
        assert!(entry.prepares.is_empty());
        entry.prepares = self
            .prepare_quorums
            .remove(&prepare.op_num)
            .unwrap()
            .into_iter()
            .collect();
        self.on_verified_prepare_tasks.remove(&prepare.op_num);

        let commit = Commit {
            view_num: self.view_num,
            op_num: prepare.op_num,
            digest: prepare.digest,
            replica_id: self.id,
        };
        self.crypto_worker.submit(Box::new(move |crypto, sender| {
            sender.send(ReplicaEvent::SignedCommit(crypto.sign(commit)))
        }))
    }

    fn on_signed_commit(&mut self, commit: Signed<Commit>) -> anyhow::Result<()> {
        if commit.view_num != self.view_num {
            return Ok(());
        }
        self.net.send(AllReplica, commit.clone())?;
        self.insert_commit(commit)
    }

    fn on_ingress_commit(&mut self, commit: Signed<Commit>) -> anyhow::Result<()> {
        let op_num = commit.op_num;
        let do_verify = move |this: &mut Self| {
            if commit.view_num != this.view_num {
                if commit.view_num > this.view_num {
                    todo!("state transfer to enter view")
                }
                return Ok(());
            }
            if let Some(entry) = this.log.get(commit.op_num as usize) {
                if !entry.commits.is_empty() {
                    return Ok(());
                }
                if let Some(pre_prepare) = &entry.pre_prepare {
                    if commit.digest != pre_prepare.digest {
                        return Ok(());
                    }
                }
            }
            this.crypto_worker.submit(Box::new(move |crypto, sender| {
                if crypto.verify(&commit.replica_id, &commit).is_ok() {
                    sender.send(ReplicaEvent::VerifiedCommit(commit))
                } else {
                    Ok(())
                }
            }))
        };
        if let Some(on_verified) = self.on_verified_commit_tasks.get_mut(&op_num) {
            on_verified.push(Box::new(do_verify));
            Ok(())
        } else {
            self.on_verified_commit_tasks
                .insert(op_num, Default::default());
            do_verify(self)
        }
    }

    fn on_verified_commit(&mut self, commit: Signed<Commit>) -> anyhow::Result<()> {
        if commit.view_num != self.view_num {
            return Ok(());
        }
        let op_num = commit.op_num;
        self.insert_commit(commit)?;
        if let Some(on_verified) = self.on_verified_commit_tasks.get_mut(&op_num) {
            if let Some(on_verified) = on_verified.pop() {
                on_verified(self)?;
            } else {
                self.on_verified_commit_tasks.remove(&op_num);
            }
        }
        Ok(())
    }

    fn insert_commit(&mut self, commit: Signed<Commit>) -> anyhow::Result<()> {
        let commit_quorum = self.commit_quorums.entry(commit.op_num).or_default();
        commit_quorum.insert(commit.replica_id, commit.clone());
        if commit_quorum.len() < self.num_replica - self.num_faulty {
            return Ok(());
        }
        let Some(entry) = self.log.get_mut(commit.op_num as usize) else {
            return Ok(());
        };
        if entry.prepares.is_empty() {
            return Ok(());
        }
        assert!(entry.commits.is_empty());
        entry.commits = self
            .commit_quorums
            .remove(&commit.op_num)
            .unwrap()
            .into_iter()
            .collect();
        self.on_verified_commit_tasks.remove(&commit.op_num);

        while let Some(entry) = self.log.get(self.commit_num as usize + 1) {
            if entry.commits.is_empty() {
                break;
            }
            self.commit_num += 1;
            for request in &entry.requests {
                let result = self.app.execute(&request.op)?;
                let seq = request.seq;
                let reply = Reply {
                    seq,
                    result,
                    view_num: self.view_num,
                    replica_id: self.id,
                };
                let addr = request.client_addr.clone();
                let on_request = move |request: &Request<A>, net: &mut M| {
                    if request.seq < seq {
                        return Ok(true);
                    }
                    if request.seq == seq {
                        net.send(addr.clone(), reply.clone())?;
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                };
                on_request(request, &mut self.client_net)?;
                self.on_request
                    .insert(request.client_id, Box::new(on_request));
            }
        }
        while self.is_primary()
            && !self.requests.is_empty()
            && self.op_num <= self.commit_num + Self::NUM_CONCURRENT_PRE_PREPARE
        {
            self.close_batch()?
        }
        Ok(())
    }
}

pub type ToClientMessageNet<T> = MessageNet<T, Reply>;

pub fn to_client_on_buf(sender: &mut impl SendEvent<Reply>, buf: &[u8]) -> anyhow::Result<()> {
    let message = bincode::options().allow_trailing_bytes().deserialize(buf)?;
    sender.send(message)
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum ToReplica<A> {
    Request(Request<A>),
    PrePrepare(Signed<PrePrepare>, Vec<Request<A>>),
    Prepare(Signed<Prepare>),
    Commit(Signed<Commit>),
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, ToReplica<A>>;

pub fn to_replica_on_buf<A: Addr>(
    sender: &mut impl SendEvent<ReplicaEvent<A>>,
    buf: &[u8],
) -> anyhow::Result<()> {
    match bincode::options().allow_trailing_bytes().deserialize(buf)? {
        ToReplica::Request(message) => sender.send(ReplicaEvent::IngressRequest(message)),
        ToReplica::PrePrepare(message, requests) => {
            sender.send(ReplicaEvent::IngressPrePrepare(message, requests))
        }
        ToReplica::Prepare(message) => sender.send(ReplicaEvent::IngressPrepare(message)),
        ToReplica::Commit(message) => sender.send(ReplicaEvent::IngressCommit(message)),
    }
}
