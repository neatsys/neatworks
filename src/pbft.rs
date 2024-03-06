use std::{collections::HashMap, fmt::Debug, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    app::App,
    crypto::{
        events::{Signed, Verified},
        Crypto, DigestHash as _, Verifiable,
    },
    event::{
        erased::{OnEventRichTimer as OnEvent, RichTimer as Timer},
        SendEvent, TimerId,
    },
    message::{Payload, Request},
    net::{deserialize, events::Recv, Addr, All, MessageNet, SendMessage},
    worker::erased::Worker,
    workload::{Invoke, InvokeOk},
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
    result: Payload,
    view_num: u32,
    replica_id: u8,
}

pub trait ToClientNet<A>: SendMessage<A, Reply> {}
impl<T: SendMessage<A, Reply>, A> ToClientNet<A> for T {}

pub trait ToReplicaNet<A>:
    SendMessage<u8, Request<A>>
    + SendMessage<All, Request<A>>
    + SendMessage<All, (Verifiable<PrePrepare>, Vec<Request<A>>)>
    + SendMessage<All, Verifiable<Prepare>>
    + SendMessage<All, Verifiable<Commit>>
{
}
impl<
        T: SendMessage<u8, Request<A>>
            + SendMessage<All, Request<A>>
            + SendMessage<All, (Verifiable<PrePrepare>, Vec<Request<A>>)>
            + SendMessage<All, Verifiable<Prepare>>
            + SendMessage<All, Verifiable<Commit>>,
        A,
    > ToReplicaNet<A> for T
{
}

pub struct Client<A> {
    id: u32,
    addr: A,
    seq: u32,
    invoke: Option<ClientInvoke>,
    view_num: u32,
    num_replica: usize,
    num_faulty: usize,

    net: Box<dyn ToReplicaNet<A> + Send + Sync>,
    upcall: Box<dyn SendEvent<InvokeOk> + Send + Sync>,
}

#[derive(Debug)]
struct ClientInvoke {
    op: Payload,
    resend_timer: TimerId,
    replies: HashMap<u8, Reply>,
}

impl<A: Addr> Debug for Client<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("id", &self.id)
            .field("addr", &self.addr)
            .field("seq", &self.seq)
            .field("invoke", &self.invoke)
            .field("view_num", &self.view_num)
            .field("num_replica", &self.num_replica)
            .field("num_faulty", &self.num_faulty)
            .finish_non_exhaustive()
    }
}

impl<A> Client<A> {
    pub fn new(
        id: u32,
        addr: A,
        net: impl ToReplicaNet<A> + Send + Sync + 'static,
        upcall: impl SendEvent<InvokeOk> + Send + Sync + 'static,
        num_replica: usize,
        num_faulty: usize,
    ) -> Self {
        Self {
            id,
            addr,
            net: Box::new(net),
            upcall: Box::new(upcall),
            num_replica,
            num_faulty,
            seq: 0,
            view_num: 0,
            invoke: Default::default(),
        }
    }
}

impl<A: Addr> OnEvent<Invoke> for Client<A> {
    fn on_event(&mut self, Invoke(op): Invoke, timer: &mut impl Timer<Self>) -> anyhow::Result<()> {
        if self.invoke.is_some() {
            anyhow::bail!("concurrent invocation")
        }
        self.seq += 1;
        let invoke = ClientInvoke {
            op,
            resend_timer: timer.set(Duration::from_millis(1000), Resend)?,
            replies: Default::default(),
        };
        self.invoke = Some(invoke);
        self.do_send((self.view_num as usize % self.num_replica) as u8)
    }
}

#[derive(Debug, Clone)]
struct Resend;

impl<A: Addr> OnEvent<Resend> for Client<A> {
    fn on_event(&mut self, Resend: Resend, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        // println!("Resend timeout on seq {}", self.seq);
        // self.do_send(AllReplica)
        Ok(())
    }
}

impl<A> OnEvent<Recv<Reply>> for Client<A> {
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

impl<A: Addr> Client<A> {
    fn do_send<B>(&mut self, dest: B) -> anyhow::Result<()>
    where
        dyn ToReplicaNet<A>: SendMessage<B, Request<A>>,
    {
        let request = Request {
            client_id: self.id,
            client_addr: self.addr.clone(),
            seq: self.seq,
            op: self.invoke.as_ref().unwrap().op.clone(),
        };
        // either this or add `Send + Sync` in trait bound above. i choose this
        (&mut *self.net as &mut dyn ToReplicaNet<A>).send(dest, request)
    }
}

pub trait SendCryptoEvent<A>:
    SendEvent<(Signed<PrePrepare>, Vec<Request<A>>)>
    + SendEvent<(Verified<PrePrepare>, Vec<Request<A>>)>
    + SendEvent<Signed<Prepare>>
    + SendEvent<Verified<Prepare>>
    + SendEvent<Signed<Commit>>
    + SendEvent<Verified<Commit>>
{
}
impl<
        T: SendEvent<(Signed<PrePrepare>, Vec<Request<A>>)>
            + SendEvent<(Verified<PrePrepare>, Vec<Request<A>>)>
            + SendEvent<Signed<Prepare>>
            + SendEvent<Verified<Prepare>>
            + SendEvent<Signed<Commit>>
            + SendEvent<Verified<Commit>>,
        A,
    > SendCryptoEvent<A> for T
{
}

pub struct Replica<S, A> {
    id: u8,
    num_replica: usize,
    num_faulty: usize,

    replies: HashMap<u32, (u32, Option<Reply>)>,
    requests: Vec<Request<A>>,
    view_num: u32,
    op_num: u32,
    log: Vec<LogEntry<A>>,
    prepare_quorums: HashMap<u32, HashMap<u8, Verifiable<Prepare>>>,
    commit_quorums: HashMap<u32, HashMap<u8, Verifiable<Commit>>>,
    commit_num: u32,
    app: S,
    // op number -> task
    pending_prepares: HashMap<u32, Vec<Verifiable<Prepare>>>,
    pending_commits: HashMap<u32, Vec<Verifiable<Commit>>>,

    net: Box<dyn ToReplicaNet<A> + Send + Sync>,
    client_net: Box<dyn ToClientNet<A> + Send + Sync>,
    crypto_worker: Worker<Crypto<u8>, dyn SendCryptoEvent<A> + Send + Sync>,
}

#[derive(Debug)]
struct LogEntry<A> {
    view_num: u32,
    pre_prepare: Option<Verifiable<PrePrepare>>,
    requests: Vec<Request<A>>,
    prepares: Vec<(u8, Verifiable<Prepare>)>,
    commits: Vec<(u8, Verifiable<Commit>)>,
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

impl<S, A> Debug for Replica<S, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica").finish_non_exhaustive()
    }
}

impl<S, A> Replica<S, A> {
    pub fn new(
        id: u8,
        app: S,
        net: impl ToReplicaNet<A> + Send + Sync + 'static,
        client_net: impl ToClientNet<A> + Send + Sync + 'static,
        crypto_worker: Worker<Crypto<u8>, dyn SendCryptoEvent<A> + Send + Sync>,
        num_replica: usize,
        num_faulty: usize,
    ) -> Self {
        Self {
            id,
            app,
            net: Box::new(net),
            client_net: Box::new(client_net),
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
            pending_prepares: Default::default(),
            pending_commits: Default::default(),
        }
    }
}

impl<S, A> Replica<S, A> {
    fn is_primary(&self) -> bool {
        (self.id as usize % self.num_replica) == self.view_num as usize
    }

    const NUM_CONCURRENT_PRE_PREPARE: u32 = 1;
}

impl<S, A: Addr> OnEvent<Recv<Request<A>>> for Replica<S, A> {
    fn on_event(
        &mut self,
        Recv(request): Recv<Request<A>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
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
            todo!("forward request")
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

impl<S, A: Addr> Replica<S, A> {
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

impl<S, A: Addr> OnEvent<(Signed<PrePrepare>, Vec<Request<A>>)> for Replica<S, A> {
    fn on_event(
        &mut self,
        (Signed(pre_prepare), requests): (Signed<PrePrepare>, Vec<Request<A>>),
        _: &mut impl Timer<Self>,
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
        self.net.send(All, (pre_prepare, requests))
    }
}

impl<S, A: Addr> OnEvent<Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>> for Replica<S, A> {
    fn on_event(
        &mut self,
        Recv((pre_prepare, requests)): Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>,
        _: &mut impl Timer<Self>,
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
                sender.send((Verified(pre_prepare), requests))
            } else {
                Ok(())
            }
        }))
    }
}

impl<S, A> OnEvent<(Verified<PrePrepare>, Vec<Request<A>>)> for Replica<S, A> {
    fn on_event(
        &mut self,
        (Verified(pre_prepare), requests): (Verified<PrePrepare>, Vec<Request<A>>),
        _: &mut impl Timer<Self>,
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
        self.log[pre_prepare.op_num as usize].pre_prepare = Some(pre_prepare.clone());
        self.log[pre_prepare.op_num as usize].view_num = self.view_num;
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
            prepare_quorum.retain(|_, prepare| prepare.digest == pre_prepare.digest);
        }
        if let Some(commit_quorum) = self.commit_quorums.get_mut(&pre_prepare.op_num) {
            commit_quorum.retain(|_, commit| commit.digest == pre_prepare.digest)
        }
        Ok(())
    }
}

impl<S, A> OnEvent<Signed<Prepare>> for Replica<S, A> {
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

impl<S, A> OnEvent<Recv<Verifiable<Prepare>>> for Replica<S, A> {
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

impl<S, A> Replica<S, A> {
    fn submit_prepare(&mut self, prepare: Verifiable<Prepare>) -> anyhow::Result<bool> {
        if prepare.view_num != self.view_num {
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
            if crypto.verify(&prepare.replica_id, &prepare).is_ok() {
                sender.send(Verified(prepare))
            } else {
                Ok(())
            }
        }))?;
        Ok(true)
    }
}

impl<S, A> OnEvent<Verified<Prepare>> for Replica<S, A> {
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

impl<S, A> Replica<S, A> {
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
        entry.prepares = self
            .prepare_quorums
            .remove(&prepare.op_num)
            .unwrap()
            .into_iter()
            .collect();

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

impl<S: App, A: Addr> OnEvent<Signed<Commit>> for Replica<S, A> {
    fn on_event(
        &mut self,
        Signed(commit): Signed<Commit>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if commit.view_num != self.view_num {
            return Ok(());
        }
        self.net.send(All, commit.clone())?;
        if self.log[commit.op_num as usize].commits.is_empty() {
            self.insert_commit(commit)?
        }
        Ok(())
    }
}

impl<S, A> OnEvent<Recv<Verifiable<Commit>>> for Replica<S, A> {
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

impl<S, A> Replica<S, A> {
    fn submit_commit(&mut self, commit: Verifiable<Commit>) -> anyhow::Result<bool> {
        if commit.view_num != self.view_num {
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
            if crypto.verify(&commit.replica_id, &commit).is_ok() {
                sender.send(Verified(commit))
            } else {
                Ok(())
            }
        }))?;
        Ok(true)
    }
}

impl<S: App, A: Addr> OnEvent<Verified<Commit>> for Replica<S, A> {
    fn on_event(
        &mut self,
        Verified(commit): Verified<Commit>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if commit.view_num != self.view_num {
            return Ok(());
        }
        let op_num = commit.op_num;
        self.insert_commit(commit)?;
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

impl<S: App, A: Addr> Replica<S, A> {
    fn insert_commit(&mut self, commit: Verifiable<Commit>) -> anyhow::Result<()> {
        let commit_quorum = self.commit_quorums.entry(commit.op_num).or_default();
        commit_quorum.insert(commit.replica_id, commit.clone());
        // println!(
        //     "{} PrePrepare {} Commit {}",
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
        entry.commits = self
            .commit_quorums
            .remove(&commit.op_num)
            .unwrap()
            .into_iter()
            .collect();

        while let Some(entry) = self.log.get(self.commit_num as usize + 1) {
            if entry.commits.is_empty() {
                break;
            }
            self.commit_num += 1;
            // println!("Commit {}", self.commit_num);
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
}

pub type ToReplicaMessageNet<T, A> = MessageNet<T, ToReplica<A>>;

pub trait SendReplicaRecvEvent<A>:
    SendEvent<Recv<Request<A>>>
    + SendEvent<Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>>
    + SendEvent<Recv<Verifiable<Prepare>>>
    + SendEvent<Recv<Verifiable<Commit>>>
{
}
impl<
        T: SendEvent<Recv<Request<A>>>
            + SendEvent<Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>>
            + SendEvent<Recv<Verifiable<Prepare>>>
            + SendEvent<Recv<Verifiable<Commit>>>,
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
    }
}
