use std::collections::BTreeMap;

use crate::{
    codec::Payload,
    crypto::{
        events::{Signed, Verified},
        Crypto, DigestHash, Verifiable, H256,
    },
    event::{
        work::{self, Submit, Upcall as _},
        OnErasedEvent, ScheduleEvent,
    },
    net::{combinators::All, events::Recv, Addr, SendMessage},
    timer::Timer,
};

use super::{
    messages::{
        Commit, NewView, PrePrepare, Prepare, QueryNewView, Quorum, Quorums, Reply, Request,
        ViewChange,
    },
    PublicParameters,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaState<S, A> {
    id: u8,
    config: PublicParameters,

    replies: BTreeMap<u32, (u32, Option<Reply>)>, // client id -> (seq, result)
    requests: Vec<Request<A>>,
    view_num: u32,
    new_views: BTreeMap<u32, Verifiable<NewView>>,
    // convention: log[0] is unused offset and always with None `pre_prepare`
    // log[op_num as usize].pre_prepare.op_num == op_num
    // for no-op slot during view change, requests == Default::default() (i.e. empty vector)
    // pre_prepare = Some(pre_prepare) where pre_prepare.digest = DIGEST_NO_OP
    // DIGEST_NO_OP is probably not empty `requests`'s digest, but it's more convenient in this way
    // a more consistent design may be log[0] also has some `pre_prepare` and becomes a regular
    // no-op slot, but i don't bother
    log: Vec<LogEntry<A>>,
    prepare_quorums: Quorums<u32, Prepare>, // u32 = op number
    commit_quorums: Quorums<u32, Commit>,
    commit_num: u32,
    app: S,

    do_view_change_timer: Timer<events::DoViewChange>,
    progress_view_change_timer: Timer<events::ProgressViewChange>,
    view_changes: Quorums<u32, ViewChange>, // u32 = view number

    // any op num presents in this maps -> there's ongoing verification submitted
    // entry presents but empty list -> no pending but one is verifying
    // no entry present -> no pending and not verifying
    // invent enum for this if wants to improve readability later
    pending_prepares: BTreeMap<u32, Vec<Verifiable<Prepare>>>,
    pending_commits: BTreeMap<u32, Vec<Verifiable<Commit>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LogEntry<A> {
    pre_prepare: Option<Verifiable<PrePrepare>>,
    requests: Vec<Request<A>>,
    prepares: Quorum<Prepare>,
    commits: Quorum<Commit>,

    progress_timer: Timer<events::ProgressPrepare>,
    state_transfer_timer: Timer<events::StateTransfer>,
}

const NO_OP_DIGEST: H256 = H256::zero();

impl<S, A> ReplicaState<S, A> {
    pub fn new(id: u8, app: S, config: PublicParameters) -> Self {
        let (
            replies,
            requests,
            view_num,
            new_views,
            log,
            prepare_quorums,
            commit_quorums,
            commit_num,
            view_changes,
            pending_prepares,
            pending_commits,
        ) = Default::default();
        Self {
            id,
            app,

            do_view_change_timer: Timer::new(config.view_change_delay),
            progress_view_change_timer: Timer::new(config.progress_view_change_interval),
            config,

            replies,
            requests,
            view_num,
            new_views,
            log,
            prepare_quorums,
            commit_quorums,
            commit_num,
            view_changes,
            pending_prepares,
            pending_commits,
        }
    }
}

pub mod events {
    #[derive(Debug)]
    pub struct DoViewChange(pub u32);

    #[derive(Debug)]
    pub struct ProgressPrepare(pub u32); // op number

    #[derive(Debug)]
    pub struct ProgressViewChange;

    #[derive(Debug)]
    pub struct StateTransfer(pub u32);
}

pub trait Context<S, A>: Sized {
    type PeerNet: SendMessage<u8, Request<A>>
        + SendMessage<All, (Verifiable<PrePrepare>, Vec<Request<A>>)>
        + SendMessage<All, Verifiable<Prepare>>
        + SendMessage<All, Verifiable<Commit>>
        + SendMessage<All, Verifiable<ViewChange>>
        + SendMessage<All, Verifiable<NewView>>
        + SendMessage<u8, QueryNewView>
        + SendMessage<u8, Verifiable<NewView>>;
    type DownlinkNet: SendMessage<A, Reply>;
    type CryptoWorker: Submit<Crypto, Self::CryptoContext>;
    type CryptoContext: work::Upcall<S, Self>;
    type Schedule: ScheduleEvent<events::DoViewChange>
        + ScheduleEvent<events::ProgressPrepare>
        + ScheduleEvent<events::ProgressViewChange>
        + ScheduleEvent<events::StateTransfer>;
    fn peer_net(&mut self) -> &mut Self::PeerNet;
    fn downlink_net(&mut self) -> &mut Self::DownlinkNet;
    fn crypto_worker(&mut self) -> &mut Self::CryptoWorker;
    fn schedule(&mut self) -> &mut Self::Schedule;
}

trait ContextExt<S, A>: Context<S, A> {
    fn submit_sign<M: DigestHash + Send + 'static>(&mut self, message: M) -> anyhow::Result<()>
    where
        S: OnErasedEvent<Signed<M>, Self>,
    {
        self.crypto_worker()
            .submit(Box::new(move |crypto, context| {
                context.send(Signed(crypto.sign(message)))
            }))
    }
}
impl<C: Context<S, A>, S, A> ContextExt<S, A> for C {}

impl<S, A> ReplicaState<S, A> {
    fn is_primary(&self) -> bool {
        (self.view_num as usize % self.config.num_replica) == self.id as usize
    }

    fn view_change(&self) -> bool {
        self.view_num != 0 && !self.new_views.contains_key(&self.view_num)
    }

    fn op_num(&self) -> u32 {
        (self.log.len() as u32).max(1)
    }

    fn default_entry(&self) -> LogEntry<A> {
        LogEntry {
            pre_prepare: None,
            requests: Default::default(),
            prepares: Default::default(),
            commits: Default::default(),
            progress_timer: Timer::new(self.config.progress_prepare_interval),
            state_transfer_timer: Timer::new(self.config.state_transfer_delay),
        }
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Recv<Request<A>>, C> for ReplicaState<S, A> {
    fn on_event(&mut self, Recv(request): Recv<Request<A>>, context: &mut C) -> anyhow::Result<()> {
        if self.view_change() {
            return Ok(());
        }
        match self.replies.get(&request.client_id) {
            Some((seq, _)) if *seq > request.seq => return Ok(()),
            Some((seq, reply)) if *seq == request.seq => {
                if let Some(reply) = reply {
                    context
                        .downlink_net()
                        .send(request.client_addr, reply.clone())?
                }
                return Ok(());
            }
            _ => {}
        }
        if !self.is_primary() {
            context.peer_net().send(
                (self.view_num as usize % self.config.num_replica) as u8,
                request,
            )?;
            let view_num = self.view_num;
            self.do_view_change_timer.set(
                move || events::DoViewChange(view_num + 1),
                context.schedule(),
            )?;
            return Ok(());
        }
        self.replies.insert(request.client_id, (request.seq, None));
        self.requests.push(request);
        if self.op_num() <= self.commit_num + self.config.num_concurrent as u32 {
            self.close_batch(context)
        } else {
            Ok(())
        }
    }
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn close_batch(&mut self, context: &mut impl Context<Self, A>) -> anyhow::Result<()> {
        assert!(self.is_primary());
        assert!(!self.view_change());
        assert!(!self.requests.is_empty());
        let requests = self
            .requests
            .drain(..self.requests.len().min(self.config.max_batch_size))
            .collect::<Vec<_>>();
        let op_num = self.op_num();
        if self.log.get(op_num as usize).is_none() {
            self.log.resize(op_num as usize + 1, self.default_entry())
        }
        let view_num = self.view_num;
        context
            .crypto_worker()
            // not `submit_sign` here because I want to postpone digesting to worker
            .submit(Box::new(move |crypto, context| {
                let pre_prepare = PrePrepare {
                    view_num,
                    op_num,
                    digest: requests.sha256(),
                };
                context.send((Signed(crypto.sign(pre_prepare)), requests))
            }))
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<(Signed<PrePrepare>, Vec<Request<A>>), C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        (Signed(pre_prepare), requests): (Signed<PrePrepare>, Vec<Request<A>>),
        context: &mut C,
    ) -> anyhow::Result<()> {
        // println!("Signed {pre_prepare:?}");
        if pre_prepare.view_num != self.view_num {
            return Ok(());
        }

        let op_num = pre_prepare.op_num;
        let replaced = self.log[op_num as usize]
            .pre_prepare
            .replace(pre_prepare.clone());
        assert!(replaced.is_none());

        self.log[op_num as usize].requests.clone_from(&requests);
        self.log[op_num as usize]
            .progress_timer
            .set(move || events::ProgressPrepare(op_num), context.schedule())?;

        let digest = pre_prepare.digest;
        context.peer_net().send(All, (pre_prepare, requests))?;

        // TODO improve readability?
        if self.config.num_replica == 1 {
            let commit = Commit {
                view_num: self.view_num,
                op_num,
                digest,
                replica_id: self.id,
            };
            context
                .crypto_worker()
                .submit(Box::new(move |crypto, sender| {
                    sender.send(Signed(crypto.sign(commit)))
                }))?
        }
        Ok(())
    }
}

// it could be the case that when this is triggered primary has been moving on
// to a higher view i.e. collected majority of ViewChange and pending NewView
// should be safe since we are just resending old PrePrepare
// (if the old PrePrepare is gone (probably because of a view change), the timer
// should be gone along with it)
impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<events::ProgressPrepare, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        events::ProgressPrepare(op_num): events::ProgressPrepare,
        context: &mut C,
    ) -> anyhow::Result<()> {
        // warn!("progress prepared {op_num}");
        let entry = &self.log[op_num as usize];
        let pre_prepare = entry
            .pre_prepare
            .clone()
            .ok_or(anyhow::format_err!("missing PrePrepare {op_num}"))?;
        context
            .peer_net()
            .send(All, (pre_prepare, entry.requests.clone()))
    }
}

impl<S, A: Addr, C: Context<Self, A>>
    OnErasedEvent<Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>, C> for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        Recv((pre_prepare, requests)): Recv<(Verifiable<PrePrepare>, Vec<Request<A>>)>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num || self.view_change() {
            if pre_prepare.view_num >= self.view_num {
                let query_new_view = QueryNewView {
                    view_num: pre_prepare.view_num,
                    replica_id: self.id,
                };
                context.peer_net().send(
                    (pre_prepare.view_num as usize % self.config.num_replica) as u8,
                    query_new_view,
                )?
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

        // a decent implementation probably should throttle here (as what we have been done to
        // prepares and commits) in order to mitigate performance degradation caused by faulty
        // proposals
        // omitted since (again) that's only on slow path

        // TODO should reject op number over high watermark here
        let replica_id = pre_prepare.view_num as usize % self.config.num_replica;
        context
            .crypto_worker()
            .submit(Box::new(move |crypto, context| {
                if (requests.sha256() == pre_prepare.digest
                    || requests.is_empty() && pre_prepare.digest == NO_OP_DIGEST)
                    && crypto.verify(replica_id, &pre_prepare).is_ok()
                {
                    context.send((Verified(pre_prepare), requests))
                } else {
                    Ok(())
                }
            }))
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<(Verified<PrePrepare>, Vec<Request<A>>), C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        (Verified(pre_prepare), requests): (Verified<PrePrepare>, Vec<Request<A>>),
        context: &mut C,
    ) -> anyhow::Result<()> {
        if pre_prepare.view_num != self.view_num {
            return Ok(());
        }
        if self.log.get(pre_prepare.op_num as usize).is_none() {
            self.log
                .resize(pre_prepare.op_num as usize + 1, self.default_entry());
        }
        if let Some(prepared) = &self.log[pre_prepare.op_num as usize].pre_prepare {
            if **prepared != *pre_prepare {
                // println!("! PrePrepare not match the prepared one");
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
        context.submit_sign(prepare)?;

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

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Signed<Prepare>, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        Signed(prepare): Signed<Prepare>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if prepare.view_num != self.view_num {
            return Ok(());
        }
        context.peer_net().send(All, prepare.clone())?;
        if self.log[prepare.op_num as usize].prepares.is_empty() {
            self.insert_prepare(prepare, context)?
        }
        Ok(())
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Recv<Verifiable<Prepare>>, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        Recv(prepare): Recv<Verifiable<Prepare>>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        // ingress throttle: verifying one Prepare (and Commit below) at a time for each op number
        // different from PrePrepare: this is for fast path performance
        // this throttling ensures we only verify 2f + 1 Prepare/Commit for each slot, get rid of
        // unnecessary verification to maximize throughput (in case it has been bounded by crypto
        // overhead)
        if let Some(pending_prepares) = self.pending_prepares.get_mut(&prepare.op_num) {
            pending_prepares.push(prepare);
            return Ok(());
        }
        let op_num = prepare.op_num;
        if self.submit_prepare(prepare, context)? {
            // insert the dummy entry to indicate there's ongoing task
            self.pending_prepares.insert(op_num, Default::default());
        }
        Ok(())
    }
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn submit_prepare(
        &mut self,
        prepare: Verifiable<Prepare>,
        context: &mut impl Context<Self, A>,
    ) -> anyhow::Result<bool> {
        if prepare.view_num != self.view_num || self.view_change() {
            if prepare.view_num >= self.view_num {
                let query_new_view = QueryNewView {
                    view_num: prepare.view_num,
                    replica_id: self.id,
                };
                context
                    .peer_net()
                    .send(prepare.replica_id, query_new_view)?
            }
            return Ok(false);
        }
        if let Some(entry) = self.log.get(prepare.op_num as usize) {
            if !entry.prepares.is_empty() {
                // TODO resend Commit for the sender of the Prepare to ensure liveness
                // not just liveness on the sender, but the liveness of this slot; there may not
                // been enough Commit successfully collected by anyone yet
                // in such case primary will keep resending PrePrepare which results in the
                // resending of Prepare, and we need to turn that into Commit in case everyone
                // already collected enough Prepare
                // yet another solution is to do nothing and rely on view change, but let's avoid
                // that as long as primary is still around
                return Ok(false);
            }
            if let Some(pre_prepare) = &entry.pre_prepare {
                if prepare.digest != pre_prepare.digest {
                    return Ok(false);
                }
            }
        }
        context
            .crypto_worker()
            .submit(Box::new(move |crypto, context| {
                if crypto.verify(prepare.replica_id, &prepare).is_ok() {
                    context.send(Verified(prepare))
                } else {
                    Ok(())
                }
            }))?;
        Ok(true)
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Verified<Prepare>, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        Verified(prepare): Verified<Prepare>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        let op_num = prepare.op_num;
        if prepare.view_num != self.view_num || !self.pending_prepares.contains_key(&op_num) {
            return Ok(());
        }
        self.insert_prepare(prepare, context)?;
        loop {
            let Some(pending_prepares) = self.pending_prepares.get_mut(&op_num) else {
                break;
            };
            let Some(prepare) = pending_prepares.pop() else {
                // there's no pending task, remove the task list to indicate
                self.pending_prepares.remove(&op_num);
                break;
            };
            if self.submit_prepare(prepare, context)? {
                break;
            }
        }
        Ok(())
    }
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn insert_prepare(
        &mut self,
        prepare: Verifiable<Prepare>,
        context: &mut impl Context<Self, A>,
    ) -> anyhow::Result<()> {
        let prepare_quorum = self.prepare_quorums.entry(prepare.op_num).or_default();
        prepare_quorum.insert(prepare.replica_id, prepare.clone());
        // println!(
        //     "{} PrePrepare {} Prepare {}",
        //     prepare.op_num,
        //     self.log.get(prepare.op_num as usize).is_some(),
        //     prepare_quorum.len()
        // );
        if prepare_quorum.len() + 1 < self.config.num_replica - self.config.num_faulty {
            return Ok(());
        }
        let Some(entry) = self.log.get_mut(prepare.op_num as usize) else {
            return Ok(());
        };
        if entry.pre_prepare.is_none() {
            // postpone prepared until receiving PrePrepare and matching digest against it
            return Ok(());
        }
        assert!(entry.prepares.is_empty());
        entry.prepares = self.prepare_quorums.remove(&prepare.op_num).unwrap();
        self.pending_prepares.remove(&prepare.op_num);

        let commit = Commit {
            view_num: self.view_num,
            op_num: prepare.op_num,
            digest: prepare.digest,
            replica_id: self.id,
        };
        context.submit_sign(commit)
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Signed<Commit>, C> for ReplicaState<S, A> {
    fn on_event(&mut self, Signed(commit): Signed<Commit>, context: &mut C) -> anyhow::Result<()> {
        if commit.view_num != self.view_num {
            return Ok(());
        }
        context.peer_net().send(All, commit.clone())?;
        if self.log[commit.op_num as usize].commits.is_empty() {
            self.insert_commit(commit, context)?
        }
        Ok(())
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Recv<Verifiable<Commit>>, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        Recv(commit): Recv<Verifiable<Commit>>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if let Some(pending_commits) = self.pending_commits.get_mut(&commit.op_num) {
            pending_commits.push(commit);
            return Ok(());
        }
        let op_num = commit.op_num;
        if self.submit_commit(commit, context)? {
            // insert the dummy entry to indicate there's ongoing task
            self.pending_commits.insert(op_num, Default::default());
        }
        Ok(())
    }
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn submit_commit(
        &mut self,
        commit: Verifiable<Commit>,
        context: &mut impl Context<Self, A>,
    ) -> anyhow::Result<bool> {
        if commit.view_num != self.view_num || self.view_change() {
            if commit.view_num >= self.view_num {
                let query_new_view = QueryNewView {
                    view_num: commit.view_num,
                    replica_id: self.id,
                };
                context.peer_net().send(commit.replica_id, query_new_view)?
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
        context
            .crypto_worker()
            .submit(Box::new(move |crypto, context| {
                if crypto.verify(commit.replica_id, &commit).is_ok() {
                    context.send(Verified(commit))
                } else {
                    Ok(())
                }
            }))?;
        Ok(true)
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Verified<Commit>, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        Verified(commit): Verified<Commit>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        let op_num = commit.op_num;
        if commit.view_num != self.view_num || !self.pending_commits.contains_key(&op_num) {
            return Ok(());
        }
        self.insert_commit(commit, context)?;
        loop {
            let Some(pending_commits) = self.pending_commits.get_mut(&op_num) else {
                break;
            };
            let Some(commit) = pending_commits.pop() else {
                // there's no pending task, remove the task list to indicate
                self.pending_commits.remove(&op_num);
                break;
            };
            if self.submit_commit(commit, context)? {
                break;
            }
        }
        Ok(())
    }
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn insert_commit(
        &mut self,
        commit: Verifiable<Commit>,
        context: &mut impl Context<Self, A>,
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

        if commit_quorum.len() < self.config.num_replica - self.config.num_faulty {
            return Ok(());
        }
        let is_primary = self.is_primary();
        let Some(log_entry) = self.log.get_mut(commit.op_num as usize) else {
            return Ok(());
        };
        assert!(log_entry.commits.is_empty());
        if log_entry.prepares.is_empty() && self.config.num_replica != 1 {
            return Ok(()); // shortcut: probably safe to commit as well
        }

        log_entry.commits = self.commit_quorums.remove(&commit.op_num).unwrap();
        self.pending_commits.remove(&commit.op_num);
        // println!("[{}] Commit {}", self.id, commit.op_num);
        if is_primary {
            log_entry.progress_timer.unset(context.schedule())?;
        } else {
            self.do_view_change_timer.ensure_unset(context.schedule())?;
        }

        while let Some(log_entry) = self.log.get_mut(self.commit_num as usize + 1) {
            if log_entry.commits.is_empty() {
                break;
            }
            let pre_prepare = log_entry.pre_prepare.as_ref().unwrap();
            if pre_prepare.digest != NO_OP_DIGEST && log_entry.requests.is_empty() {
                break;
            }
            self.commit_num += 1;
            // println!("[{}] Execute {}", self.id, self.commit_num);
            log_entry
                .state_transfer_timer
                .ensure_unset(context.schedule())?;

            for request in &log_entry.requests {
                // println!("Execute {request:?}");
                let reply = Reply {
                    seq: request.seq,
                    // result: Payload(self.app.execute(&request.op)?),
                    result: Payload(Default::default()),
                    view_num: pre_prepare.view_num,
                    replica_id: self.id,
                };
                // this replica can be very late on executing the request i.e. client already
                // collect enough replies from other replicas, move on to the following request, and
                // the later request has been captured by `replies`, so not assert anything
                if self
                    .replies
                    .get(&request.client_id)
                    .map(|(seq, _)| *seq <= request.seq)
                    .unwrap_or(true)
                {
                    self.replies
                        .insert(request.client_id, (request.seq, Some(reply.clone())));
                }
                context
                    .downlink_net()
                    .send(request.client_addr.clone(), reply)?
            }
        }

        if self.is_primary() {
            while !self.requests.is_empty()
                && self.op_num() <= self.commit_num + self.config.num_concurrent as u32
            {
                self.close_batch(context)?
            }
        } else if commit.op_num > self.commit_num {
            for op_num in self.commit_num + 1..=commit.op_num {
                self.log[op_num as usize]
                    .state_transfer_timer
                    .ensure_set(move || events::StateTransfer(op_num), context.schedule())?
            }
        }
        Ok(())
    }
}

impl<S, A, C: Context<Self, A>> OnErasedEvent<events::StateTransfer, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        events::StateTransfer(op_num): events::StateTransfer,
        _: &mut C,
    ) -> anyhow::Result<()> {
        // TODO
        anyhow::bail!("{:?} timeout", events::StateTransfer(op_num))
    }
}

impl<S, A, C: Context<Self, A>> OnErasedEvent<Recv<QueryNewView>, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        Recv(query_new_view): Recv<QueryNewView>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if let Some(new_view) = self.new_views.get(&query_new_view.view_num) {
            context
                .peer_net()
                .send(query_new_view.replica_id, new_view.clone())?
        }
        Ok(())
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<events::DoViewChange, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        events::DoViewChange(view_num): events::DoViewChange,
        context: &mut C,
    ) -> anyhow::Result<()> {
        // warn!("[{}] do view change for view {view_num}", self.id);
        assert!(view_num >= self.view_num);
        self.view_num = view_num;
        // let DoViewChange(also_view_num) =
        self.do_view_change_timer.unset(context.schedule())?;
        // anyhow::ensure!(also_view_num == view_num);
        self.progress_view_change_timer
            .ensure_set(|| events::ProgressViewChange, context.schedule())?;
        // self.progress_view_change_timer.reset(timer)?; // not really necessary just feels more correct :)
        self.do_view_change(context)
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<events::ProgressViewChange, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        events::ProgressViewChange: events::ProgressViewChange,
        context: &mut C,
    ) -> anyhow::Result<()> {
        self.do_view_change(context)
    }
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn do_view_change(&mut self, context: &mut impl Context<Self, A>) -> Result<(), anyhow::Error> {
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
            view_num: self.view_num,
            log,
            replica_id: self.id,
        };
        context.submit_sign(view_change)
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Signed<ViewChange>, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        Signed(view_change): Signed<ViewChange>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if view_change.view_num == self.view_num {
            context.peer_net().send(All, view_change.clone())?;
            self.insert_view_change(view_change, context)?
        }
        Ok(())
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
        anyhow::ensure!(prepares.len() + 1 >= num_replica - num_faulty);
        crypto.verify(pre_prepare.view_num as usize % num_replica, pre_prepare)?;
        for prepare in prepares.values() {
            anyhow::ensure!(prepare.digest == pre_prepare.digest);
            crypto.verify(prepare.replica_id, prepare)?
        }
    }
    Ok(())
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Recv<Verifiable<ViewChange>>, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        Recv(view_change): Recv<Verifiable<ViewChange>>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if view_change.view_num < self.view_num {
            return Ok(());
        }
        let num_replica = self.config.num_replica;
        let num_faulty = self.config.num_faulty;
        context
            .crypto_worker()
            .submit(Box::new(move |crypto, context| {
                if verify_view_change(crypto, &view_change, num_replica, num_faulty).is_ok() {
                    context.send(Verified(view_change))
                } else {
                    Ok(())
                }
            }))
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Verified<ViewChange>, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        Verified(view_change): Verified<ViewChange>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        self.insert_view_change(view_change, context)
    }
}

fn pre_prepares_for_view_changes(
    view_num: u32,
    view_changes: &Quorum<ViewChange>,
) -> anyhow::Result<Vec<PrePrepare>> {
    let mut carried_pre_prepares = BTreeMap::new();
    for view_change in view_changes.values() {
        for (prepared, _) in &view_change.log {
            let pre_prepare = carried_pre_prepares
                .entry(prepared.op_num)
                .or_insert_with(|| PrePrepare {
                    view_num,
                    op_num: prepared.op_num,
                    digest: prepared.digest,
                });
            anyhow::ensure!(
                pre_prepare.digest == prepared.digest,
                "prepared invariant violated"
            )
        }
    }
    let mut carried_pre_prepares = carried_pre_prepares.into_values();
    let mut pre_prepares = carried_pre_prepares.next().into_iter().collect::<Vec<_>>();
    for pre_prepare in carried_pre_prepares {
        let last_op = pre_prepares.last().as_ref().unwrap().op_num;
        assert!(last_op <= pre_prepare.op_num, "BTreeMap should be ordered");
        pre_prepares.extend((last_op..pre_prepare.op_num).map(|op_num| PrePrepare {
            view_num,
            op_num,
            digest: NO_OP_DIGEST,
        }));
        pre_prepares.push(pre_prepare)
    }
    if pre_prepares.is_empty() {
        pre_prepares.push(PrePrepare {
            view_num,
            // it's always 1 for now, should be decided from checkpoint positions of `ViewChange`s
            op_num: 1,
            digest: NO_OP_DIGEST,
        })
    }
    Ok(pre_prepares)
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn have_entered(&self, view_num: u32) -> bool {
        self.view_num > view_num || self.view_num == view_num && !self.view_change()
    }

    fn insert_view_change(
        &mut self,
        view_change: Verifiable<ViewChange>,
        context: &mut impl Context<Self, A>,
    ) -> anyhow::Result<()> {
        if self.have_entered(view_change.view_num) {
            return Ok(());
        }
        let view_change_quorum = self.view_changes.entry(view_change.view_num).or_default();
        if view_change_quorum.len() >= self.config.num_replica - self.config.num_faulty {
            return Ok(());
        }

        view_change_quorum.insert(view_change.replica_id, view_change.clone());
        // println!(
        //     "[{}] view {} ViewChange {}",
        //     self.id,
        //     view_change.view_num,
        //     quorum.len()
        // );
        if view_change_quorum.len() == self.config.num_replica - self.config.num_faulty {
            // it is possible that i'm working on view change into view v while collecting a
            // majority that working on view change into view v' > v
            self.view_num = view_change.view_num;
            let view_changes = view_change_quorum.clone();
            if self.is_primary() {
                let view_num = self.view_num;
                context
                    .crypto_worker()
                    // not `submit_sign` here for postponing generating PrePrepare to worker
                    .submit(Box::new(move |crypto, context| {
                        let new_view = NewView {
                            view_num,
                            pre_prepares: pre_prepares_for_view_changes(view_num, &view_changes)?
                                .into_iter()
                                .map(|pre_prepare| crypto.sign(pre_prepare))
                                .collect(),
                            view_changes,
                        };
                        context.send(Signed(crypto.sign(new_view)))
                    }))?
            } else {
                let view_num = self.view_num;
                self.do_view_change_timer.ensure_set(
                    move || events::DoViewChange(view_num + 1),
                    context.schedule(),
                )?
            }
        }
        // TODO "shortcut" sending ViewChange of view v after collecting f + 1 ViewChange of view
        // >= v, as described in 4.5.2 Liveness "Second, ..."
        Ok(())
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Signed<NewView>, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        Signed(new_view): Signed<NewView>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if self.view_num == new_view.view_num {
            context.peer_net().send(All, new_view.clone())?;
            self.enter_view(new_view, context)?
        }
        Ok(())
    }
}

impl<S, A: Addr> ReplicaState<S, A> {
    fn enter_view(
        &mut self,
        new_view: Verifiable<NewView>,
        context: &mut impl Context<Self, A>,
    ) -> anyhow::Result<()> {
        assert!(!self.have_entered(new_view.view_num));
        self.view_num = new_view.view_num;
        assert!(self.view_change());
        for pre_prepare in &new_view.pre_prepares {
            // somehow duplicating `impl OnErasedEvent<(Verified<PrePrepare>, Vec<Request<M::A>>)>`
            // maybe just perform necessary clean up then redirect to there
            if self.log.get(pre_prepare.op_num as usize).is_none() {
                self.log
                    .resize(pre_prepare.op_num as usize + 1, self.default_entry())
            }
            let is_primary = self.is_primary();
            let log_entry = &mut self.log[pre_prepare.op_num as usize];
            if let Some(prev_pre_prepare) = &mut log_entry.pre_prepare {
                if prev_pre_prepare.digest != pre_prepare.digest {
                    log_entry.requests.clear()
                }
            }
            log_entry.pre_prepare = Some(pre_prepare.clone());
            log_entry.prepares.clear();
            log_entry.commits.clear();
            // i don't know whether this is possible on primary, maybe the view change happens to
            // rotate back to the original primary? = =
            // just get ready for anything weird that may (i.e. will) happen during model checking
            log_entry.progress_timer.ensure_unset(context.schedule())?;
            if is_primary {
                let op_num = pre_prepare.op_num;
                log_entry
                    .progress_timer
                    .set(move || events::ProgressPrepare(op_num), context.schedule())?
            } else {
                // println!("[{}] Redo Prepare {}", self.id, pre_prepare.op_num);
                let prepare = Prepare {
                    view_num: self.view_num,
                    op_num: pre_prepare.op_num,
                    digest: pre_prepare.digest,
                    replica_id: self.id,
                };
                context
                    .crypto_worker()
                    .submit(Box::new(move |crypto, context| {
                        context.send(Signed(crypto.sign(prepare)))
                    }))?
            }
        }
        let op_num = new_view.pre_prepares.last().as_ref().unwrap().op_num as usize + 1;
        assert!(self.op_num() >= op_num as u32);
        if self.op_num() > op_num as u32 {
            for mut log_entry in self.log.drain(op_num..) {
                log_entry.progress_timer.ensure_unset(context.schedule())?;
            }
        }
        // consider `drain(..)` on these?
        self.requests.clear();
        self.prepare_quorums.clear();
        self.commit_quorums.clear();
        self.do_view_change_timer.ensure_unset(context.schedule())?;
        self.progress_view_change_timer
            .ensure_unset(context.schedule())?;
        self.view_changes = self.view_changes.split_off(&(self.view_num + 1));

        self.new_views.insert(self.view_num, new_view);
        Ok(())
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Recv<Verifiable<NewView>>, C>
    for ReplicaState<S, A>
{
    fn on_event(
        &mut self,
        Recv(new_view): Recv<Verifiable<NewView>>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if self.have_entered(new_view.view_num) {
            return Ok(());
        }
        let num_replica = self.config.num_replica;
        let num_faulty = self.config.num_faulty;
        context
            .crypto_worker()
            .submit(Box::new(move |crypto, context| {
                let do_verify = || {
                    let index = new_view.view_num as usize % num_replica;
                    crypto.verify(index, &new_view)?;
                    anyhow::ensure!(new_view.view_changes.len() >= num_replica - num_faulty);
                    for view_change in new_view.view_changes.values() {
                        verify_view_change(crypto, view_change, num_replica, num_faulty)?
                    }
                    for (pre_prepare, expected_pre_prepare) in
                        new_view
                            .pre_prepares
                            .iter()
                            .zip(pre_prepares_for_view_changes(
                                new_view.view_num,
                                &new_view.view_changes,
                            )?)
                    {
                        anyhow::ensure!(**pre_prepare == expected_pre_prepare);
                        crypto.verify(index, pre_prepare)?;
                    }
                    anyhow::Ok(())
                };
                if do_verify().is_ok() {
                    context.send(Verified(new_view))?
                }
                Ok(())
            }))
    }
}

impl<S, A: Addr, C: Context<Self, A>> OnErasedEvent<Verified<NewView>, C> for ReplicaState<S, A> {
    fn on_event(
        &mut self,
        Verified(new_view): Verified<NewView>,
        context: &mut C,
    ) -> anyhow::Result<()> {
        if !self.have_entered(new_view.view_num) {
            self.enter_view(new_view, context)?
        }
        Ok(())
    }
}

pub mod context {
    use super::*;

    pub struct Context<PN, DN, CW: CryptoWorkerOn<Self>, T: ScheduleOn<Self>> {
        pub peer_net: PN,
        pub downlink_net: DN,
        pub crypto_worker: CW::Out,
        pub schedule: T::Out,
    }

    pub trait CryptoWorkerOn<C> {
        type Out: Submit<Crypto, Self::Context>;
        type Context;
    }

    pub trait ScheduleOn<C> {
        type Out: ScheduleEvent<events::DoViewChange>
            + ScheduleEvent<events::ProgressPrepare>
            + ScheduleEvent<events::ProgressViewChange>
            + ScheduleEvent<events::StateTransfer>;
    }

    impl<PN, DN, CW: CryptoWorkerOn<Self>, T: ScheduleOn<Self>, S, A> super::Context<S, A>
        for Context<PN, DN, CW, T>
    where
        PN: SendMessage<u8, Request<A>>
            + SendMessage<All, (Verifiable<PrePrepare>, Vec<Request<A>>)>
            + SendMessage<All, Verifiable<Prepare>>
            + SendMessage<All, Verifiable<Commit>>
            + SendMessage<All, Verifiable<ViewChange>>
            + SendMessage<All, Verifiable<NewView>>
            + SendMessage<u8, QueryNewView>
            + SendMessage<u8, Verifiable<NewView>>,
        DN: SendMessage<A, Reply>,
        CW::Context: work::Upcall<S, Self>,
    {
        type PeerNet = PN;
        type DownlinkNet = DN;
        type CryptoWorker = CW::Out;
        type CryptoContext = CW::Context;
        type Schedule = T::Out;
        fn peer_net(&mut self) -> &mut Self::PeerNet {
            &mut self.peer_net
        }
        fn downlink_net(&mut self) -> &mut Self::DownlinkNet {
            &mut self.downlink_net
        }
        fn crypto_worker(&mut self) -> &mut Self::CryptoWorker {
            &mut self.crypto_worker
        }
        fn schedule(&mut self) -> &mut Self::Schedule {
            &mut self.schedule
        }
    }
}
