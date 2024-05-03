// notice: `App`-specific `impl Workload`s are in `app` module
// only `App`-agnostic combinator lives here
// maybe not the most reasonable organization but makes enough sense to me

use std::{
    collections::VecDeque,
    fmt::Debug,
    mem::replace,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};

use derive_where::derive_where;
use rand::Rng;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    event::{
        erased::{events::Init, OnEvent},
        BlackHole, OnTimer, SendEvent, SendEventOnce, Timer,
    },
    net::Payload,
};

pub trait Workload {
    type Op;
    type OpContext;
    type Result;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>>;

    fn on_result(&mut self, result: Self::Result, context: Self::OpContext) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct Iter<I, O = Payload, R = Payload>(I, std::marker::PhantomData<(O, R)>);

impl<I, O, R> From<I> for Iter<I, O, R> {
    fn from(value: I) -> Self {
        Self(value, Default::default())
    }
}

impl<T: Iterator<Item = O>, O, R> Workload for Iter<T, O, R> {
    type Op = O;
    type OpContext = ();
    type Result = R;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>> {
        Ok(self.0.next().map(|op| (op, ())))
    }

    fn on_result(&mut self, _: Self::Result, (): Self::OpContext) -> anyhow::Result<()> {
        Ok(())
    }
}

// coupling workload generation and latency measurement may not be a good design
// generally speaking, there should be a concept of "transaction" that composed from one or more
// ops, and latency is mean to be measured against transactions
// currently the transaction concept is skipped, maybe revisit the design later
#[derive(Debug, derive_more::Deref, derive_more::AsMut)]
pub struct OpLatency<W> {
    #[deref]
    inner: W,
    #[as_mut]
    pub latencies: Vec<Duration>,
}

impl<W> OpLatency<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            latencies: Default::default(),
        }
    }
}

impl<W: Workload> Workload for OpLatency<W> {
    type Op = W::Op;
    type OpContext = (Instant, W::OpContext);
    type Result = W::Result;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>> {
        let Some((op, attach)) = self.inner.next_op()? else {
            return Ok(None);
        };
        Ok(Some((op, (Instant::now(), attach))))
    }

    fn on_result(
        &mut self,
        result: Self::Result,
        (start, context): Self::OpContext,
    ) -> anyhow::Result<()> {
        self.latencies.push(start.elapsed());
        self.inner.on_result(result, context)
    }
}

#[derive(Debug, Clone, derive_more::Deref)]
pub struct Recorded<W: Workload> {
    #[deref]
    inner: W,
    pub invocations: Vec<(W::Op, W::Result)>,
}

impl<W: Workload> From<W> for Recorded<W> {
    fn from(value: W) -> Self {
        Self {
            inner: value,
            invocations: Default::default(),
        }
    }
}

impl<W: Workload> Workload for Recorded<W>
where
    W::Op: Clone,
    W::Result: Clone,
{
    type Op = W::Op;
    type OpContext = (W::Op, W::OpContext);
    type Result = W::Result;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>> {
        Ok(self
            .inner
            .next_op()?
            .map(|(op, attach)| (op.clone(), (op, attach))))
    }

    fn on_result(
        &mut self,
        result: Self::Result,
        (op, context): Self::OpContext,
    ) -> anyhow::Result<()> {
        self.invocations.push((op, result.clone()));
        self.inner.on_result(result, context)
    }
}

#[derive(Debug, Clone)]
pub struct Total<W> {
    inner: W,
    remain_count: Arc<AtomicU32>,
}

impl<W> Total<W> {
    pub fn new(inner: W, count: u32) -> Self {
        Self {
            inner,
            remain_count: Arc::new(AtomicU32::new(count)),
        }
    }
}

impl<W: Workload> Workload for Total<W> {
    type Op = W::Op;
    type OpContext = W::OpContext;
    type Result = W::Result;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>> {
        let mut remain_count = self.remain_count.load(SeqCst);
        loop {
            if remain_count == 0 {
                return Ok(None);
            }
            match self.remain_count.compare_exchange_weak(
                remain_count,
                remain_count - 1,
                SeqCst,
                SeqCst,
            ) {
                Ok(_) => break,
                Err(count) => remain_count = count,
            }
        }
        self.inner.next_op()
    }

    fn on_result(&mut self, result: Self::Result, context: Self::OpContext) -> anyhow::Result<()> {
        self.inner.on_result(result, context)
    }
}

// this one is not fully composable i.e. cannot wrap any other workload
// i guess that's reasonable since you probably want to control the ops because
// only so you can assert on the results
#[derive(Debug, Clone)]
pub struct Check<I, O, R> {
    inner: I,
    _m: std::marker::PhantomData<(O, R)>,
}

impl<I, O, R> Check<I, O, R> {
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            _m: Default::default(),
        }
    }
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
#[display(bound = "R: Debug")]
#[display(fmt = "{self:?}")]
pub struct UnexpectedResult<R> {
    pub expect: R,
    pub actual: R,
}

impl<I: Iterator<Item = (O, R)>, O, R: Debug + Eq + Send + Sync + 'static> Workload
    for Check<I, O, R>
{
    type Op = O;
    type OpContext = R;
    type Result = R;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>> {
        let Some((op, expected_result)) = self.inner.next() else {
            return Ok(None);
        };
        Ok(Some((op, expected_result)))
    }

    fn on_result(
        &mut self,
        result: Self::Result,
        expected_result: Self::OpContext,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            result == expected_result,
            UnexpectedResult {
                expect: expected_result,
                actual: result
            }
        );
        Ok(())
    }
}

#[derive(Debug, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct Json<W>(pub W);

impl<W: AsMut<T>, T> AsMut<T> for Json<W> {
    fn as_mut(&mut self) -> &mut T {
        self.0.as_mut()
    }
}

impl<W: Workload> Workload for Json<W>
where
    W::Op: Serialize,
    W::Result: DeserializeOwned,
{
    type Op = Payload;
    type OpContext = W::OpContext;
    type Result = Payload;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>> {
        let Some((op, attach)) = self.0.next_op()? else {
            return Ok(None);
        };
        Ok(Some((Payload(serde_json::to_vec(&op)?), attach)))
    }

    fn on_result(&mut self, result: Self::Result, context: Self::OpContext) -> anyhow::Result<()> {
        self.0.on_result(serde_json::from_slice(&result)?, context)
    }
}

pub struct Weighted2<W0, W1, R>(pub W0, pub W1, pub R, pub f64);

impl<
        W0: Workload,
        W1: Workload<Op = W0::Op, OpContext = W0::OpContext, Result = W0::Result>,
        R: Rng,
    > Workload for Weighted2<W0, W1, R>
{
    type Op = W0::Op;
    type OpContext = (bool, W0::OpContext);
    type Result = W0::Result;

    fn next_op(&mut self) -> anyhow::Result<Option<(Self::Op, Self::OpContext)>> {
        let choose0 = self.2.gen_bool(self.3);
        let Some((op, context)) = if choose0 {
            self.0.next_op()
        } else {
            self.1.next_op()
        }?
        else {
            // simply returning `Ok(None)` here is not a good manner, as the unselected workload may
            // still have op yet to produce, which may cause the following `next_op` call to return
            // `Some` op
            // certain combinator and close loop relies on the fused property, so we'd better to not
            // break it (though don't have a good idea on how to design then for now)
            anyhow::bail!("unimplemented")
        };
        Ok(Some((op, (choose0, context))))
    }

    fn on_result(
        &mut self,
        result: Self::Result,
        (choose0, context): Self::OpContext,
    ) -> anyhow::Result<()> {
        if choose0 {
            self.0.on_result(result, context)
        } else {
            self.1.on_result(result, context)
        }
    }
}

pub mod events {
    use crate::net::Payload;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Invoke<O = Payload>(pub O);

    // newtype namespace may be desired after the type erasure migration
    // the `u32` field was for client id, and becomes unused after remove multiple
    // client support on `CloseLoop`
    // too lazy to refactor it off
    pub type InvokeOk<R = Payload> = (u32, R);

    pub struct Stop;
}

pub trait Upcall<R = Payload>: SendEvent<events::InvokeOk<R>> {}
impl<T: SendEvent<events::InvokeOk<R>>, R> Upcall<R> for T {}

#[derive(Debug, Clone)]
#[derive_where(PartialEq, Eq, Hash; C, E, SE)]
pub struct CloseLoop<W, C, E, SE = BlackHole> {
    pub sender: E,
    // don't consider `workload` state for comparing, this is aligned with how DSLabs also ignores
    // workload in its `ClientWorker`
    // still feels we are risking missing states where the system goes back to an identical previous
    // state after consuming one item from workload, but anyway i'm not making it worse
    #[derive_where[skip]]
    pub workload: W,
    op_context: Option<C>,
    pub stop_sender: Option<SE>,
    pub done: bool,
}

impl<W, C, E> CloseLoop<W, C, E> {
    pub fn new(sender: E, workload: W) -> Self {
        Self {
            sender,
            workload,
            op_context: None,
            stop_sender: Some(BlackHole),
            done: false,
        }
    }
}

impl<W: Workload, E: SendEvent<events::Invoke<W::Op>>, SE> OnEvent<Init>
    for CloseLoop<W, W::OpContext, E, SE>
{
    fn on_event(&mut self, Init: Init, _: &mut impl Timer) -> anyhow::Result<()> {
        let (op, attach) = self
            .workload
            .next_op()?
            .ok_or(anyhow::format_err!("not enough op"))?;
        let replaced = self.op_context.replace(attach);
        anyhow::ensure!(replaced.is_none(), "duplicated launching");
        self.sender.send(events::Invoke(op))
    }
}

impl<W: Workload, E: SendEvent<events::Invoke<W::Op>>, SE: SendEventOnce<events::Stop>>
    OnEvent<events::InvokeOk<W::Result>> for CloseLoop<W, W::OpContext, E, SE>
{
    fn on_event(
        &mut self,
        (_, result): events::InvokeOk<W::Result>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        let Some(attach) = self.op_context.take() else {
            anyhow::bail!("missing workload attach")
        };
        self.workload.on_result(result, attach)?;
        if let Some((op, attach)) = self.workload.next_op()? {
            self.op_context.replace(attach);
            self.sender.send(events::Invoke(op))
        } else {
            let replaced = replace(&mut self.done, true);
            assert!(!replaced);
            self.stop_sender.send(events::Stop)
        }
    }
}

impl<W: Workload, C, E, SE> OnTimer for CloseLoop<W, C, E, SE> {
    fn on_timer(&mut self, _: crate::event::TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        unreachable!()
    }
}

pub struct Queue<E, O> {
    sender: E,
    ops: Option<VecDeque<O>>,
}

impl<E, O> Queue<E, O> {
    pub fn new(sender: E) -> Self {
        Self {
            sender,
            ops: Default::default(),
        }
    }
}

impl<E: SendEvent<events::Invoke<O>>, O> OnEvent<events::Invoke<O>> for Queue<E, O> {
    fn on_event(
        &mut self,
        events::Invoke(op): events::Invoke<O>,
        _: &mut impl Timer,
    ) -> anyhow::Result<()> {
        if let Some(ops) = &mut self.ops {
            ops.push_back(op);
            Ok(())
        } else {
            self.ops = Some(Default::default());
            self.sender.send(events::Invoke(op))
        }
    }
}

impl<E: SendEvent<events::Invoke<O>>, O, R> OnEvent<events::InvokeOk<R>> for Queue<E, O> {
    fn on_event(&mut self, _: events::InvokeOk<R>, _: &mut impl Timer) -> anyhow::Result<()> {
        if let Some(ops) = &mut self.ops {
            if let Some(op) = ops.pop_front() {
                self.sender.send(events::Invoke(op))?
            } else {
                self.ops = None
            }
        }
        Ok(())
    }
}

impl<E, O> OnTimer for Queue<E, O> {
    fn on_timer(&mut self, _: crate::event::TimerId, _: &mut impl Timer) -> anyhow::Result<()> {
        unreachable!()
    }
}
