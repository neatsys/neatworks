// notice: `App`-specific `impl Workload`s are in `app` module
// only `App`-agnostic combinators live here
// maybe not the most reasonable organization but makes enough sense to me

use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::{
    event::{
        erased::{OnEvent, Timer},
        SendEvent,
    },
    message::Payload,
};

pub trait Workload {
    type Dry;

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>>;

    fn on_result(&mut self, result: Payload) -> anyhow::Result<()>;

    fn dehydrate(self) -> Self::Dry;
}

impl<T: Iterator<Item = Payload>> Workload for T {
    type Dry = ();

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
        Ok(self.next())
    }

    fn on_result(&mut self, _: Payload) -> anyhow::Result<()> {
        Ok(())
    }

    fn dehydrate(self) -> Self::Dry {}
}

#[derive(Debug, derive_more::Deref)]
pub struct OpLatency<W> {
    #[deref]
    inner: W,
    pub latencies: Vec<Duration>,
    start: Option<Instant>,
}

impl<W> OpLatency<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            latencies: Default::default(),
            start: None,
        }
    }
}

impl<W: Workload> Workload for OpLatency<W> {
    type Dry = Infallible;

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
        let Some(op) = self.inner.next_op()? else {
            return Ok(None);
        };
        let replaced = self.start.replace(Instant::now());
        if replaced.is_some() {
            anyhow::bail!("last invocation not finished")
        }
        Ok(Some(op))
    }

    fn on_result(&mut self, result: Payload) -> anyhow::Result<()> {
        let Some(start) = self.start.take() else {
            anyhow::bail!("missing invocation")
        };
        self.latencies.push(start.elapsed());
        self.inner.on_result(result)
    }

    fn dehydrate(self) -> Self::Dry {
        unimplemented!()
    }
}

pub trait WorkloadWithSavedOp {
    type Dry;

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>>;

    fn on_result(&mut self, op: Payload, result: Payload) -> anyhow::Result<()>;

    fn dehydrate(self) -> Self::Dry;
}

impl<T: Workload> WorkloadWithSavedOp for T {
    type Dry = T::Dry;

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
        Workload::next_op(self)
    }

    fn on_result(&mut self, _: Payload, result: Payload) -> anyhow::Result<()> {
        Workload::on_result(self, result)
    }

    fn dehydrate(self) -> Self::Dry {
        Workload::dehydrate(self)
    }
}

#[derive(Debug, derive_more::Deref)]
pub struct SaveOp<W> {
    #[deref]
    inner: W,
    op: Option<Payload>,
}

impl<W: WorkloadWithSavedOp> Workload for SaveOp<W> {
    type Dry = W::Dry;

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
        let Some(op) = self.inner.next_op()? else {
            return Ok(None);
        };
        let replaced = self.op.replace(op.clone());
        if replaced.is_some() {
            anyhow::bail!("last invocation not finished")
        }
        Ok(Some(op))
    }

    fn on_result(&mut self, result: Payload) -> anyhow::Result<()> {
        let Some(op) = self.op.take() else {
            anyhow::bail!("missing invocation")
        };
        self.inner.on_result(op, result)
    }

    fn dehydrate(self) -> Self::Dry {
        self.inner.dehydrate()
    }
}

#[derive(Debug, derive_more::Deref)]
pub struct Recorded<W> {
    #[deref]
    inner: W,
    pub invocations: Vec<(Payload, Payload)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DryRecorded<T> {
    inner: T,
    pub invocations: Vec<(Payload, Payload)>,
}

impl<W: Workload> WorkloadWithSavedOp for Recorded<W> {
    type Dry = DryRecorded<W::Dry>;

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
        self.inner.next_op()
    }

    fn on_result(&mut self, op: Payload, result: Payload) -> anyhow::Result<()> {
        self.invocations.push((op, result.clone()));
        self.inner.on_result(result)
    }

    fn dehydrate(self) -> Self::Dry {
        DryRecorded {
            inner: self.inner.dehydrate(),
            invocations: self.invocations,
        }
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
    type Dry = W::Dry;

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
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

    fn on_result(&mut self, result: Payload) -> anyhow::Result<()> {
        self.inner.on_result(result)
    }

    fn dehydrate(self) -> Self::Dry {
        self.inner.dehydrate()
    }
}

#[derive(Debug, Clone)]
pub struct Check<I> {
    inner: I,
    expected_result: Option<Payload>,
}

impl<I> Check<I> {
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            expected_result: None,
        }
    }
}

#[derive(Debug)]
pub struct UnexpectedResult {
    pub expect: Payload,
    pub actual: Payload,
}

impl Display for UnexpectedResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for UnexpectedResult {}

impl<I: Iterator<Item = (Payload, Payload)>> Workload for Check<I> {
    type Dry = ();

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
        let Some((op, expected_result)) = self.inner.next() else {
            return Ok(None);
        };
        let replaced = self.expected_result.replace(expected_result);
        if replaced.is_some() {
            anyhow::bail!("last invocation not finished")
        }
        Ok(Some(op))
    }

    fn on_result(&mut self, result: Payload) -> anyhow::Result<()> {
        let Some(expected_result) = self.expected_result.take() else {
            anyhow::bail!("missing invocation")
        };
        if result == expected_result {
            Ok(())
        } else {
            Err(UnexpectedResult {
                expect: expected_result,
                actual: result,
            })?
        }
    }

    fn dehydrate(self) -> Self::Dry {}
}

#[derive(Debug, Clone)]
pub struct Invoke(pub Payload);

// newtype namespace may be desired after the type erasure migration
// the `u32` field was for client id, and becomes unused after remove multiple
// client support on `CloseLoop`
// too lazy to refactor it off
pub type InvokeOk = (u32, Payload);

pub struct CloseLoop<W> {
    sender: Box<dyn SendEvent<Invoke> + Send + Sync>,
    pub workload: W,
    pub stop: Option<CloseLoopStop>,
    pub done: bool,
}

type CloseLoopStop = Box<dyn FnOnce() -> anyhow::Result<()> + Send + Sync>;

impl<W> Debug for CloseLoop<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloseLoop").finish_non_exhaustive()
    }
}

impl<W> CloseLoop<W> {
    pub fn new(sender: impl SendEvent<Invoke> + Send + Sync + 'static, workload: W) -> Self {
        Self {
            sender: Box::new(sender),
            workload,
            stop: None,
            done: false,
        }
    }
}

impl<W: Clone> CloseLoop<W> {
    pub fn duplicate<E: SendEvent<Invoke> + Send + Sync + 'static>(
        &self,
        sender: E,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            sender: Box::new(sender),
            workload: self.workload.clone(),
            stop: None,
            done: self.done,
        })
    }
}

impl<W: Workload> CloseLoop<W> {
    pub fn launch(&mut self) -> anyhow::Result<()> {
        let op = self
            .workload
            .next_op()?
            .ok_or(anyhow::anyhow!("not enough op"))?;
        self.sender.send(Invoke(op))
    }
}

impl<W: Workload> OnEvent<InvokeOk> for CloseLoop<W> {
    fn on_event(&mut self, (_, result): InvokeOk, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        self.workload.on_result(result)?;
        if let Some(op) = self.workload.next_op()? {
            self.sender.send(Invoke(op))
        } else {
            self.done = true;
            if let Some(stop) = self.stop.take() {
                stop()?
            }
            Ok(())
        }
    }
}

pub mod check {
    use super::{CloseLoop, Workload};

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DryCloseLoop<T>(T);

    impl<W: Workload> From<CloseLoop<W>> for DryCloseLoop<W::Dry> {
        fn from(value: CloseLoop<W>) -> Self {
            Self(value.workload.dehydrate())
        }
    }
}
