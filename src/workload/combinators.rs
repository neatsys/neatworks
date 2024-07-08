use derive_more::Deref;

use std::marker::PhantomData;

use crate::event::SendEvent;

use super::{
    events::{Invoke, InvokeOk},
    Workload,
};

#[derive(Debug, Clone)]
pub struct Iter<R, I> {
    generate: I,
    expected_result: Option<R>,
    pub done: bool,
}

impl<R, I> Iter<R, I> {
    pub fn new(generate: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            generate: generate.into_iter(),
            expected_result: None,
            done: false,
        }
    }
}

impl<I: Iterator> Workload for Iter<<I::Item as Pair>::Second, I>
where
    I::Item: Pair,
    <I::Item as Pair>::Second: Eq,
{
    type Op = <I::Item as Pair>::First;
    type Result = <I::Item as Pair>::Second;

    fn init(&mut self, mut sender: impl SendEvent<Invoke<Self::Op>>) -> anyhow::Result<()> {
        let Some((op, result)) = self.generate.next().map(Pair::into) else {
            self.done = true;
            return Ok(());
        };
        let replaced = self.expected_result.replace(result);
        anyhow::ensure!(replaced.is_none());
        sender.send(Invoke(op))
    }

    fn on_result(
        &mut self,
        InvokeOk(result): InvokeOk<Self::Result>,
        mut sender: impl SendEvent<Invoke<Self::Op>>,
    ) -> anyhow::Result<()> {
        let Some(expected_result) = self.expected_result.take() else {
            anyhow::bail!("missing expected result")
        };
        anyhow::ensure!(result == expected_result);
        let Some((op, result)) = self.generate.next().map(Pair::into) else {
            self.done = true;
            return Ok(());
        };
        self.expected_result = Some(result);
        sender.send(Invoke(op))
    }
}

pub trait Pair {
    type First;
    type Second;

    fn into(self) -> (Self::First, Self::Second);
}

impl<A, B> Pair for (A, B) {
    type First = A;
    type Second = B;

    fn into(self) -> (Self::First, Self::Second) {
        self
    }
}

#[derive(Debug, Clone)]
pub struct UncheckedIter<R, I> {
    generate: I,
    pub done: bool,
    _m: PhantomData<R>,
}

impl<R, I> UncheckedIter<R, I> {
    pub fn new(generate: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            generate: generate.into_iter(),
            done: false,
            _m: Default::default(),
        }
    }
}

impl<R, I: Iterator> Workload for UncheckedIter<R, I> {
    type Op = I::Item;
    type Result = R;

    fn init(&mut self, mut sender: impl SendEvent<Invoke<Self::Op>>) -> anyhow::Result<()> {
        let Some(op) = self.generate.next() else {
            self.done = true;
            return Ok(());
        };
        sender.send(Invoke(op))
    }

    fn on_result(
        &mut self,
        _: InvokeOk<Self::Result>,
        sender: impl SendEvent<Invoke<Self::Op>>,
    ) -> anyhow::Result<()> {
        self.init(sender)
    }
}

#[derive(Debug, Clone, Deref)]
pub struct Record<O, R, W> {
    #[deref]
    inner: W,
    pub invocations: Vec<(O, R)>,
    outstanding: Option<O>,
}

impl<O, R, W> Record<O, R, W> {
    pub fn new(workload: W) -> Self {
        Self {
            inner: workload,
            invocations: Default::default(),
            outstanding: None,
        }
    }
}

impl<W: Workload> Workload for Record<W::Op, W::Result, W>
where
    W::Op: Clone,
    W::Result: Clone,
{
    type Op = W::Op;
    type Result = W::Result;

    fn init(&mut self, mut sender: impl SendEvent<Invoke<Self::Op>>) -> anyhow::Result<()> {
        let mut intercept = None;
        self.inner.init(&mut intercept)?;
        let Some(Invoke(op)) = intercept.take() else {
            anyhow::bail!("missing init op")
        };
        let replaced = self.outstanding.replace(op.clone());
        anyhow::ensure!(replaced.is_none());
        sender.send(Invoke(op))
    }

    fn on_result(
        &mut self,
        InvokeOk(result): InvokeOk<Self::Result>,
        mut sender: impl SendEvent<Invoke<Self::Op>>,
    ) -> anyhow::Result<()> {
        let Some(op) = self.outstanding.take() else {
            anyhow::bail!("missing outstanding op");
        };
        self.invocations.push((op, result.clone()));

        let mut intercept = None;
        self.inner.on_result(InvokeOk(result), &mut intercept)?;
        if let Some(Invoke(op)) = intercept.take() {
            self.outstanding = Some(op.clone());
            sender.send(Invoke(op))?
        }
        Ok(())
    }
}
