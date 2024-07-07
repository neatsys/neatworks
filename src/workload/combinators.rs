use crate::event::SendEvent;

use super::Workload;

#[derive(Debug)]
pub struct Iter<I, R> {
    generate: I,
    expected_result: Option<R>,
    pub done: bool,
}

impl<I: Iterator> Workload for Iter<I, <I::Item as Pair>::Second>
where
    I::Item: Pair,
    <I::Item as Pair>::Second: Eq,
{
    type Op = <I::Item as Pair>::First;
    type Result = <I::Item as Pair>::Second;

    fn init(&mut self, mut sender: impl SendEvent<Self::Op>) -> anyhow::Result<()> {
        let Some((op, result)) = self.generate.next().map(Pair::into) else {
            self.done = true;
            return Ok(());
        };
        let replaced = self.expected_result.replace(result);
        anyhow::ensure!(replaced.is_none());
        sender.send(op)
    }

    fn on_result(
        &mut self,
        result: Self::Result,
        mut sender: impl SendEvent<Self::Op>,
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
        sender.send(op)
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
