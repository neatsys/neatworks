use std::mem::take;

use derive_more::{Display, Error};

use crate::event::SendEvent;

pub trait Choose {
    fn choose<'a, T>(&mut self, choices: &'a [T]) -> anyhow::Result<&'a T>;
    fn choose_index(&mut self, len: usize) -> anyhow::Result<usize>;
}

#[cfg(test)]
impl Choose for arbtest::arbitrary::Unstructured<'_> {
    fn choose<'a, T>(&mut self, choices: &'a [T]) -> anyhow::Result<&'a T> {
        Self::choose(self, choices)
            .or_else(|err| {
                if matches!(err, arbtest::arbitrary::Error::NotEnoughData) {
                    choices.get(0).ok_or(arbtest::arbitrary::Error::EmptyChoose)
                } else {
                    Err(err)
                }
            })
            .map_err(Into::into)
    }

    fn choose_index(&mut self, len: usize) -> anyhow::Result<usize> {
        Self::choose_index(self, len)
            .or_else(|err| {
                if matches!(err, arbtest::arbitrary::Error::NotEnoughData) {
                    if len > 0 {
                        Ok(0)
                    } else {
                        Err(arbtest::arbitrary::Error::EmptyChoose)
                    }
                } else {
                    Err(err)
                }
            })
            .map_err(Into::into)
    }
}

pub trait State: SendEvent<Self::Event> {
    type Event;

    fn pop_event(&mut self, choose: &mut impl Choose) -> anyhow::Result<Self::Event>;
}

#[derive(Debug, Display, Error)]
pub struct ProgressExhausted;

#[cfg(test)]
pub fn step(
    state: &mut impl State,
    u: &mut arbtest::arbitrary::Unstructured<'_>,
) -> anyhow::Result<()> {
    let event = state.pop_event(u)?;
    state.send(event)
}

impl<A: Clone + Ord, M: Clone + Ord> super::NetworkState<A, M> {
    pub fn pop_event(&mut self, choose: &mut impl Choose) -> anyhow::Result<(A, M)> {
        // this looks silly and inefficient
        // both arbitrary::Unstructured and BTreeSet have bad interfaces, nothing much I can do
        let index = choose.choose_index(self.messages.len())?;
        let mut messages = take(&mut self.messages).into_iter();
        self.messages.extend(messages.by_ref().take(index));
        let event = messages.next().unwrap();
        self.messages.extend(messages);
        Ok(event)
    }
}
