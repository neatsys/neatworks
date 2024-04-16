use std::{collections::VecDeque, fmt::Debug, hash::Hash};

use bincode::Options;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    event::{erased::OnEvent, OnTimer, SendEvent, Timer},
    workload::{Invoke, InvokeOk},
};

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, derive_more::Deref, Serialize, Deserialize,
)]
pub struct Payload(pub Vec<u8>);

impl Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "Payload(\"{s}\")")
        } else {
            write!(
                f,
                "Payload({}{})",
                self.0
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .take(32)
                    .collect::<Vec<_>>()
                    .concat(),
                if self.0.len() > 32 {
                    format!(".. <len {}>", self.0.len())
                } else {
                    String::new()
                }
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Payload,
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

impl<E: SendEvent<Invoke<O>>, O> OnEvent<Invoke<O>> for Queue<E, O> {
    fn on_event(&mut self, Invoke(op): Invoke<O>, _: &mut impl Timer) -> anyhow::Result<()> {
        if let Some(ops) = &mut self.ops {
            ops.push_back(op);
            Ok(())
        } else {
            self.ops = Some(Default::default());
            self.sender.send(Invoke(op))
        }
    }
}

impl<E: SendEvent<Invoke<O>>, O, R> OnEvent<InvokeOk<R>> for Queue<E, O> {
    fn on_event(&mut self, _: InvokeOk<R>, _: &mut impl Timer) -> anyhow::Result<()> {
        if let Some(ops) = &mut self.ops {
            if let Some(op) = ops.pop_front() {
                self.sender.send(Invoke(op))?
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

pub struct BincodeSer<E, M>(E, std::marker::PhantomData<M>);

impl<E, M> From<E> for BincodeSer<E, M> {
    fn from(value: E) -> Self {
        Self(value, Default::default())
    }
}

impl<E: SendEvent<Payload>, M: Serialize> SendEvent<M> for BincodeSer<E, M> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        self.0.send(Payload(bincode::options().serialize(&event)?))
    }
}

pub struct BincodeDe<E, M>(E, std::marker::PhantomData<M>);

impl<E, M> From<E> for BincodeDe<E, M> {
    fn from(value: E) -> Self {
        Self(value, Default::default())
    }
}

impl<E: SendEvent<M>, M: DeserializeOwned> SendEvent<Payload> for BincodeDe<E, M> {
    fn send(&mut self, Payload(buf): Payload) -> anyhow::Result<()> {
        self.0.send(
            bincode::options()
                .allow_trailing_bytes()
                .deserialize(&buf)?,
        )
    }
}

// cSpell:words bincode deque
