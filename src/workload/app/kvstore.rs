use std::{collections::BTreeMap, hash::Hash};

use derive_where::derive_where;
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::codec::Encode;
use crate::event::SendEvent;
use crate::workload::events::{Invoke, InvokeOk};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct KVStore(BTreeMap<String, String>);

impl KVStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Op {
    Put(String, String),
    Get(String),
    Append(String, String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Result {
    PutOk,
    GetResult(String),
    KeyNotFound,
    AppendResult(String),
}

pub type App = crate::codec::Decode<Op, Encode<Result, KVStore>>;

impl<E: SendEvent<InvokeOk<Result>>> SendEvent<Invoke<Op>> for (&'_ mut KVStore, E) {
    fn send(&mut self, Invoke(op): Invoke<Op>) -> anyhow::Result<()> {
        let (KVStore(store), response) = self;
        let result = match op {
            Op::Put(key, value) => {
                store.insert(key, value);
                Result::PutOk
            }
            Op::Get(key) => {
                if let Some(value) = store.get(&key) {
                    Result::GetResult(value.clone())
                } else {
                    Result::KeyNotFound
                }
            }
            Op::Append(key, postfix) => {
                let mut value = store.get(&key).cloned().unwrap_or_default();
                value += &postfix;
                store.insert(key, value.clone());
                Result::AppendResult(value)
            }
        };
        response.send(InvokeOk(result))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[derive_where(Hash)]
pub struct InfinitePutGet {
    namespace: String,
    #[derive_where(skip)] // is this safe? probably since `Eq` still considers it
    rng: StdRng,
    values: [String; 5],
    should_get: bool,
}

impl InfinitePutGet {
    pub fn new(namespace: impl Into<String>, seed_rng: &mut impl Rng) -> anyhow::Result<Self> {
        Ok(Self {
            namespace: namespace.into(),
            rng: StdRng::from_rng(seed_rng)?,
            values: Default::default(),
            should_get: false,
        })
    }
}

impl Iterator for InfinitePutGet {
    type Item = (Op, Result);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.rng.gen_range(0..5);
        let (op, result) = if self.should_get {
            (
                Op::Get(format!("{}-{index}", self.namespace)),
                if self.values[index] == String::default() {
                    Result::KeyNotFound
                } else {
                    Result::GetResult(self.values[index].clone())
                },
            )
        } else {
            let value = (&mut self.rng)
                .sample_iter(Alphanumeric)
                .take(8)
                .map(char::from)
                .collect::<String>();
            self.values[index].clone_from(&value);
            (
                Op::Put(format!("{}-{index}", self.namespace), value),
                Result::PutOk,
            )
        };
        self.should_get = !self.should_get;
        Some((op, result))
    }
}
