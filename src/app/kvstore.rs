use std::{collections::BTreeMap, hash::Hash, panic::UnwindSafe};

use derive_where::derive_where;
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::{
    net::Payload,
    workload::{Check, Workload},
};

use super::App;

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

impl App for KVStore {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>> {
        let Self(store) = self;
        let result = match serde_json::from_slice(op)? {
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
        Ok(serde_json::to_vec(&result)?)
    }
}

pub fn static_workload(
    // ask for exact size as a safety manner, we are `collecting` inside
    rounds: impl ExactSizeIterator<Item = (Op, Result)>,
) -> anyhow::Result<impl Workload<Attach = ()> + Clone + UnwindSafe> {
    Ok(Check::new(
        rounds
            .map(|(op, result)| {
                Ok((
                    Payload(serde_json::to_vec(&op)?),
                    Payload(serde_json::to_vec(&result)?),
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter(),
    ))
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
    type Item = (Payload, Payload);

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
        Some((
            Payload(serde_json::to_vec(&op).unwrap()),
            Payload(serde_json::to_vec(&result).unwrap()),
        ))
    }
}
