use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub trait App {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>>;
}

#[derive(Debug)]
pub struct Null;

impl App for Null {
    fn execute(&mut self, _: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(Default::default())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct KVStore(BTreeMap<String, String>);

impl KVStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KVStoreOp {
    Put(String, String),
    Get(String),
    Append(String, String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KVStoreResult {
    PutOk,
    GetResult(String),
    KetyNotFound,
    AppendResult(String),
}

impl App for KVStore {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>> {
        let Self(store) = self;
        let result = match serde_json::from_slice(op)? {
            KVStoreOp::Put(key, value) => {
                store.insert(key, value);
                KVStoreResult::PutOk
            }
            KVStoreOp::Get(key) => {
                if let Some(value) = store.get(&key) {
                    KVStoreResult::GetResult(value.clone())
                } else {
                    KVStoreResult::KetyNotFound
                }
            }
            KVStoreOp::Append(key, postfix) => {
                let mut value = store.get(&key).cloned().unwrap_or_default();
                value += &postfix;
                store.insert(key, value.clone());
                KVStoreResult::AppendResult(value)
            }
        };
        Ok(serde_json::to_vec(&result)?)
    }
}
