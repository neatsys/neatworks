use bincode::Options;

use super::{
    ycsb::{Op, Result},
    App,
};

#[derive(Debug, Default)]
pub struct BTreeMap(pub std::collections::BTreeMap<String, Vec<String>>);

impl BTreeMap {
    pub fn new() -> Self {
        Self::default()
    }
}

impl App for BTreeMap {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>> {
        let result = match bincode::options().deserialize(op)? {
            Op::Read(key) => {
                if let Some(record) = self.0.get(&key) {
                    // for simplicity, panic on out of bound
                    // YCSB workload probably can do it right
                    Result::ReadOk(record.clone())
                } else {
                    Result::NotFound
                }
            }
            Op::Update(key, field, value) => {
                if let Some(record) = self.0.get_mut(&key) {
                    record[field] = value;
                    Result::Ok
                } else {
                    Result::NotFound
                }
            }
            Op::Insert(key, value) => {
                self.0.insert(key, value);
                Result::Ok
            }
            Op::Scan(key, field, count) => Result::ScanOk(
                self.0
                    .range(key..)
                    // .map(|(_, value)| value[field].clone())
                    // whole string is too long to be sent over UDP packets
                    .map(|(_, value)| {
                        use std::hash::{BuildHasher, BuildHasherDefault};
                        BuildHasherDefault::<rustc_hash::FxHasher>::default()
                            .hash_one(&value[field])
                            .to_string()
                    })
                    .take(count)
                    .collect(),
            ),
            Op::Delete(key) => {
                self.0.remove(&key);
                Result::Ok // NotFound when key not present?
            }
        };
        Ok(bincode::options().serialize(&result)?)
    }
}
