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
            Op::Insert(key, values) => {
                self.0.insert(key, values);
                Result::Ok
            }
            Op::Scan(key, count) => Result::ScanOk(
                self.0
                    .range(key..)
                    // .map(|(_, values)| values.clone())
                    // whole string is too long to be sent over UDP packets, so reduce each field
                    // into single checksum letter
                    // 1000 max scan length * (10 field * 1 byte per field + 1 byte length) = 11K
                    // roughly 7 * MTU, probably acceptable
                    .map(|(_, values)| {
                        values
                            .iter()
                            .map(|value| {
                                use std::hash::{BuildHasher, BuildHasherDefault};
                                format!(
                                    "{:x}",
                                    BuildHasherDefault::<rustc_hash::FxHasher>::default()
                                        .hash_one(value)
                                        & 0xf
                                )
                            })
                            .collect()
                    })
                    .take(count)
                    .collect(),
            ),
            Op::Delete(key) => {
                if self.0.remove(&key).is_some() {
                    Result::Ok
                } else {
                    Result::NotFound
                }
            }
        };
        Ok(bincode::options().serialize(&result)?)
    }
}
