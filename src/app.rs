pub mod kvstore;

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

pub use kvstore::{KVStore, KVStoreOp, KVStoreResult};
