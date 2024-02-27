// some submodule has not `impl App`, but `impl Iterator<Workload>`
// applications and workloads are tighly coupled, so it should be desirable to
// keep them together (and i don't want their modules to be named
// `app_and_workload`)
pub mod kvstore;
pub mod ycsb;
pub mod btree;

pub trait App {
    // following SpexPaxos convention, this interface takes "binary in, binary out" approach, avoids
    // exposing type parameters (for `op` and `result`) to replication/transaction protocols, along
    // with unnecessary ser/de overhead
    //
    // however, the binary format is not desirable for testing purpose. to mitigate this issue, the
    // `impl App` that is for testing should works with utf-8 encoded human-readable (e.g. JSON)
    // `op` and `result`. `rpc::Payload`, which comes with `Debug` impl that is optimized for this
    // convention, should be used to wrap all `op` and `result` sites
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>>;
}

#[derive(Debug)]
pub struct Null;

impl App for Null {
    fn execute(&mut self, _: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(Default::default())
    }
}

pub use kvstore::KVStore;
pub use btree::BTreeMap;
