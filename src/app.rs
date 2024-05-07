// some submodule has not `impl App`, but `impl Iterator<Workload>`
// applications and workloads are tighly coupled, so it should be desirable to
// keep them together (and i don't want their modules to be named
// `app_and_workload`)
pub mod btree;
pub mod kvstore;
pub mod sqlite;
pub mod ycsb;

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

impl<T: ?Sized + App> App for Box<T> {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>> {
        T::execute(self, op)
    }
}

#[derive(Debug)]
pub struct Null;

impl App for Null {
    fn execute(&mut self, _: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(Default::default())
    }
}

// pairing with event sending closures, this effectively allows any
// `impl OnEvent` (either variant) to be running as a `impl App` state machine
#[derive(Debug)]
pub struct OnBuf<F>(pub F);

impl<F: for<'a> FnMut(&'a [u8]) -> anyhow::Result<()>> App for OnBuf<F> {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>> {
        (self.0)(op)?;
        Ok(Default::default())
    }
}

pub use btree::BTreeMap;
pub use kvstore::KVStore;
pub use sqlite::Sqlite;
