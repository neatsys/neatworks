use bytes::Bytes;

pub mod events {
    #[derive(Debug)]
    pub struct Invoke<M>(pub M);

    #[derive(Debug)]
    pub struct InvokeOk<M>(pub M);
}
pub trait App {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Bytes>;
}

#[derive(Debug)]
pub struct Null;

impl App for Null {
    fn execute(&mut self, _: &[u8]) -> anyhow::Result<Bytes> {
        Ok(Default::default())
    }
}
