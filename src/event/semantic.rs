use derive_more::{Display, Error};

#[derive(Debug, Display, Error)]
pub struct Exit;

#[derive(Debug)]
pub struct Invoke<M>(pub M);

#[derive(Debug)]
pub struct InvokeOk<M>(pub M);
