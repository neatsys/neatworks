pub mod events {
    #[derive(Debug)]
    pub struct Invoke<M>(pub M);

    #[derive(Debug)]
    pub struct InvokeOk<M>(pub M);
}
