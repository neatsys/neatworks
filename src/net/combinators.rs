use crate::event::SendEvent;

use super::{events::Send, Addr};

#[derive(Debug)]
pub struct Forward<A, N>(pub A, pub N);

impl<A: Addr, N: SendEvent<Send<A, M>>, M> SendEvent<Send<(), M>> for Forward<A, N> {
    fn send(&mut self, Send((), message): Send<(), M>) -> anyhow::Result<()> {
        self.1.send(Send(self.0.clone(), message))
    }
}

#[derive(Debug)]
pub struct All;
