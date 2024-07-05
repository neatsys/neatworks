use bytes::Bytes;

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

#[derive(Debug)]
pub struct IndexNet<A, N> {
    addrs: Vec<A>,
    all_except: Option<usize>,
    inner: N,
}

impl<A, N> IndexNet<A, N> {
    pub fn new(addrs: Vec<A>, all_except: impl Into<Option<usize>>, net: N) -> Self {
        Self {
            addrs,
            all_except: all_except.into(),
            inner: net,
        }
    }
}

impl<A: Addr, N: SendEvent<Send<A, M>>, M, I: Into<usize>> SendEvent<Send<I, M>>
    for IndexNet<A, N>
{
    fn send(&mut self, Send(index, message): Send<I, M>) -> anyhow::Result<()> {
        let index = index.into();
        let addr = self
            .addrs
            .get(index)
            .ok_or(anyhow::format_err!("missing address of index {index}"))?;
        self.inner.send(Send(addr.clone(), message))
    }
}

impl<A: Addr, N: SendEvent<Send<A, Bytes>>> SendEvent<Send<All, Bytes>> for IndexNet<A, N> {
    fn send(&mut self, Send(All, message): Send<All, Bytes>) -> anyhow::Result<()> {
        for (index, addr) in self.addrs.iter().enumerate() {
            if Some(index) == self.all_except {
                continue;
            }
            self.inner.send(Send(addr.clone(), message.clone()))?
        }
        Ok(())
    }
}
