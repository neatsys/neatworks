use bytes::Bytes;

use crate::event::SendEvent;

use super::{events::Cast, Addr};

#[derive(Debug)]
pub struct Forward<A, N>(pub A, pub N);

impl<A: Addr, N: SendEvent<Cast<A, M>>, M> SendEvent<Cast<(), M>> for Forward<A, N> {
    fn send(&mut self, Cast((), message): Cast<(), M>) -> anyhow::Result<()> {
        self.1.send(Cast(self.0.clone(), message))
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

impl<A: Addr, N: SendEvent<Cast<A, M>>, M, I: Into<usize>> SendEvent<Cast<I, M>>
    for IndexNet<A, N>
{
    fn send(&mut self, Cast(index, message): Cast<I, M>) -> anyhow::Result<()> {
        let index = index.into();
        let addr = self
            .addrs
            .get(index)
            .ok_or(anyhow::format_err!("missing address of index {index}"))?;
        self.inner.send(Cast(addr.clone(), message))
    }
}

impl<A: Addr, N: SendEvent<Cast<A, Bytes>>> SendEvent<Cast<All, Bytes>> for IndexNet<A, N> {
    fn send(&mut self, Cast(All, message): Cast<All, Bytes>) -> anyhow::Result<()> {
        for (index, addr) in self.addrs.iter().enumerate() {
            if Some(index) == self.all_except {
                continue;
            }
            self.inner.send(Cast(addr.clone(), message.clone()))?
        }
        Ok(())
    }
}
