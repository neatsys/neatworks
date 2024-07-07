use derive_more::{Deref, DerefMut};
use derive_where::derive_where;

use super::{OnEvent, SendEvent};

#[derive(Debug)]
pub struct Inline<S, C>(pub S, pub C);

impl<S: OnEvent<C>, C> SendEvent<S::Event> for Inline<S, C> {
    fn send(&mut self, event: S::Event) -> anyhow::Result<()> {
        self.0.on_event(event, &mut self.1)
    }
}

// a bit wild to directly impl on foreign type, hope no conflict to anything
impl<M> SendEvent<M> for Option<M> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        let replaced = self.replace(event);
        anyhow::ensure!(replaced.is_none());
        Ok(())
    }
}

#[derive(Debug, Deref, DerefMut)]
#[derive_where(Default)]
pub struct Transient<M>(pub Vec<M>);

impl<M> Transient<M> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<M: Into<N>, N> SendEvent<M> for Transient<N> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        self.push(event.into());
        Ok(())
    }
}

#[derive(Debug)]
pub struct Map<F, E>(pub F, pub E);

impl<F: FnMut(M) -> N, M, N, E: SendEvent<N>> SendEvent<M> for Map<F, E> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        self.1.send((self.0)(event))
    }
}

pub mod work {
    use crate::event::Untyped;

    pub type Inline<S, C> = super::Inline<Untyped<C, S>, C>;

    impl<S, C> Inline<S, C> {
        pub fn new_worker(state: S, context: C) -> Self {
            Self(Untyped::new(state), context)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::event::Submit as _;

    use super::*;

    #[test]
    fn inline_worker() -> anyhow::Result<()> {
        let mut inline = Inline::new_worker(1, 0);
        for _ in 0..10 {
            inline.submit(Box::new(move |state, context| {
                let old_state = *state;
                *state += *context;
                *context = old_state;
                anyhow::Ok(())
            }))?
        }
        anyhow::ensure!(inline.1 == 55);
        Ok(())
    }
}
