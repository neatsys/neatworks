use derive_more::{Deref, DerefMut};
use derive_where::derive_where;

use super::{OnEvent, SendEvent};

#[derive(Debug)]
pub struct Inline<S, C>(pub S, pub C);

// impl<S: OnEvent<C>, C> SendEvent<S::Event> for Inline<S, C> {
//     fn send(&mut self, event: S::Event) -> anyhow::Result<()> {
//         self.0.on_event(event, &mut self.1)
//     }
// }

impl<S: OnEvent<C>, C> SendEvent<S::Event> for Inline<&'_ mut S, &'_ mut C> {
    fn send(&mut self, event: S::Event) -> anyhow::Result<()> {
        self.0.on_event(event, self.1)
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
    use crate::event::{OnEvent as _, Submit, Untyped, UntypedEvent};

    pub type Inline<'a, S, C> = super::Inline<Untyped<&'a mut C, &'a mut S>, &'a mut C>;

    impl<'a, S, C> Inline<'a, S, C> {
        pub fn new_worker(state: &'a mut S, context: &'a mut C) -> Self {
            Self(Untyped::new(state), context)
        }
    }

    // fix to 'static because UntypedEvent takes a Box<_ + 'static>
    // it is possible for UntypedEvent to be `struct UntypedEvent<'a>(Box<_ + 'a>)` but that is too
    // overly engineered
    impl<'a, S: 'static, C: 'static> Submit<S, C> for Inline<'a, S, C> {
        fn submit(&mut self, work: crate::event::Work<S, C>) -> anyhow::Result<()> {
            self.0.on_event(
                UntypedEvent(Box::new(move |state, context| work(*state, *context))),
                &mut self.1,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::event::Submit as _;

    use super::*;

    #[test]
    fn inline_worker() -> anyhow::Result<()> {
        let mut state = 1;
        let mut context = 0;
        let mut inline = Inline::new_worker(&mut state, &mut context);
        for _ in 0..10 {
            inline.submit(Box::new(move |state, context| {
                let old_state = *state;
                *state += *context;
                *context = old_state;
                anyhow::Ok(())
            }))?
        }
        anyhow::ensure!(context == 55);
        Ok(())
    }
}
