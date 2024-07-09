use derive_more::{Deref, DerefMut};
use derive_where::derive_where;

use super::{OnEvent, SendEvent, Submit};

// the alternative definition would be Inline<'a, S, C>(&'a mut S, &'a mut C)
// (or two distinct lifetimes if preferred)
// there may be some benefits by not fixing the inners to references in the
// future, so i have chosen this representation, but there is indeed nothing
// much for now since the use cases for Inline always takes references
#[derive(Debug)]
pub struct Inline<S, C>(pub S, pub C);

impl<S: OnEvent<C>, C> SendEvent<S::Event> for Inline<&'_ mut S, &'_ mut C> {
    fn send(&mut self, event: S::Event) -> anyhow::Result<()> {
        self.0.on_event(event, self.1)
    }
}

// the original intent was to reuse the SendEvent<UntypedEvent<_, _>> impl of a
// Inline<Untyped<_, _>, _>
// that did not work because UntypedEvent<_, _> contains a Box<_ + 'static> so a
// wrapping pass through UntypedEvent would unnecessarily amplify `'a` to
// `'static` which is highly undesired for Inline use cases
// it is possible for UntypedEvent to be `struct UntypedEvent<'a>(Box<_ + 'a>)`
// but that would complicate a lot just for this niche case, while this direct
// repeated impl, though conceptually repeating, really takes only trivial code
impl<'a, S, C> Submit<S, C> for Inline<&'a mut S, &'a mut C> {
    fn submit(&mut self, work: crate::event::Work<S, C>) -> anyhow::Result<()> {
        work(self.0, self.1)
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

#[cfg(test)]
mod tests {
    use crate::event::Submit as _;

    use super::*;

    #[test]
    fn inline_worker() -> anyhow::Result<()> {
        let mut state = 1;
        let mut context = 0;
        let mut inline = Inline(&mut state, &mut context);
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
