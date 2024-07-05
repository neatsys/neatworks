use derive_more::{Deref, DerefMut};

use super::{OnEvent, SendEvent};

#[derive(Debug)]
pub struct Inline<S, C>(pub S, pub C);

impl<S: OnEvent<C>, C> SendEvent<S::Event> for Inline<S, C> {
    fn send(&mut self, event: S::Event) -> anyhow::Result<()> {
        self.0.on_event(event, &mut self.1)
    }
}

#[derive(Debug, Deref, DerefMut)]
pub struct Transient<M>(pub Vec<M>);

impl<M: Into<N>, N> SendEvent<M> for Transient<N> {
    fn send(&mut self, event: M) -> anyhow::Result<()> {
        self.push(event.into());
        Ok(())
    }
}

pub mod work {
    use crate::event::work::{Event, Submit};

    #[derive(Debug)]
    pub struct Inline<S, C>(pub S, pub C);

    impl<S, C> Submit<S, C> for Inline<S, C> {
        fn submit(&mut self, work: Event<S, C>) -> anyhow::Result<()> {
            work(&mut self.0, &mut self.1)
        }
    }
}
