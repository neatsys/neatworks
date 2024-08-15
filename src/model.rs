use crate::event::SendEvent;

pub mod search;
pub mod simulate;

pub trait State: SendEvent<Self::Event> {
    type Event;

    fn events(&self) -> impl Iterator<Item = Self::Event> + '_;
}

// the alternative `State` interface
//   trait State = OnEvent<C> where C: Context<Self::Event>
//   pub trait Context<M> {
//       fn register(&mut self, event: M) -> anyhow::Result<()>;
//   }
// the custom `events` method can be removed then, results in more compact
// interface
//
// the downside of this alternation
// * bootstrapping. the every first event(s) that applied to the initial state
//   is hard to be provided
// * it doesn't fit the current searching workflows. it may be possible to
//   adjust the workflows to maintain a buffer of not yet applied events, but
//   in my opinion that complicates things

#[cfg(test)]
mod tests {
    use arbtest::arbtest;

    #[test]
    fn pigeonhole() {
        arbtest(|u| {
            let buf = u.arbitrary::<[u8; 257]>()?;
            assert!(buf
                .iter()
                .enumerate()
                .any(|(i, b1)| buf.iter().skip(i + 1).any(|b2| b2 == b1)));
            Ok(())
        });
    }
}
