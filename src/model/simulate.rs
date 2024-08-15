use derive_more::{Display, Error};

#[derive(Debug, Display, Error)]
pub struct ProgressExhausted;

pub trait State {
    fn step(&mut self) -> anyhow::Result<()>;

    fn progress(&mut self) -> anyhow::Result<()> {
        loop {
            match self.step() {
                Err(err) if err.is::<ProgressExhausted>() => return Ok(()),
                result => result?,
            }
        }
    }
}
