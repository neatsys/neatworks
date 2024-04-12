use std::time::Duration;

use augustus::event::{
    erased::{events::Init, session::Sender, Blanket, OnEvent, Session, Unify},
    OnTimer, SendEvent, TimerId,
};
use tokio::time::Instant;

struct Foo;

#[derive(Debug, derive_more::Display, derive_more::Error)]
struct Exit;

impl OnEvent<Init> for Foo {
    fn on_event(
        &mut self,
        Init: Init,
        timer: &mut impl augustus::event::Timer,
    ) -> anyhow::Result<()> {
        let mut start = Instant::now();
        for i in 0..40_000_000 {
            let id = timer.set(Duration::from_secs(86400))?;
            timer.unset(id)?;
            if (i + 1) % 1_000_000 == 0 {
                println!("{:?}", start.elapsed());
                start = Instant::now()
            }
        }
        Err(Exit)?
    }
}

impl OnTimer for Foo {
    fn on_timer(&mut self, _: TimerId, _: &mut impl augustus::event::Timer) -> anyhow::Result<()> {
        unreachable!()
    }
}

struct Bar<E> {
    sender: E,
    timer_id: Option<TimerId>,
    start: Instant,
}

struct Tick(u32);

impl<E: SendEvent<Tick>> OnEvent<Tick> for Bar<E> {
    fn on_event(
        &mut self,
        Tick(n): Tick,
        timer: &mut impl augustus::event::Timer,
    ) -> anyhow::Result<()> {
        let replaced = self
            .timer_id
            .replace(timer.set(Duration::from_secs(86400))?);
        if let Some(timer_id) = replaced {
            timer.unset(timer_id)?
        }
        if (n + 1) % 1_000_000 == 0 {
            println!("{:?}", self.start.elapsed());
            self.start = Instant::now()
        }
        if n + 1 == 20_000_000 {
            Err(Exit)?
        }
        self.sender.send(Tick(n + 100))
    }
}

impl<E> OnTimer for Bar<E> {
    fn on_timer(&mut self, _: TimerId, _: &mut impl augustus::event::Timer) -> anyhow::Result<()> {
        unreachable!()
    }
}

#[tokio::main]
// #[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut foo_session = Session::new();
    // let mut foo = Blanket(Unify(Foo));
    let mut bar = Blanket(Unify(Bar {
        sender: Box::new(Sender::from(foo_session.sender()))
            as Box<dyn SendEvent<Tick> + Send + Sync>,
        timer_id: None,
        start: Instant::now(),
    }));
    // Sender::from(foo_session.sender()).send(Init)?;
    for i in 0..100 {
        Sender::from(foo_session.sender()).send(Tick(i))?
    }
    // match foo_session.run(&mut foo).await {
    match foo_session.run(&mut bar).await {
        Ok(()) => unreachable!(),
        Err(err) if err.is::<Exit>() => {}
        Err(err) => Err(err)?,
    }
    Ok(())
}
