// stress test for the "reset forever" timer pattern
// reset forever pattern is commonly used for keep alive test: the desired
// semantic is "do not make a move as long as some condition holds", while
// "some condition" can be satisfied via handling certain events
// with a event loop that does not support reset (i.e. unset, since reset is
// effectively unset than set), reset forever pattern can only be approximated
// with a "aliveness" flag and a periodical "check liveness" timer. the
// downsides include
// * when state machine may only take action on check liveness timer alarm,
//   which is somewhere (T, 2T] after the liveness go off, where T is the check
//   liveness interval. there's no way for state machine to take action exactly
//   after a fixed delay
//   (it's true that state machine may take action at any time may not be the
//   desired behavior in some case, i'm just talking about user loses an option)
// * the meaning of timer is weakened. it has to be paired with a flag state to
//   restore the original intent. in another word, the expressiveness is
//   degraded
// this codebase requires event loop to not only support unset timers, but in
// a semantically strong way: it must support reliably cancellation of timers.
// in another word, a timer is guaranteed to not go off as soon as it has been
// unset. in the context of reset forever pattern, it means as soon as you reset
// a timer, it will be reliably postponed, will not alarm at the original
// deadline under any circumstance
//
// while this is great for application, event loop must carefully implement it
// to ensure constant timer overhead over time. with current tokio based
// implementation, the latency overhead is constant, while memory consumption
// keeps growing. there's probably no memory leak, just tokio is not reclaiming
// dead tasks as fast as event loop creating new ones
// anyway this stress test is just for future proof. currently there's no
// protocol taking reset forever pattern, and presumably no protocol will ever
// take it in such a tight loop
// the ever growing memory consumption issue could be solved if i don't spawn
// a task for every `set`. currently the reset intent is not passed to timer
// service: all it can see is a `unset` immediately followed by a `set`. if we
// pass down the reset intent, and manage to make use of tokio's `Sleep::reset`
// method (yeah it's all about that: i'm unhappy just because there's a reset
// available while my codebase is not using it), maybe the event loop can
// perform better in this stress test, hopefully

use std::time::Duration;

use augustus::event::{
    erased::{events::Init, session::Sender, Blanket, OnEvent, Session, Unify},
    OnTimer, SendEvent, TimerId,
};
use tokio::time::Instant;

pub struct Foo;

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

pub struct Bar<E> {
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
