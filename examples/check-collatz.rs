use std::{env::args, thread::available_parallelism, time::Duration};

use augustus::search::{breadth_first, random_depth_first, SearchResult, Settings};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct State(u64);

#[derive(Debug, Clone)]
enum Event {
    Div2,
    Mul3Add1,
}

impl augustus::search::State for State {
    type Event = Event;

    fn duplicate(&self) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(*self)
    }

    fn events(&self) -> Vec<Self::Event> {
        let mut events = vec![Event::Div2];
        if self.0 > 1 && (self.0 - 1) % 3 == 0 {
            events.push(Event::Mul3Add1)
        }
        events
    }

    fn step(&mut self, event: Self::Event) -> anyhow::Result<()> {
        match event {
            Event::Div2 => self.0 *= 2,
            Event::Mul3Add1 => self.0 = (self.0 - 1) / 3,
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let challenging_n = args()
        .nth(1)
        .unwrap_or(String::from("117418"))
        .parse::<u64>()?;
    let settings = Settings {
        invariant: move |&State(n): &State| {
            if n == challenging_n {
                Err(anyhow::anyhow!("fail to defeat conjecture"))
            } else {
                Ok(())
            }
        },
        goal: |_: &_| false,
        prune: |State(n): &State| n.leading_zeros() == 0,
        max_depth: None,
    };
    let random = args().nth(2);
    let result = if random.as_deref() == Some("random") {
        random_depth_first::<_, State, _, _, _>(
            State(1),
            settings,
            available_parallelism()?,
            Duration::from_secs(60),
        )?
    } else {
        breadth_first::<_, State, _, _, _>(
            State(1),
            settings,
            available_parallelism()?,
            Duration::from_secs(60),
        )?
    };
    if let SearchResult::InvariantViolation(trace, err) = result {
        for (event, state) in trace.into_iter().rev() {
            println!("{state:?}");
            println!("{event:?}")
        }
        println!("{err}")
    } else {
        println!("{result:?}")
    }
    Ok(())
}
