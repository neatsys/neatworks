use anyhow::Context as _;
use boson::{Clock, ClockCircuit, F};
use plonky2::{field::types::Field as _, plonk::circuit_data::CircuitConfig};
use plonky2_maybe_rayon::rayon;
use tracing::info;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = CircuitConfig::standard_recursion_config();

    rayon::ThreadPoolBuilder::new()
        .build()
        .context("Failed to build thread pool.")?
        .install(|| {
            info!(
                "Using {} compute threads on {} cores",
                rayon::current_num_threads(),
                16
            );

            let circuits = ClockCircuit::precompted(4, 2, config)?;
            let clock0 = Clock::genesis(&circuits)?;
            let clock11 = clock0.increment(0, F::ONE, &circuits)?;
            let clock12 = clock0.increment(1, F::ONE, &circuits)?;
            println!("{:?} {:?} ->", clock11.counters(), clock12.counters(),);
            let clock2 = clock11.merge(&clock12, &circuits)?;
            println!("{:?}", clock2.counters());

            // let max_depth = 32;
            // let circuits = ClockCircuit::precompted(4, max_depth, config)?;
            // let clock = Clock::genesis(&circuits)?;
            // clock.verify(&circuits)?;
            // let mut clocks = Vec::new();
            // for index in 0..4 {
            //     clocks.push(clock.clone());
            //     // for i in 0..max_depth {
            //     for i in 0..8 {
            //         let clock = clocks.last().as_ref().unwrap().increment(
            //             index,
            //             F::from_canonical_usize(i + 1),
            //             &circuits,
            //         )?;
            //         clock.verify(&circuits)?;
            //         if index == 0 {
            //             info!(
            //                 "proof length of depth {i} = {}",
            //                 clock.proof.to_bytes().len()
            //             )
            //         }
            //         clocks.push(clock)
            //     }
            // }
            // for _ in 0..1000 {
            //     use rand::seq::SliceRandom;
            //     let clock1 = clocks.choose(&mut rand::thread_rng()).unwrap();
            //     let clock2 = clocks.choose(&mut rand::thread_rng()).unwrap();
            //     info!(
            //         "merge {:?}@{} and {:?}@{}",
            //         clock1.counters(),
            //         clock1.depth,
            //         clock2.counters(),
            //         clock2.depth
            //     );
            //     let clock = clock1.merge(clock2, &circuits)?;
            //     info!("merged into {:?}@{}", clock.counters(), clock.depth);
            //     clocks.push(clock)
            // }
            anyhow::Result::<_>::Ok(())
        })?;

    Ok(())
}

pub fn parse_hex_u64(src: &str) -> anyhow::Result<u64> {
    let src = src.strip_prefix("0x").unwrap_or(src);
    u64::from_str_radix(src, 16).map_err(Into::into)
}

// fn parse_range_usize(src: &str) -> anyhow::Result<RangeInclusive<usize>> {
//     if let Some((left, right)) = src.split_once("..=") {
//         Ok(RangeInclusive::new(
//             usize::from_str(left)?,
//             usize::from_str(right)?,
//         ))
//     } else if let Some((left, right)) = src.split_once("..") {
//         Ok(RangeInclusive::new(
//             usize::from_str(left)?,
//             if right.is_empty() {
//                 usize::MAX
//             } else {
//                 usize::from_str(right)?.saturating_sub(1)
//             },
//         ))
//     } else {
//         let value = usize::from_str(src)?;
//         Ok(RangeInclusive::new(value, value))
//     }
// }
