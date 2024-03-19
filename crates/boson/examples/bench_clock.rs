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

            let circuit = ClockCircuit::new(4, config)?;
            let mut clock = Clock::genesis(&circuit);
            for i in 0..10 {
                clock = clock.increment(0, F::from_canonical_usize(i + 1), &circuit)?;
                clock.verify(&circuit)?
            }
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
