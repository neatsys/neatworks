use std::{fs::write, path::Path};

use boson::{index_secret, public_key, Clock};
use plonky2::plonk::circuit_data::CircuitConfig;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let (clock, circuit) = Clock::genesis(
        [(); 4].map({
            let mut i = 0;
            move |()| {
                let secret = index_secret(i);
                i += 1;
                public_key(secret)
            }
        }),
        CircuitConfig::standard_ecc_config(),
    )?;
    write(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("genesis_clock4.bin"),
        clock.to_bytes(),
    )?;
    write(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("circuit4.bin"),
        circuit.to_bytes()?,
    )?;
    Ok(())
}
