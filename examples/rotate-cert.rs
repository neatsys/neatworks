use std::{fs::write, path::Path};

fn main() -> anyhow::Result<()> {
    let certified_key = rcgen::generate_simple_self_signed(vec!["neatworks.quic".into()])?;
    write(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/cert.pem"),
        certified_key.cert.pem(),
    )?;
    write(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/key.pem"),
        certified_key.key_pair.serialize_pem(),
    )?;
    Ok(())
}
