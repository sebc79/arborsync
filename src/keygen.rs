use std::path::PathBuf;

use arborsync_core::{format_hex_key, write_static_key};

pub fn run(out: Option<PathBuf>) -> anyhow::Result<()> {
    let path = out.ok_or_else(|| anyhow::anyhow!("--out is required"))?;
    let public = write_static_key(&path)?;
    println!("{}", format_hex_key(&public));
    Ok(())
}
