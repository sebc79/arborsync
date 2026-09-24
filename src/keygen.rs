use std::path::Path;

use arborsync_core::{format_hex_key, write_static_key};

pub fn run(out: &Path) -> anyhow::Result<()> {
    let public = write_static_key(out)?;
    println!("{}", format_hex_key(&public));
    Ok(())
}
