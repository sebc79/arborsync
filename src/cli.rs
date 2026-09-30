use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::{keygen, master, path_dump, recompute, slave};

#[derive(Parser, Debug)]
#[command(
    name = "arborsync",
    version,
    about = "Selective subtree file synchronization daemon"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the master daemon
    Master {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Run the slave daemon
    Slave {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Write a 32-byte X25519 secret and print the public key
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Recompute master directory hashes from indexed children
    Recompute {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Print disk, index, directory hash, and last_synced for one path
    Path {
        #[arg(long)]
        config: Option<PathBuf>,
        /// Host path under the central root or a checkout, or a canonical path
        path: PathBuf,
    },
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        match self.command {
            Command::Master { config } => master::run(resolve_config(config)),
            Command::Slave { config } => slave::run(resolve_config(config)),
            Command::Keygen { out } => keygen::run(&out),
            Command::Recompute { config } => recompute::run(resolve_config(config)),
            Command::Path { config, path } => path_dump::run(resolve_config(config), path),
        }
    }
}

fn resolve_config(explicit: Option<PathBuf>) -> Option<PathBuf> {
    explicit.or_else(|| std::env::var_os("ARBORSYNC_CONFIG").map(PathBuf::from))
}
