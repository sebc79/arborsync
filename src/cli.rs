use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::{keygen, master, slave};

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
}

impl Cli {
    pub fn run(self) -> anyhow::Result<()> {
        match self.command {
            Command::Master { config } => master::run(resolve_config(config)),
            Command::Slave { config } => slave::run(resolve_config(config)),
            Command::Keygen { out } => keygen::run(&out),
        }
    }
}

fn resolve_config(explicit: Option<PathBuf>) -> Option<PathBuf> {
    explicit.or_else(|| std::env::var_os("ARBORSYNC_CONFIG").map(PathBuf::from))
}
