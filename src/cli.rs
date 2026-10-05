use std::path::PathBuf;

use clap::{Parser, Subcommand};

use arborsync_core::path::CanonicalPath;

use crate::{keygen, master, path_dump, recompute, restore, slave};

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
    /// Copy a directory onto one central prefix while the master is stopped
    Restore {
        #[arg(long)]
        config: Option<PathBuf>,
        /// Directory whose root is `--prefix`. `source/foo` becomes `{prefix}/foo`.
        #[arg(long)]
        source: PathBuf,
        /// Canonical prefix. `/` is the whole central tree.
        #[arg(long, default_value = "/", value_parser = parse_prefix)]
        prefix: CanonicalPath,
        /// Print pulls and write nothing. No output means a slave holding
        /// `--source` would only push.
        #[arg(long)]
        pretend: bool,
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
            Command::Restore {
                config,
                source,
                prefix,
                pretend,
            } => restore::run(resolve_config(config), source, prefix, pretend),
            Command::Recompute { config } => recompute::run(resolve_config(config)),
            Command::Path { config, path } => path_dump::run(resolve_config(config), path),
        }
    }
}

fn parse_prefix(raw: &str) -> Result<CanonicalPath, String> {
    CanonicalPath::parse(raw).map_err(|err| err.to_string())
}

fn resolve_config(explicit: Option<PathBuf>) -> Option<PathBuf> {
    explicit.or_else(|| std::env::var_os("ARBORSYNC_CONFIG").map(PathBuf::from))
}
