use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};

use crate::campaign::{execute, replay};
use crate::schedule::{Bin, Limits, Schedule};
use crate::types::{HarnessError, Verdict};

#[derive(Clone, Debug)]
pub(crate) enum Command {
    Run {
        bin: Option<PathBuf>,
        seed: Option<u64>,
        steps: u16,
        slaves: u8,
    },
    Replay {
        bin: Option<PathBuf>,
        artifact: PathBuf,
    },
}

#[derive(Parser, Debug)]
#[command(name = "arborsync-fuzz")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Drive a real master and one or two slaves.
    Run {
        #[arg(long)]
        bin: Option<PathBuf>,
        #[arg(long)]
        seed: Option<u64>,
        #[arg(long, default_value_t = 32)]
        steps: u16,
        #[arg(long, default_value_t = 2)]
        slaves: u8,
    },
    /// Re-run one finding artifact with shrink turned off.
    Replay {
        #[arg(long)]
        bin: Option<PathBuf>,
        artifact: PathBuf,
    },
}

impl From<Cli> for Command {
    fn from(cli: Cli) -> Self {
        match cli.command {
            Cmd::Run {
                bin,
                seed,
                steps,
                slaves,
            } => Command::Run {
                bin,
                seed,
                steps,
                slaves,
            },
            Cmd::Replay { bin, artifact } => Command::Replay { bin, artifact },
        }
    }
}

pub(crate) fn main_from_env() -> i32 {
    match Cli::try_parse() {
        Ok(cli) => main_result(cli.into()),
        Err(err) => {
            let code = err.exit_code();
            let _ = err.print();
            if code == 0 { 0 } else { 2 }
        }
    }
}

/// `0` clean, `1` finding, `2` harness error. Prints the seed before any spawn.
pub(crate) fn main_result(cmd: Command) -> i32 {
    match cmd {
        Command::Run {
            bin,
            seed,
            steps,
            slaves,
        } => {
            let limits = match Limits::new(steps, slaves) {
                Ok(limits) => limits,
                Err(err) => {
                    eprintln!("{err}");
                    return 2;
                }
            };
            let seed = seed.unwrap_or_else(draw_seed);
            println!("seed {seed}");
            let bin = match open_bin(bin) {
                Ok(bin) => bin,
                Err(err) => {
                    eprintln!("{err}");
                    return 2;
                }
            };
            finish(execute(&bin, Schedule::from_seed(seed, limits)))
        }
        Command::Replay { bin, artifact } => {
            let bin = match open_bin(bin) {
                Ok(bin) => bin,
                Err(err) => {
                    eprintln!("{err}");
                    return 2;
                }
            };
            finish(replay(&bin, &artifact))
        }
    }
}

fn finish(result: Result<Verdict, HarnessError>) -> i32 {
    match result {
        Ok(Verdict::Clean) => 0,
        Ok(Verdict::Finding { found, artifact }) => {
            eprintln!("{found} at {}", artifact.display());
            1
        }
        Err(err) => {
            eprintln!("{err}");
            2
        }
    }
}

fn open_bin(path: Option<PathBuf>) -> Result<Bin, HarnessError> {
    Bin::new(path.unwrap_or_else(|| PathBuf::from("arborsync")))
}

fn draw_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(1);
    nanos ^ ((std::process::id() as u64) << 32)
}
