//! Live schedule runner. Public surface is [`Bin`], [`Limits`], [`Schedule`],
//! [`Verdict`], [`Finding`], [`execute`], and [`replay`].

mod campaign;
mod cli;
mod oracle;
mod schedule;
mod types;
mod world;

pub use campaign::{execute, replay};
pub use schedule::{Bin, Limits, Schedule};
pub use types::{Finding, HarnessError, Verdict};

#[doc(hidden)]
pub fn arborsync_fuzz_main() -> i32 {
    cli::main_from_env()
}
