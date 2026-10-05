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

/// Process entry for the `arborsync-fuzz` binary. The bin crate cannot call
/// `pub(crate)` items, so this is the only extra export.
#[doc(hidden)]
pub fn arborsync_fuzz_main() -> i32 {
    cli::main_from_env()
}
