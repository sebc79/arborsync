mod cli;
mod keygen;
mod master;
mod slave;

use clap::Parser;

use cli::Cli;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logger();
    cli.run()
}

fn init_logger() {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    if let Ok(level) = std::env::var("ARBORSYNC_LOG_LEVEL") {
        builder.parse_filters(&level);
    }
    let _ = builder.try_init();
}
