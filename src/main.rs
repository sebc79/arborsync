mod cli;
mod keygen;
mod master;
mod reload;
mod slave;
mod status;
mod watch;

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
    } else {
        builder.filter_level(log::LevelFilter::Trace);
    }
    let _ = builder.try_init();
    if std::env::var_os("ARBORSYNC_LOG_LEVEL").is_none() {
        log::set_max_level(log::LevelFilter::Info);
    }
}
