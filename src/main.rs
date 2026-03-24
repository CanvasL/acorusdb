mod cli;

use acorusdb::{config::Config, error::AcorusResult, server};
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> AcorusResult<()> {
    let cli = Cli::parse_args();
    let (config, loaded_from_file) = Config::load(cli.config.as_path())?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new(&config.logging.level))
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    if loaded_from_file {
        tracing::info!(config_path = %cli.config.display(), "loaded configuration");
    } else {
        tracing::info!(
            config_path = %cli.config.display(),
            "configuration file not found, using default configuration"
        );
    }

    server::run(config).await
}
