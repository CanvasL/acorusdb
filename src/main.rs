mod command;
mod config;
mod database;
mod protocol;
mod session;
mod snapshot;
mod storage_engine;
mod wal;

use std::{io::Result, path::PathBuf, sync::Arc};

use clap::Parser;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "acorusdb")]
#[command(version)]
#[command(about = "A lightweight TCP key-value database")]
struct Cli {
    #[arg(short, long, default_value = "acorusdb.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let (config, loaded_from_file) = config::Config::load(cli.config.as_path())?;

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

    let listener = TcpListener::bind(&config.server.bind_addr).await?;
    let addr = listener.local_addr()?;

    tracing::info!(%addr, "acorusdb listening");

    let database = Arc::new(database::Database::open(
        config.snapshot.path.as_path(),
        config.wal.path.as_path(),
        config.wal.compact_threshold_bytes,
    )?);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let database = Arc::clone(&database);
                tokio::spawn(async move {
                    if let Err(err) = session::run(stream, database).await {
                        tracing::error!(%peer_addr, error = %err, "connection failed");
                    }
                });
            }
            Err(err) => tracing::error!(error = %err, "failed to accept connection"),
        }
    }
}
