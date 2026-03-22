mod cli;
mod command;
mod config;
mod database;
mod protocol;
mod session;
mod shutdown;
mod snapshot;
mod storage_engine;
mod wal;

use std::{
    io::Result,
    sync::Arc,
};

use tokio::{
    net::TcpListener,
    sync::broadcast,
    task::JoinSet,
};
use tracing_subscriber::EnvFilter;

use crate::{
    cli::Cli,
    shutdown::wait_for_shutdown_signal,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse_args();
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

    let (shutdown_tx, _) = broadcast::channel(1);
    let mut sessions = JoinSet::new();
    let shutdown_signal = wait_for_shutdown_signal();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            signal = &mut shutdown_signal => {
                let signal = signal?;
                tracing::info!(signal = %signal.as_str(), "received shutdown signal");
                break;
            }
            accept_result = listener.accept() => match accept_result {
                Ok((stream, peer_addr)) => {
                    let database = Arc::clone(&database);
                    let shutdown_rx = shutdown_tx.subscribe();
                    sessions.spawn(async move {
                        if let Err(err) = session::run(stream, database, shutdown_rx).await {
                            tracing::error!(%peer_addr, error = %err, "connection failed");
                        }
                    });
                }
                Err(err) => tracing::error!(error = %err, "failed to accept connection"),
            }
        }
    }

    drop(listener);
    let _ = shutdown_tx.send(());
    drop(shutdown_tx);

    while let Some(result) = sessions.join_next().await {
        if let Err(err) = result {
            tracing::error!(error = %err, "session task failed");
        }
    }

    tracing::info!("shutdown complete");

    Ok(())
}
