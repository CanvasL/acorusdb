use std::sync::Arc;

use tokio::{
    net::TcpListener,
    sync::broadcast,
    task::JoinSet,
};

use crate::{
    config::Config,
    database::Database,
    error::{
        AcorusError,
        AcorusResult,
    },
    session,
    shutdown::wait_for_shutdown_signal,
    storage_engine::{
        StoragePaths,
        StoragePolicy,
    },
};

pub async fn run(config: Config) -> AcorusResult<()> {
    let bind_addr = config.server.bind_addr.clone();
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(|source| AcorusError::Bind {
            addr: bind_addr,
            source,
        })?;
    let addr = listener.local_addr()?;

    tracing::info!(%addr, "acorusdb listening");

    let manifest_path = config.manifest.path();
    let sstable_base_path = config.sstable.base_path();
    let wal_path = config.wal.path();

    let database = Arc::new(Database::open(
        StoragePaths::new(manifest_path, sstable_base_path, wal_path),
        StoragePolicy::new(
            config.wal.flush_threshold_entries,
            config.sstable.compact_threshold_bytes,
        ),
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
