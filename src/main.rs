mod command;
mod database;
mod protocol;
mod session;
mod storage_engine;
mod wal_entry;

use std::{
    io::Result,
    path::Path,
    sync::Arc,
};

use tokio::net::TcpListener;
use tracing::{
    error,
    info,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let listener = TcpListener::bind("127.0.0.1:7634").await?;
    let addr = listener.local_addr()?;

    info!(%addr, "AcorusDB listening");
    info!("Commands: {}", protocol::COMMANDS_BANNER);

    let database = Arc::new(database::Database::open(Path::new("acorusdb.wal"))?);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let database = Arc::clone(&database);
                tokio::spawn(async move {
                    if let Err(err) = session::run(stream, database).await {
                        error!(%peer_addr, error = %err, "connection failed");
                    }
                });
            }
            Err(err) => error!(error = %err, "failed to accept connection"),
        }
    }
}
