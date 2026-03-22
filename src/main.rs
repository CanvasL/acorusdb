mod command;
mod storage_engine;
mod wal_entry;

use std::{io::Result, path::Path, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::Mutex,
};
use tracing::{error, info};
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
    info!("Commands: SET key value | GET key | DEL key | EXIT/QUIT");

    let storage_engine = storage_engine::StorageEngine::open(Path::new("acorusdb.db"))?;
    let storage_engine: Arc<Mutex<storage_engine::StorageEngine>> =
        Arc::new(Mutex::new(storage_engine));

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let storage_engine = Arc::clone(&storage_engine);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, storage_engine).await {
                        error!(%peer_addr, error = %err, "connection failed");
                    }
                });
            }
            Err(err) => error!(error = %err, "failed to accept connection"),
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    storage_engine: Arc<Mutex<storage_engine::StorageEngine>>,
) -> Result<()> {
    let peer_addr = stream.peer_addr().ok();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    if let Some(peer_addr) = peer_addr {
        info!(%peer_addr, "accepted connection");
    }

    write_response(&mut writer, "WELCOME AcorusDB").await?;

    while let Some(line) = lines.next_line().await? {
        let command = command::Command::parse(&line);

        let response = {
            let mut storage_engine = storage_engine.lock().await;
            command.execute(&mut storage_engine)
        }?;
        write_response(&mut writer, &response).await?;
    }

    if let Some(peer_addr) = peer_addr {
        info!(%peer_addr, "closed connection");
    }

    Ok(())
}

async fn write_response(writer: &mut OwnedWriteHalf, message: &str) -> Result<()> {
    writer.write_all(message.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}
