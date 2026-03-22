use std::{io::Result, sync::Arc};

use crate::{database::Database, protocol};

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    sync::broadcast,
};

#[derive(Debug, Clone, Copy)]
enum CloseReason {
    ClientExit,
    PeerClosed,
    Shutdown,
}

impl CloseReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ClientExit => "client_exit",
            Self::PeerClosed => "peer_closed",
            Self::Shutdown => "shutdown",
        }
    }
}

pub async fn run(
    stream: TcpStream,
    database: Arc<Database>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let peer_addr = stream.peer_addr().ok();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    if let Some(peer_addr) = peer_addr {
        tracing::info!(%peer_addr, "accepted connection");
    }

    protocol::write_line(&mut writer, protocol::WELCOME_LINE).await?;

    let close_reason = loop {
        let line = tokio::select! {
            result = lines.next_line() => result?,
            _ = shutdown.recv() => {
                let _ = protocol::write_response(&mut writer, &protocol::Response::Bye).await;
                break CloseReason::Shutdown;
            }
        };

        let Some(line) = line else {
            break CloseReason::PeerClosed;
        };

        let response = match protocol::parse_request(&line) {
            Ok(Some(protocol::Request::Command(command))) => {
                protocol::Response::from(database.execute(command).await?)
            }
            Ok(Some(protocol::Request::Exit)) => protocol::Response::Bye,
            Ok(None) => continue,
            Err(error) => protocol::Response::Error(error),
        };
        let should_close = response.should_close();
        protocol::write_response(&mut writer, &response).await?;
        if should_close {
            break CloseReason::ClientExit;
        }
    };

    if let Some(peer_addr) = peer_addr {
        tracing::info!(%peer_addr, reason = %close_reason.as_str(), "closed connection");
    }

    Ok(())
}
