use std::io::Result;
use std::sync::Arc;

use crate::database::Database;
use crate::protocol;

use tokio::{
    io::{
        AsyncBufReadExt,
        BufReader,
    },
    net::TcpStream,
};

use tracing::info;

pub async fn run(stream: TcpStream, database: Arc<Database>) -> Result<()> {
    let peer_addr = stream.peer_addr().ok();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    if let Some(peer_addr) = peer_addr {
        info!(%peer_addr, "accepted connection");
    }

    protocol::write_line(&mut writer, protocol::WELCOME_LINE).await?;

    while let Some(line) = lines.next_line().await? {
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
            break;
        }
    }

    if let Some(peer_addr) = peer_addr {
        info!(%peer_addr, "closed connection");
    }

    Ok(())
}
