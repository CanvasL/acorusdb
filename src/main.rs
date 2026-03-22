mod storage;

use std::{io::Result, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::Mutex,
};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:34254").await?;
    let addr = listener.local_addr()?;

    println!("AcorusDB listening on {addr}");
    println!("Commands: SET key value | GET key | DEL key | EXIT");

    let storage: Arc<Mutex<storage::Storage>> = Arc::new(Mutex::new(storage::Storage::new()));

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let storage = Arc::clone(&storage);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, storage).await {
                        eprintln!("Connection {peer_addr} failed: {err}");
                    }
                });
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
}

async fn handle_connection(stream: TcpStream, storage: Arc<Mutex<storage::Storage>>) -> Result<()> {
    let peer_addr = stream.peer_addr().ok();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    if let Some(peer_addr) = peer_addr {
        println!("Accepted connection from {peer_addr}");
    }

    write_response(&mut writer, "WELCOME AcorusDB").await?;

    while let Some(line) = lines.next_line().await? {
        let command = line.trim();
        if command.is_empty() {
            continue;
        }

        let (cmd, args) = split_command(command);
        match cmd.to_ascii_uppercase().as_str() {
            "SET" => {
                if let Some((key, value)) = split_once_whitespace(args) {
                    let mut storage = storage.lock().await;
                    storage.set(key, value);
                    drop(storage);
                    write_response(&mut writer, "OK").await?;
                } else {
                    write_response(&mut writer, "ERR usage: SET key value").await?;
                }
            }
            "GET" => {
                if let Some(key) = single_arg(args) {
                    let value = {
                        let storage = storage.lock().await;
                        storage.get(key).cloned()
                    };

                    match value {
                        Some(value) => write_response(&mut writer, &value).await?,
                        None => write_response(&mut writer, "(nil)").await?,
                    }
                } else {
                    write_response(&mut writer, "ERR usage: GET key").await?;
                }
            }
            "DEL" => {
                if let Some(key) = single_arg(args) {
                    let deleted = {
                        let mut storage = storage.lock().await;
                        storage.delete(key)
                    };

                    write_response(&mut writer, if deleted { "1" } else { "0" }).await?;
                } else {
                    write_response(&mut writer, "ERR usage: DEL key").await?;
                }
            }
            "EXIT" => {
                write_response(&mut writer, "BYE").await?;
                break;
            }
            _ => write_response(&mut writer, "ERR unknown command").await?,
        }
    }

    if let Some(peer_addr) = peer_addr {
        println!("Closed connection from {peer_addr}");
    }

    Ok(())
}

async fn write_response(writer: &mut OwnedWriteHalf, message: &str) -> Result<()> {
    writer.write_all(message.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}

fn split_command(input: &str) -> (&str, &str) {
    split_once_whitespace(input).unwrap_or((input, ""))
}

fn split_once_whitespace(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim();
    let split_at = trimmed.find(char::is_whitespace)?;
    let (head, tail) = trimmed.split_at(split_at);
    Some((head, tail.trim_start()))
}

fn single_arg(input: &str) -> Option<&str> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
        return None;
    }

    Some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::{single_arg, split_command, split_once_whitespace};

    #[test]
    fn split_command_keeps_set_payload_intact() {
        assert_eq!(
            split_command("SET greeting hello world"),
            ("SET", "greeting hello world")
        );
    }

    #[test]
    fn split_once_whitespace_trims_leading_spaces_in_tail() {
        assert_eq!(
            split_once_whitespace("key   spaced value"),
            Some(("key", "spaced value"))
        );
    }

    #[test]
    fn single_arg_rejects_extra_tokens() {
        assert_eq!(single_arg("name"), Some("name"));
        assert_eq!(single_arg("name extra"), None);
    }
}
