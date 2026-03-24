use std::sync::Arc;

use crate::{
    database::Database,
    error::Result,
    protocol,
};

use tokio::{
    io::{
        AsyncBufReadExt,
        BufReader,
    },
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
            Ok(Some(protocol::Request::Ping)) => protocol::Response::Pong,
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{
                AtomicU64,
                Ordering,
            },
        },
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use tokio::{
        io::{
            AsyncBufReadExt,
            AsyncWriteExt,
            BufReader,
        },
        net::{
            TcpListener,
            TcpStream,
        },
        sync::broadcast,
        task::JoinHandle,
    };

    use super::run;
    use crate::{
        database::Database,
        error::Result,
        protocol,
    };

    #[tokio::test]
    async fn session_round_trip_supports_ping_exists_and_exit() -> Result<()> {
        let server = TestServer::spawn().await?;
        let (reader, mut writer) = connect_lines(server.addr).await?;
        let mut lines = BufReader::new(reader).lines();

        assert_eq!(
            lines.next_line().await?,
            Some(protocol::WELCOME_LINE.to_string())
        );

        writer.write_all(b"PING\n").await?;
        assert_eq!(lines.next_line().await?, Some("PONG".to_string()));

        writer.write_all(b"EXISTS missing\n").await?;
        assert_eq!(lines.next_line().await?, Some("0".to_string()));

        writer.write_all(b"SET name acorus db\n").await?;
        assert_eq!(lines.next_line().await?, Some("OK".to_string()));

        writer.write_all(b"EXISTS name\n").await?;
        assert_eq!(lines.next_line().await?, Some("1".to_string()));

        writer.write_all(b"GET name\n").await?;
        assert_eq!(lines.next_line().await?, Some("acorus db".to_string()));

        writer.write_all(b"DEL name\n").await?;
        assert_eq!(lines.next_line().await?, Some("1".to_string()));

        writer.write_all(b"EXISTS name\n").await?;
        assert_eq!(lines.next_line().await?, Some("0".to_string()));

        writer.write_all(b"GET name\n").await?;
        assert_eq!(lines.next_line().await?, Some("(nil)".to_string()));

        writer.write_all(b"EXIT\n").await?;
        assert_eq!(lines.next_line().await?, Some("BYE".to_string()));
        assert_eq!(lines.next_line().await?, None);

        server.finish().await?;

        Ok(())
    }

    #[tokio::test]
    async fn session_shutdown_sends_bye_to_client() -> Result<()> {
        let server = TestServer::spawn().await?;
        let (reader, _writer) = connect_lines(server.addr).await?;
        let mut lines = BufReader::new(reader).lines();

        assert_eq!(
            lines.next_line().await?,
            Some(protocol::WELCOME_LINE.to_string())
        );

        let _ = server.shutdown_tx.send(());

        assert_eq!(lines.next_line().await?, Some("BYE".to_string()));
        assert_eq!(lines.next_line().await?, None);

        server.finish().await?;

        Ok(())
    }

    async fn connect_lines(
        addr: std::net::SocketAddr,
    ) -> Result<(
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
    )> {
        let stream = TcpStream::connect(addr).await?;
        Ok(stream.into_split())
    }

    struct TestServer {
        addr: std::net::SocketAddr,
        shutdown_tx: broadcast::Sender<()>,
        session_task: JoinHandle<Result<()>>,
        _paths: TestPaths,
    }

    impl TestServer {
        async fn spawn() -> Result<Self> {
            let paths = TestPaths::new()?;
            let database = Arc::new(Database::open(
                paths.snapshot_path.as_path(),
                paths.wal_path.as_path(),
                usize::MAX,
            )?);
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let (shutdown_tx, _) = broadcast::channel(1);
            let shutdown_rx = shutdown_tx.subscribe();

            let session_task = tokio::spawn(async move {
                let (stream, _) = listener.accept().await?;
                run(stream, database, shutdown_rx).await
            });

            Ok(Self {
                addr,
                shutdown_tx,
                session_task,
                _paths: paths,
            })
        }

        async fn finish(self) -> Result<()> {
            self.session_task.await.expect("session task panicked")
        }
    }

    struct TestPaths {
        root_dir: PathBuf,
        snapshot_path: PathBuf,
        wal_path: PathBuf,
    }

    impl TestPaths {
        fn new() -> Result<Self> {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let root_dir = std::env::temp_dir().join(format!(
                "acorusdb-session-tests-{}-{timestamp}-{sequence}",
                std::process::id()
            ));

            fs::create_dir_all(&root_dir)?;

            Ok(Self {
                snapshot_path: root_dir.join("data.snapshot"),
                wal_path: root_dir.join("data.wal"),
                root_dir,
            })
        }
    }

    impl Drop for TestPaths {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }
}
