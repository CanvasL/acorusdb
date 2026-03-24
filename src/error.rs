use std::{io, path::PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum AcorusError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("failed to read config file {path}: {source}", path = .path.display())]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to parse config file {path}: {message}", path = .path.display())]
    ConfigParse { path: PathBuf, message: String },

    #[error("failed to create parent directory for {path}: {source}", path = .path.display())]
    CreateParentDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to bind server to {addr}: {source}")]
    Bind {
        addr: String,
        #[source]
        source: io::Error,
    },

    #[error("failed to install shutdown signal handler: {0}")]
    ShutdownSignal(#[source] io::Error),

    #[error("failed to open WAL file {path}: {source}", path = .path.display())]
    WalOpen {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to read WAL file {path}: {source}", path = .path.display())]
    WalRead {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to write WAL file {path}: {source}", path = .path.display())]
    WalWrite {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to reset WAL file {path}: {source}", path = .path.display())]
    WalReset {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("corrupted WAL file {path} at line {line}: {message}", path = .path.display())]
    CorruptedWal {
        path: PathBuf,
        line: usize,
        message: String,
    },

    #[error("failed to encode sstable {path}: {message}", path = .path.display())]
    SSTableEncode { path: PathBuf, message: String },

    #[error("failed to write sstable {path}: {source}", path = .path.display())]
    SSTableWrite {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to read sstable {path}: {source}", path = .path.display())]
    SSTableRead {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("failed to decode sstable {path}: {message}", path = .path.display())]
    SSTableDecode { path: PathBuf, message: String },
}

pub type AcorusResult<T> = std::result::Result<T, AcorusError>;
