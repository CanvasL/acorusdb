use std::{
    fs,
    io::{
        Error,
        ErrorKind,
        Result,
    },
    path::{
        Path,
        PathBuf,
    },
};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub snapshot: SnapshotConfig,
    pub wal: WalConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:7634".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SnapshotConfig {
    pub path: PathBuf,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("acorusdb.snapshot"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    pub path: PathBuf,
    pub compact_threshold_bytes: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("acorusdb.wal"),
            compact_threshold_bytes: 1024,
        }
    }
}

impl Config {
    /// Loads the configuration from a TOML file. If the file does not exist, returns the default
    /// configuration.
    pub fn load(path: &Path) -> Result<(Self, bool)> {
        if !path.exists() {
            return Ok((Self::default(), false));
        }

        let raw = fs::read_to_string(path)?;
        let config: Self =
            toml::from_str(&raw).map_err(|error| Error::new(ErrorKind::InvalidData, error))?;

        Ok((config, true))
    }
}
