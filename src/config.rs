use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::error::{AcorusError, AcorusResult};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    #[serde(alias = "snapshot")]
    pub sstable: SSTableConfig,
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
pub struct SSTableConfig {
    pub path: PathBuf,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("acorusdb.sst"),
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
    pub fn load(path: &Path) -> AcorusResult<(Self, bool)> {
        if !path.exists() {
            return Ok((Self::default(), false));
        }

        let raw = fs::read_to_string(path).map_err(|source| AcorusError::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = toml::from_str(&raw).map_err(|error| AcorusError::ConfigParse {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

        Ok((config, true))
    }
}
