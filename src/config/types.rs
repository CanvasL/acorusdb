use std::path::{
    Path,
    PathBuf,
};

use serde::Deserialize;

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub manifest: ManifestConfig,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestConfig {
    pub dir: PathBuf,
    pub prefix: String,
}

impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
            prefix: "manifest".to_string(),
        }
    }
}

impl ManifestConfig {
    pub fn path(&self) -> PathBuf {
        self.dir.join(format!("{}.toml", self.prefix))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableConfig {
    pub dir: PathBuf,
    pub prefix: String,
    pub compact_threshold_bytes: u64,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
            prefix: "acorusdb".to_string(),
            compact_threshold_bytes: 4 * 1024 * 1024,
        }
    }
}

impl SSTableConfig {
    pub fn base_path(&self) -> PathBuf {
        self.dir.join(format!("{}.sst", self.prefix))
    }

    pub(super) fn from_legacy_path(path: &Path) -> Option<Self> {
        let prefix = path.file_stem()?.to_str()?.to_string();
        Some(Self {
            dir: config_parent_dir(path),
            prefix,
            compact_threshold_bytes: Self::default().compact_threshold_bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub prefix: String,
    pub flush_threshold_entries: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
            prefix: "acorusdb".to_string(),
            flush_threshold_entries: 1024,
        }
    }
}

impl WalConfig {
    pub fn path(&self) -> PathBuf {
        self.dir.join(format!("{}.wal", self.prefix))
    }

    pub(super) fn from_legacy_path(path: &Path) -> Option<(PathBuf, String)> {
        let prefix = path.file_stem()?.to_str()?.to_string();
        Some((config_parent_dir(path), prefix))
    }
}

fn config_parent_dir(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}
