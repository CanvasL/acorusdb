use std::{
    fs,
    path::{
        Path,
        PathBuf,
    },
};

use serde::Deserialize;

use crate::error::{
    AcorusError,
    AcorusResult,
};

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
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
pub struct SSTableConfig {
    pub dir: PathBuf,
    pub prefix: String,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data"),
            prefix: "acorusdb".to_string(),
        }
    }
}

impl SSTableConfig {
    pub fn base_path(&self) -> PathBuf {
        self.dir.join(format!("{}.sst", self.prefix))
    }

    fn from_legacy_path(path: &Path) -> Option<Self> {
        let prefix = path.file_stem()?.to_str()?.to_string();
        Some(Self {
            dir: config_parent_dir(path),
            prefix,
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

    fn from_legacy_path(path: &Path) -> Option<(PathBuf, String)> {
        let prefix = path.file_stem()?.to_str()?.to_string();
        Some((config_parent_dir(path), prefix))
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct RawConfig {
    server: ServerConfig,
    logging: LoggingConfig,
    storage: Option<SharedStorageConfig>,
    sstable: RawSSTableConfig,
    wal: RawWalConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct SharedStorageConfig {
    dir: Option<PathBuf>,
    prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct RawSSTableConfig {
    dir: Option<PathBuf>,
    prefix: Option<String>,
    path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct RawWalConfig {
    dir: Option<PathBuf>,
    prefix: Option<String>,
    path: Option<PathBuf>,
    flush_threshold_entries: usize,
}

impl Default for RawWalConfig {
    fn default() -> Self {
        Self {
            dir: None,
            prefix: None,
            path: None,
            flush_threshold_entries: WalConfig::default().flush_threshold_entries,
        }
    }
}

impl From<RawConfig> for Config {
    fn from(raw: RawConfig) -> Self {
        let shared = raw.storage;
        let sstable = build_sstable_config(raw.sstable, shared.as_ref());
        let wal = build_wal_config(raw.wal, shared.as_ref());

        Self {
            server: raw.server,
            logging: raw.logging,
            sstable,
            wal,
        }
    }
}

fn build_sstable_config(
    raw: RawSSTableConfig,
    shared: Option<&SharedStorageConfig>,
) -> SSTableConfig {
    let defaults = SSTableConfig::default();
    let legacy = raw
        .path
        .as_deref()
        .and_then(SSTableConfig::from_legacy_path);

    SSTableConfig {
        dir: raw
            .dir
            .or_else(|| shared.and_then(|storage| storage.dir.clone()))
            .or_else(|| legacy.as_ref().map(|config| config.dir.clone()))
            .unwrap_or(defaults.dir),
        prefix: raw
            .prefix
            .or_else(|| shared.and_then(|storage| storage.prefix.clone()))
            .or_else(|| legacy.as_ref().map(|config| config.prefix.clone()))
            .unwrap_or(defaults.prefix),
    }
}

fn build_wal_config(raw: RawWalConfig, shared: Option<&SharedStorageConfig>) -> WalConfig {
    let defaults = WalConfig::default();
    let legacy = raw.path.as_deref().and_then(WalConfig::from_legacy_path);

    WalConfig {
        dir: raw
            .dir
            .or_else(|| shared.and_then(|storage| storage.dir.clone()))
            .or_else(|| legacy.as_ref().map(|(dir, _)| dir.clone()))
            .unwrap_or(defaults.dir),
        prefix: raw
            .prefix
            .or_else(|| shared.and_then(|storage| storage.prefix.clone()))
            .or_else(|| legacy.as_ref().map(|(_, prefix)| prefix.clone()))
            .unwrap_or(defaults.prefix),
        flush_threshold_entries: raw.flush_threshold_entries,
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

        Self::from_toml_str(&raw, path).map(|config| (config, true))
    }

    fn from_toml_str(raw: &str, path: &Path) -> AcorusResult<Self> {
        let raw_config: RawConfig =
            toml::from_str(raw).map_err(|error| AcorusError::ConfigParse {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        Ok(raw_config.into())
    }
}

fn config_parent_dir(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::Config;

    #[test]
    fn default_paths_are_derived_from_separate_sstable_and_wal_config() {
        let config = Config::default();

        assert_eq!(config.sstable.base_path(), Path::new("data/acorusdb.sst"));
        assert_eq!(config.wal.path(), Path::new("data/acorusdb.wal"));
        assert_eq!(config.wal.flush_threshold_entries, 1024);
    }

    #[test]
    fn parses_separate_sstable_and_wal_config() {
        let config = Config::from_toml_str(
            r#"
[sstable]
dir = "db/sst"
prefix = "main"

[wal]
dir = "db/wal"
prefix = "main-log"
flush_threshold_entries = 64
"#,
            Path::new("acorusdb.toml"),
        )
        .expect("config should parse");

        assert_eq!(config.sstable.base_path(), Path::new("db/sst/main.sst"));
        assert_eq!(config.wal.path(), Path::new("db/wal/main-log.wal"));
        assert_eq!(config.wal.flush_threshold_entries, 64);
    }

    #[test]
    fn parses_shared_storage_config_as_fallback() {
        let config = Config::from_toml_str(
            r#"
[storage]
dir = "db"
prefix = "shared"

[wal]
flush_threshold_entries = 32
"#,
            Path::new("acorusdb.toml"),
        )
        .expect("shared storage config should parse");

        assert_eq!(config.sstable.base_path(), Path::new("db/shared.sst"));
        assert_eq!(config.wal.path(), Path::new("db/shared.wal"));
        assert_eq!(config.wal.flush_threshold_entries, 32);
    }

    #[test]
    fn parses_legacy_path_config_into_separate_layouts() {
        let config = Config::from_toml_str(
            r#"
[sstable]
path = "data/sstable/acorusdb.sst"

[wal]
path = "data/wal/acorusdb.wal"
flush_threshold_entries = 16
"#,
            Path::new("acorusdb.toml"),
        )
        .expect("legacy config should parse");

        assert_eq!(config.sstable.dir, Path::new("data/sstable"));
        assert_eq!(config.sstable.prefix, "acorusdb");
        assert_eq!(config.wal.dir, Path::new("data/wal"));
        assert_eq!(config.wal.prefix, "acorusdb");
        assert_eq!(config.wal.flush_threshold_entries, 16);
    }
}
