use std::{
    fs,
    path::Path,
};

use crate::error::{
    AcorusError,
    AcorusResult,
};

use super::{
    Config,
    raw::RawConfig,
};

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

    pub(crate) fn from_toml_str(raw: &str, path: &Path) -> AcorusResult<Self> {
        let raw_config: RawConfig =
            toml::from_str(raw).map_err(|error| AcorusError::ConfigParse {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        Ok(raw_config.into())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::config::Config;

    #[test]
    fn default_paths_are_derived_from_separate_sstable_and_wal_config() {
        let config = Config::default();

        assert_eq!(config.sstable.base_path(), Path::new("data/acorusdb.sst"));
        assert_eq!(config.sstable.compact_threshold_bytes, 4 * 1024 * 1024);
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
compact_threshold_bytes = 2048

[wal]
dir = "db/wal"
prefix = "main-log"
flush_threshold_entries = 64
"#,
            Path::new("acorusdb.toml"),
        )
        .expect("config should parse");

        assert_eq!(config.sstable.base_path(), Path::new("db/sst/main.sst"));
        assert_eq!(config.sstable.compact_threshold_bytes, 2048);
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
        assert_eq!(config.sstable.compact_threshold_bytes, 4 * 1024 * 1024);
        assert_eq!(config.wal.path(), Path::new("db/shared.wal"));
        assert_eq!(config.wal.flush_threshold_entries, 32);
    }

    #[test]
    fn parses_legacy_path_config_into_separate_layouts() {
        let config = Config::from_toml_str(
            r#"
[sstable]
path = "data/sstable/acorusdb.sst"
compact_threshold_bytes = 4096

[wal]
path = "data/wal/acorusdb.wal"
flush_threshold_entries = 16
"#,
            Path::new("acorusdb.toml"),
        )
        .expect("legacy config should parse");

        assert_eq!(config.sstable.dir, Path::new("data/sstable"));
        assert_eq!(config.sstable.prefix, "acorusdb");
        assert_eq!(config.sstable.compact_threshold_bytes, 4096);
        assert_eq!(config.wal.dir, Path::new("data/wal"));
        assert_eq!(config.wal.prefix, "acorusdb");
        assert_eq!(config.wal.flush_threshold_entries, 16);
    }
}
