use std::path::PathBuf;

use serde::Deserialize;

use super::types::{
    Config,
    LoggingConfig,
    ManifestConfig,
    SSTableConfig,
    ServerConfig,
    WalConfig,
};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub(super) struct RawConfig {
    server: ServerConfig,
    logging: LoggingConfig,
    storage: Option<SharedStorageConfig>,
    manifest: RawManifestConfig,
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
struct RawManifestConfig {
    dir: Option<PathBuf>,
    prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct RawSSTableConfig {
    dir: Option<PathBuf>,
    prefix: Option<String>,
    path: Option<PathBuf>,
    compact_threshold_bytes: u64,
}

impl Default for RawSSTableConfig {
    fn default() -> Self {
        Self {
            dir: None,
            prefix: None,
            path: None,
            compact_threshold_bytes: SSTableConfig::default().compact_threshold_bytes,
        }
    }
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
        let manifest = build_manifest_config(raw.manifest, shared.as_ref());
        let sstable = build_sstable_config(raw.sstable, shared.as_ref());
        let wal = build_wal_config(raw.wal, shared.as_ref());

        Self {
            server: raw.server,
            logging: raw.logging,
            manifest,
            sstable,
            wal,
        }
    }
}

fn build_manifest_config(
    raw: RawManifestConfig,
    shared: Option<&SharedStorageConfig>,
) -> ManifestConfig {
    let defaults = ManifestConfig::default();

    ManifestConfig {
        dir: raw
            .dir
            .or_else(|| shared.and_then(|storage| storage.dir.clone()))
            .unwrap_or(defaults.dir),
        prefix: raw
            .prefix
            .or_else(|| shared.and_then(|storage| storage.prefix.clone()))
            .unwrap_or(defaults.prefix),
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
        compact_threshold_bytes: raw.compact_threshold_bytes,
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
