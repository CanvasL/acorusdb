use std::{
    fs::File,
    io::{
        BufReader,
        BufWriter,
        Read,
        Write,
    },
    path::{
        Path,
        PathBuf,
    },
};

use serde::{
    Deserialize,
    Serialize,
};
use toml;

use crate::{
    error::{
        AcorusError,
        AcorusResult,
    },
    fs_utils::{
        ensure_parent_dir,
        parent_dir_for_sync,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ManifestFile {
    version: u64,
    current_sstables: Vec<String>,
}

impl ManifestFile {
    const CURRENT_VERSION: u64 = 1;

    fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            current_sstables: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub path: PathBuf,
    pub version: u64,
    pub current_sstables: Vec<String>,
}

impl Manifest {
    const TMP_EXTENSION: &str = "tmp";

    pub fn load(path: &Path) -> AcorusResult<Self> {
        if !path.exists() {
            let manifest = Self::new(path);
            manifest.save_atomically()?;
        }

        let file = File::open(path).map_err(|source| manifest_read_error(path, source))?;
        let mut reader = ManifestReader::new(path, BufReader::new(file));
        let manifest_file = reader.read_manifest_file()?;

        Ok(Self::from_file(path, manifest_file))
    }

    pub fn new(path: &Path) -> Self {
        Self::from_file(path, ManifestFile::new())
    }

    pub fn save_atomically(&self) -> AcorusResult<()> {
        let manifest_path = self.path.clone();
        ensure_parent_dir(&manifest_path)?;
        let tmp_path = manifest_path.with_extension(Self::TMP_EXTENSION);

        let tmp_file =
            File::create(&tmp_path).map_err(|source| manifest_write_error(&tmp_path, source))?;

        let mut writer = ManifestWriter::new(&tmp_path, BufWriter::new(tmp_file));

        writer.write_manifest_file(&self.to_file())?;
        writer.flush()?;
        drop(writer);

        let file =
            File::open(&tmp_path).map_err(|source| manifest_write_error(&tmp_path, source))?;
        file.sync_all()
            .map_err(|source| manifest_write_error(&tmp_path, source))?;

        std::fs::rename(&tmp_path, &self.path)
            .map_err(|source| manifest_write_error(&manifest_path, source))?;

        let dir = parent_dir_for_sync(&manifest_path);
        let dir_path = dir.to_path_buf();
        let dir_file = File::open(dir).map_err(|source| manifest_write_error(&dir_path, source))?;
        dir_file
            .sync_all()
            .map_err(|source| manifest_write_error(&dir_path, source))?;

        Ok(())
    }

    fn from_file(path: &Path, file: ManifestFile) -> Self {
        Self {
            path: path.to_path_buf(),
            version: file.version,
            current_sstables: file.current_sstables,
        }
    }

    fn to_file(&self) -> ManifestFile {
        ManifestFile {
            version: self.version,
            current_sstables: self.current_sstables.clone(),
        }
    }
}

struct ManifestReader<'a, R> {
    path: &'a Path,
    reader: R,
}

impl<'a, R: Read> ManifestReader<'a, R> {
    fn new(path: &'a Path, reader: R) -> Self {
        Self { path, reader }
    }

    fn read_manifest_file(&mut self) -> AcorusResult<ManifestFile> {
        let mut content = String::new();
        self.reader
            .read_to_string(&mut content)
            .map_err(|source| manifest_read_error(self.path, source))?;

        toml::from_str(&content).map_err(|source| AcorusError::ManifestLoad {
            path: self.path.to_path_buf(),
            source,
        })
    }
}

struct ManifestWriter<'a, W> {
    path: &'a Path,
    writer: W,
}

impl<'a, W: Write> ManifestWriter<'a, W> {
    fn new(path: &'a Path, writer: W) -> Self {
        Self { path, writer }
    }

    fn write_manifest_file(&mut self, manifest: &ManifestFile) -> AcorusResult<()> {
        let content = toml::to_string(manifest).map_err(|source| AcorusError::ManifestParse {
            path: self.path.to_path_buf(),
            source,
        })?;

        self.write_all(content.as_bytes())
    }

    fn flush(&mut self) -> AcorusResult<()> {
        self.writer
            .flush()
            .map_err(|source| manifest_write_error(self.path, source))
    }

    fn write_all(&mut self, bytes: &[u8]) -> AcorusResult<()> {
        self.writer
            .write_all(bytes)
            .map_err(|source| manifest_write_error(self.path, source))
    }
}

fn manifest_write_error(path: &Path, source: std::io::Error) -> AcorusError {
    AcorusError::ManifestWrite {
        path: path.to_path_buf(),
        source,
    }
}

fn manifest_read_error(path: &Path, source: std::io::Error) -> AcorusError {
    AcorusError::ManifestRead {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{
            AtomicU64,
            Ordering,
        },
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use super::Manifest;
    use crate::error::AcorusResult;

    #[test]
    fn load_creates_missing_parent_directories() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("nested/state/manifest.toml");

        let manifest = Manifest::load(&manifest_path)?;

        assert_eq!(manifest.version, 1);
        assert!(manifest.current_sstables.is_empty());
        assert!(manifest_path.exists());

        fs::remove_dir_all(root_dir)?;

        Ok(())
    }

    #[test]
    fn save_then_load_round_trip_preserves_fields() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("manifest.toml");
        let mut manifest = Manifest::new(&manifest_path);
        manifest.current_sstables = vec![
            root_dir
                .join("data-000001.sst")
                .to_string_lossy()
                .to_string(),
            root_dir
                .join("data-000002.sst")
                .to_string_lossy()
                .to_string(),
        ];

        manifest.save_atomically()?;

        let loaded = Manifest::load(&manifest_path)?;

        assert_eq!(loaded.version, manifest.version);
        assert_eq!(loaded.current_sstables, manifest.current_sstables);

        fs::remove_dir_all(root_dir)?;

        Ok(())
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        std::env::temp_dir().join(format!(
            "acorusdb-{prefix}-tests-{}-{timestamp}-{sequence}",
            std::process::id()
        ))
    }
}
