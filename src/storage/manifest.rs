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
    path: PathBuf,
    version: u64,
    current_sstables: Vec<String>,
}

impl Manifest {
    const TMP_EXTENSION: &str = "tmp";

    pub fn new(path: &Path) -> Self {
        Self::from_file(path, ManifestFile::new())
    }

    pub fn load_or_create(path: &Path) -> AcorusResult<Self> {
        if !path.exists() {
            let manifest = Self::new(path);
            manifest.save_atomically()?;
        }

        let file = File::open(path).map_err(|source| AcorusError::ManifestRead {
            path: path.to_path_buf(),
            source,
        })?;
        let mut reader = ManifestReader::new(path, BufReader::new(file));
        let manifest_file = reader.read_manifest_file()?;

        Ok(Self::from_file(path, manifest_file))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn current_sstables(&self) -> &[String] {
        &self.current_sstables
    }

    pub fn append_table(&mut self, path: &Path) {
        self.current_sstables
            .push(path.to_string_lossy().to_string());
    }

    pub fn replace_tables<'a>(&mut self, paths: impl IntoIterator<Item = &'a Path>) {
        self.current_sstables = paths
            .into_iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect();
    }

    pub fn save_atomically(&self) -> AcorusResult<()> {
        let manifest_path = self.path.clone();
        ensure_parent_dir(&manifest_path)?;
        let tmp_path = manifest_path.with_extension(Self::TMP_EXTENSION);

        let tmp_file = File::create(&tmp_path).map_err(|source| AcorusError::ManifestWrite {
            path: tmp_path.clone(),
            source,
        })?;

        let mut writer = ManifestWriter::new(&tmp_path, BufWriter::new(tmp_file));

        writer.write_manifest_file(&self.to_file())?;
        writer.flush()?;
        drop(writer);

        let file = File::open(&tmp_path).map_err(|source| AcorusError::ManifestWrite {
            path: tmp_path.clone(),
            source,
        })?;
        file.sync_all()
            .map_err(|source| AcorusError::ManifestWrite {
                path: tmp_path.clone(),
                source,
            })?;

        std::fs::rename(&tmp_path, &self.path).map_err(|source| AcorusError::ManifestWrite {
            path: manifest_path.clone(),
            source,
        })?;

        let dir = parent_dir_for_sync(&manifest_path);
        let dir_path = dir.to_path_buf();
        let dir_file = File::open(dir).map_err(|source| AcorusError::ManifestWrite {
            path: dir_path.clone(),
            source,
        })?;
        dir_file
            .sync_all()
            .map_err(|source| AcorusError::ManifestWrite {
                path: dir_path,
                source,
            })?;

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
            .map_err(|source| AcorusError::ManifestRead {
                path: self.path.to_path_buf(),
                source,
            })?;

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
            .map_err(|source| AcorusError::ManifestWrite {
                path: self.path.to_path_buf(),
                source,
            })
    }

    fn write_all(&mut self, bytes: &[u8]) -> AcorusResult<()> {
        self.writer
            .write_all(bytes)
            .map_err(|source| AcorusError::ManifestWrite {
                path: self.path.to_path_buf(),
                source,
            })
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
    use crate::error::{
        AcorusError,
        AcorusResult,
    };

    #[test]
    fn load_creates_missing_parent_directories() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("nested/state/manifest.toml");

        let manifest = Manifest::load_or_create(&manifest_path)?;

        assert_eq!(manifest.version(), 1);
        assert!(manifest.current_sstables().is_empty());
        assert!(manifest_path.exists());

        fs::remove_dir_all(root_dir)?;

        Ok(())
    }

    #[test]
    fn load_rejects_invalid_toml() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("manifest.toml");
        fs::create_dir_all(&root_dir)?;

        fs::write(&manifest_path, "version = {")?;

        let err = Manifest::load_or_create(&manifest_path)
            .expect_err("expected invalid TOML to fail manifest load");
        assert!(matches!(err, AcorusError::ManifestLoad { .. }));

        fs::remove_dir_all(root_dir)?;

        Ok(())
    }

    #[test]
    fn load_rejects_invalid_field_types() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("manifest.toml");
        fs::create_dir_all(&root_dir)?;

        fs::write(
            &manifest_path,
            r#"
version = 1
current_sstables = "not-an-array"
"#,
        )?;

        let err = Manifest::load_or_create(&manifest_path)
            .expect_err("expected invalid field types to fail manifest load");
        assert!(matches!(err, AcorusError::ManifestLoad { .. }));

        fs::remove_dir_all(root_dir)?;

        Ok(())
    }

    #[test]
    fn save_then_load_round_trip_preserves_fields() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("manifest.toml");
        let mut manifest = Manifest::new(&manifest_path);
        let first = root_dir.join("data-000001.sst");
        let second = root_dir.join("data-000002.sst");
        manifest.replace_tables([first.as_path(), second.as_path()]);

        manifest.save_atomically()?;

        let loaded = Manifest::load_or_create(&manifest_path)?;

        assert_eq!(loaded.version(), manifest.version());
        assert_eq!(loaded.current_sstables(), manifest.current_sstables());

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
