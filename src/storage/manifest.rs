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
    #[serde(default, alias = "current_sstables")]
    current_table_files: Vec<String>,
}

impl ManifestFile {
    const CURRENT_VERSION: u64 = 1;

    fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            current_table_files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    path: PathBuf,
    version: u64,
    current_table_files: Vec<String>,
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

    pub fn current_table_files(&self) -> &[String] {
        &self.current_table_files
    }

    pub fn append_table(&mut self, path: &Path) {
        self.current_table_files.push(table_file_name(path));
    }

    pub fn replace_tables<'a>(&mut self, paths: impl IntoIterator<Item = &'a Path>) {
        self.current_table_files = paths.into_iter().map(table_file_name).collect();
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
            current_table_files: file
                .current_table_files
                .into_iter()
                .map(|raw| normalize_manifest_table_file(&raw))
                .collect(),
        }
    }

    fn to_file(&self) -> ManifestFile {
        ManifestFile {
            version: self.version,
            current_table_files: self.current_table_files.clone(),
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

        let manifest: ManifestFile =
            toml::from_str(&content).map_err(|source| AcorusError::ManifestLoad {
                path: self.path.to_path_buf(),
                source,
            })?;

        if manifest.version != ManifestFile::CURRENT_VERSION {
            return Err(AcorusError::ManifestVersion {
                path: self.path.to_path_buf(),
                expected: ManifestFile::CURRENT_VERSION,
                found: manifest.version,
            });
        }

        Ok(manifest)
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

fn normalize_manifest_table_file(raw: &str) -> String {
    Path::new(raw)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(raw)
        .to_string()
}

fn table_file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string_lossy().into_owned())
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
        assert!(manifest.current_table_files().is_empty());
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
current_table_files = "not-an-array"
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
        let content = fs::read_to_string(&manifest_path)?;

        assert_eq!(loaded.version(), manifest.version());
        assert_eq!(loaded.current_table_files(), manifest.current_table_files());
        assert_eq!(
            loaded.current_table_files(),
            &["data-000001.sst".to_string(), "data-000002.sst".to_string()]
        );
        assert!(content.contains("current_table_files"));
        assert!(!content.contains(&root_dir.display().to_string()));

        fs::remove_dir_all(root_dir)?;

        Ok(())
    }

    #[test]
    fn load_normalizes_legacy_current_sstables_paths() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("manifest.toml");
        let legacy_path = root_dir.join("nested/data-000001.sst");
        fs::create_dir_all(&root_dir)?;

        fs::write(
            &manifest_path,
            format!(
                "version = 1\ncurrent_sstables = [\"{}\"]\n",
                legacy_path.display()
            ),
        )?;

        let manifest = Manifest::load_or_create(&manifest_path)?;

        assert_eq!(
            manifest.current_table_files(),
            &["data-000001.sst".to_string()]
        );

        fs::remove_dir_all(root_dir)?;

        Ok(())
    }

    #[test]
    fn load_rejects_unsupported_manifest_version() -> AcorusResult<()> {
        let root_dir = unique_test_dir("manifest");
        let manifest_path = root_dir.join("manifest.toml");
        fs::create_dir_all(&root_dir)?;

        fs::write(&manifest_path, "version = 2\ncurrent_table_files = []\n")?;

        let err = Manifest::load_or_create(&manifest_path)
            .expect_err("expected unsupported manifest version to fail load");
        assert!(matches!(
            err,
            AcorusError::ManifestVersion {
                expected: 1,
                found: 2,
                ..
            }
        ));

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
