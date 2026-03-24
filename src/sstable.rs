use std::{
    collections::BTreeMap,
    fs::{
        self,
        File,
    },
    path::{
        Path,
        PathBuf,
    },
};

use crate::{
    error::{
        AcorusError,
        AcorusResult,
    },
    fs_utils::{
        ensure_parent_dir,
        parent_dir_for_sync,
    },
    storage_engine::MemValue,
};

const SSTABLE_FILE_TMP_EXTENSION: &str = "sst.tmp";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableEntry {
    pub key: String,
    pub value: MemValue,
}

pub struct SSTable {
    path: PathBuf,
}

impl SSTable {
    pub fn open(path: &Path) -> AcorusResult<Self> {
        ensure_parent_dir(path)?;

        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn write_from_mem_table(&self, mem_table: &BTreeMap<String, MemValue>) -> AcorusResult<()> {
        let entries = mem_table
            .iter()
            .map(|(key, value)| TableEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();

        let sst_path = self.path.clone();

        // 1. generate temp file path
        let tmp_path = sst_path.with_extension(SSTABLE_FILE_TMP_EXTENSION);

        // 2. serialize the memtable
        let bytes = rmp_serde::to_vec(&entries).map_err(|error| AcorusError::SSTableEncode {
            path: sst_path.clone(),
            message: error.to_string(),
        })?;

        // 3. write to temp file
        fs::write(&tmp_path, &bytes).map_err(|source| AcorusError::SSTableWrite {
            path: tmp_path.clone(),
            source,
        })?;

        // 4. sync temp file to disk
        let file = File::open(&tmp_path).map_err(|source| AcorusError::SSTableWrite {
            path: tmp_path.clone(),
            source,
        })?;
        file.sync_all()
            .map_err(|source| AcorusError::SSTableWrite {
                path: tmp_path.clone(),
                source,
            })?;

        // 5. atomically rename temp file to the target sstable file
        std::fs::rename(&tmp_path, &sst_path).map_err(|source| AcorusError::SSTableWrite {
            path: sst_path.clone(),
            source,
        })?;

        // 6. sync directory to ensure the rename is persisted
        let dir = parent_dir_for_sync(&sst_path);
        let dir_path = dir.to_path_buf();
        let dir_file = File::open(dir).map_err(|source| AcorusError::SSTableWrite {
            path: dir_path.clone(),
            source,
        })?;
        dir_file
            .sync_all()
            .map_err(|source| AcorusError::SSTableWrite {
                path: dir_path,
                source,
            })?;

        Ok(())
    }

    pub fn load_to_mem_table(&self) -> AcorusResult<BTreeMap<String, MemValue>> {
        let sst_path = self.path.clone();

        // 1. clean up any stale temp file from an interrupted previous write
        let tmp_path = sst_path.with_extension(SSTABLE_FILE_TMP_EXTENSION);
        if tmp_path.exists() {
            fs::remove_file(&tmp_path).map_err(|source| AcorusError::SSTableRead {
                path: tmp_path.clone(),
                source,
            })?;
        }

        // 2. if the sstable does not exist yet, recovery starts from an empty memtable
        if !sst_path.exists() {
            return Ok(BTreeMap::new());
        }

        // 3. read the sstable file, deserialize it, and rebuild the memtable
        let bytes = fs::read(&sst_path).map_err(|source| AcorusError::SSTableRead {
            path: sst_path.clone(),
            source,
        })?;
        let entries: Vec<TableEntry> =
            rmp_serde::from_slice(&bytes).map_err(|error| AcorusError::SSTableDecode {
                path: sst_path,
                message: error.to_string(),
            })?;

        let mem_table = entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<BTreeMap<_, _>>();

        Ok(mem_table)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
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

    use super::{
        SSTable,
        TableEntry,
    };
    use crate::{
        error::AcorusResult,
        storage_engine::MemValue,
    };

    #[test]
    fn missing_file_returns_empty_mem_table() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::open(paths.sstable_path.as_path())?;

        assert!(sstable.load_to_mem_table()?.is_empty());

        Ok(())
    }

    #[test]
    fn write_then_load_round_trip() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::open(paths.sstable_path.as_path())?;
        let mem_table = BTreeMap::from([
            ("language".to_string(), MemValue::Value("rust".to_string())),
            ("name".to_string(), MemValue::Value("acorus".to_string())),
        ]);

        sstable.write_from_mem_table(&mem_table)?;

        let loaded = SSTable::open(paths.sstable_path.as_path())?.load_to_mem_table()?;
        assert_eq!(loaded, mem_table);

        Ok(())
    }

    #[test]
    fn preserves_tombstone_during_round_trip() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::open(paths.sstable_path.as_path())?;
        let mem_table = BTreeMap::from([
            ("deleted".to_string(), MemValue::Tombstone),
            ("live".to_string(), MemValue::Value("visible".to_string())),
        ]);

        sstable.write_from_mem_table(&mem_table)?;

        let loaded = SSTable::open(paths.sstable_path.as_path())?.load_to_mem_table()?;
        assert_eq!(loaded, mem_table);

        Ok(())
    }

    #[test]
    fn writes_entries_in_sorted_key_order() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::open(paths.sstable_path.as_path())?;
        let mut mem_table = BTreeMap::new();
        mem_table.insert("c".to_string(), MemValue::Value("3".to_string()));
        mem_table.insert("a".to_string(), MemValue::Value("1".to_string()));
        mem_table.insert("b".to_string(), MemValue::Value("2".to_string()));

        sstable.write_from_mem_table(&mem_table)?;

        let bytes = fs::read(paths.sstable_path.as_path())?;
        let entries: Vec<TableEntry> =
            rmp_serde::from_slice(&bytes).expect("sstable bytes should decode");

        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );

        Ok(())
    }

    struct TestPaths {
        root_dir: PathBuf,
        sstable_path: PathBuf,
    }

    impl TestPaths {
        fn new() -> AcorusResult<Self> {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let root_dir = std::env::temp_dir().join(format!(
                "acorusdb-sstable-tests-{}-{timestamp}-{sequence}",
                std::process::id()
            ));
            let sstable_path = root_dir.join("data.sst");

            fs::create_dir_all(&root_dir)?;

            Ok(Self {
                root_dir,
                sstable_path,
            })
        }
    }

    impl Drop for TestPaths {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }
}
