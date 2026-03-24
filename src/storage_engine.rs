use std::{
    collections::BTreeMap,
    path::Path,
};

use crate::{
    error::AcorusResult,
    sstable::SSTable,
    wal::{
        Wal,
        WalEntry,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MemValue {
    Value(String),
    Tombstone,
}

/// The `StorageEngine` owns the in-memory memtable together with the SSTable and WAL files.
/// It is responsible for applying writes, serving reads, recovering state on startup, and
/// compacting the current memtable into an SSTable.
pub struct StorageEngine {
    mem_table: BTreeMap<String, MemValue>,
    wal_compact_threshold_bytes: usize,
    sstable: SSTable,
    wal: Wal,
}

impl StorageEngine {
    /// Opens the storage engine by loading the SSTable and replaying the WAL into the memtable.
    /// This is the startup recovery path.
    pub fn open(
        sstable_path: &Path,
        wal_path: &Path,
        wal_compact_threshold_bytes: usize,
    ) -> AcorusResult<Self> {
        let sstable = SSTable::open(sstable_path)?;
        let mut wal = Wal::open(wal_path)?;

        let mut mem_table = sstable.load_to_mem_table()?;

        for entry in wal.read_entries()? {
            match entry {
                WalEntry::Set { key, value } => {
                    mem_table.insert(key, MemValue::Value(value));
                }
                WalEntry::Delete { key } => {
                    mem_table.insert(key, MemValue::Tombstone);
                }
            }
        }

        Ok(Self {
            mem_table,
            sstable,
            wal,
            wal_compact_threshold_bytes,
        })
    }

    /// Sets a key-value pair by appending to the WAL first and then applying the value to the
    /// memtable.
    pub fn set(&mut self, key: &str, value: &str) -> AcorusResult<()> {
        let entry = WalEntry::Set {
            key: key.into(),
            value: value.into(),
        };
        self.wal.append(&entry)?;
        self.apply_wal(entry);
        self.maybe_compact();

        Ok(())
    }

    /// Gets the visible value of a key from the memtable. Returns `None` when the key is missing
    /// or currently marked as a tombstone.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.mem_table.get(key).and_then(|value| match value {
            MemValue::Value(value) => Some(value.as_str()),
            MemValue::Tombstone => None,
        })
    }

    /// Marks a key as deleted in the memtable by writing a tombstone. Returns `true` only when the
    /// key previously held a visible value.
    pub fn delete(&mut self, key: &str) -> AcorusResult<bool> {
        if matches!(self.mem_table.get(key), None | Some(&MemValue::Tombstone)) {
            return Ok(false);
        }

        let entry = WalEntry::Delete { key: key.into() };
        self.wal.append(&entry)?;
        self.apply_wal(entry);
        self.maybe_compact();

        Ok(true)
    }

    /// Persists the current memtable as an SSTable and clears the WAL.
    fn compact(&mut self) -> AcorusResult<()> {
        self.sstable.write_from_mem_table(&self.mem_table)?;
        self.wal.reset()?;
        Ok(())
    }

    /// Checks whether the WAL size exceeds the compact threshold and, if so, compacts the current
    /// memtable.
    fn maybe_compact(&mut self) {
        if self.wal.should_compact(self.wal_compact_threshold_bytes)
            && let Err(err) = self.compact()
        {
            tracing::error!(error = %err, "failed to compact data");
        }
    }

    /// Applies a WAL entry to the memtable. This is used both on the live write path and during
    /// startup recovery.
    fn apply_wal(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Set { key, value } => {
                self.mem_table.insert(key, MemValue::Value(value));
            }
            WalEntry::Delete { key } => {
                self.mem_table.insert(key, MemValue::Tombstone);
            }
        }
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

    use super::{
        MemValue,
        StorageEngine,
    };
    use crate::{
        error::{
            AcorusError,
            AcorusResult,
        },
        wal::WalEntry,
    };

    #[test]
    fn recovers_value_from_wal_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "acorus db")?;
            assert_eq!(engine.get("name"), Some("acorus db"));
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name"), Some("acorus db"));

        Ok(())
    }

    #[test]
    fn recovers_delete_from_wal_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name"), None);

        Ok(())
    }

    #[test]
    fn delete_twice_returns_false_on_second_call() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let mut engine = open_engine(&paths, usize::MAX)?;

        engine.set("name", "fan")?;

        assert!(engine.delete("name")?);
        assert!(!engine.delete("name")?);

        Ok(())
    }

    #[test]
    fn set_after_tombstone_revives_key_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
            engine.set("name", "acorus")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name"), Some("acorus"));

        Ok(())
    }

    #[test]
    fn restart_preserves_tombstone_from_wal() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name"), None);
        assert!(matches!(
            engine.mem_table.get("name"),
            Some(MemValue::Tombstone)
        ));

        Ok(())
    }

    #[test]
    fn compact_preserves_tombstone_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name"), None);
        assert!(matches!(
            engine.mem_table.get("name"),
            Some(MemValue::Tombstone)
        ));

        Ok(())
    }

    #[test]
    fn compaction_persists_sstable_and_clears_wal() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("color", "blue")?;
        }

        assert!(paths.sstable_path.exists());
        assert_eq!(fs::metadata(&paths.wal_path)?.len(), 0);

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("color"), Some("blue"));

        Ok(())
    }

    #[test]
    fn restart_keeps_sorted_iteration_order() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("c", "3")?;
            engine.set("a", "1")?;
            engine.set("b", "2")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(key_order(&engine), vec!["a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn compact_then_restart_keeps_sorted_iteration_order() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("c", "3")?;
            engine.set("a", "1")?;
            engine.set("b", "2")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(key_order(&engine), vec!["a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn replays_wal_on_top_of_sstable_during_recovery() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("shared", "old")?;
            engine.set("keep", "yes")?;
        }

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("shared", "new")?;
            assert!(engine.delete("keep")?);
            engine.set("overlay", "present")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("shared"), Some("new"));
        assert_eq!(engine.get("keep"), None);
        assert_eq!(engine.get("overlay"), Some("present"));

        Ok(())
    }

    #[test]
    fn ignores_malformed_last_wal_line_during_recovery() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let valid = WalEntry::Set {
            key: "name".into(),
            value: "fan".into(),
        }
        .to_line();

        fs::write(&paths.wal_path, format!("{valid}\nBROKEN"))?;

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name"), Some("fan"));

        Ok(())
    }

    #[test]
    fn rejects_malformed_non_final_wal_line_during_recovery() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let first = WalEntry::Set {
            key: "first".into(),
            value: "1".into(),
        }
        .to_line();
        let last = WalEntry::Set {
            key: "last".into(),
            value: "2".into(),
        }
        .to_line();

        fs::write(&paths.wal_path, format!("{first}\nBROKEN\n{last}\n"))?;

        let err = open_engine(&paths, usize::MAX)
            .err()
            .expect("expected WAL corruption to fail recovery");
        assert!(matches!(err, AcorusError::CorruptedWal { .. }));

        Ok(())
    }

    fn open_engine(
        paths: &TestPaths,
        wal_compact_threshold_bytes: usize,
    ) -> AcorusResult<StorageEngine> {
        StorageEngine::open(
            paths.sstable_path.as_path(),
            paths.wal_path.as_path(),
            wal_compact_threshold_bytes,
        )
    }

    fn key_order(engine: &StorageEngine) -> Vec<&str> {
        engine.mem_table.keys().map(|key| key.as_str()).collect()
    }

    struct TestPaths {
        root_dir: PathBuf,
        sstable_path: PathBuf,
        wal_path: PathBuf,
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
                "acorusdb-storage-engine-tests-{}-{timestamp}-{sequence}",
                std::process::id()
            ));

            fs::create_dir_all(&root_dir)?;

            Ok(Self {
                sstable_path: root_dir.join("data.sst"),
                wal_path: root_dir.join("data.wal"),
                root_dir,
            })
        }
    }

    impl Drop for TestPaths {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }
}
