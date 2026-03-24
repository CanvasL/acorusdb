use std::{
    collections::BTreeMap,
    path::Path,
};

use crate::{
    error::Result,
    snapshot::Snapshot,
    wal::{
        Wal,
        WalEntry,
    },
};

#[derive(PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MemValue {
    Value(String),
    Tombstone,
}

/// The `StorageEngine` is responsible for managing the in-memory data, the snapshot, and the WAL.
/// It provides methods to set, get, and delete key-value pairs, as well as to compact the data by
/// saving a new snapshot and clearing the WAL.
pub struct StorageEngine {
    data: BTreeMap<String, MemValue>,
    wal_compact_threshold_bytes: usize,
    snapshot: Snapshot,
    wal: Wal,
}

impl StorageEngine {
    /// Opening the storage engine by loading the snapshot and replaying the WAL. This should be
    /// called during startup to restore the state.
    pub fn open(
        snapshot_path: &Path,
        wal_path: &Path,
        wal_compact_threshold_bytes: usize,
    ) -> Result<Self> {
        let mut snapshot = Snapshot::open(snapshot_path)?;
        let mut wal = Wal::open(wal_path)?;

        let mut data = snapshot.load()?;

        for entry in wal.read_entries()? {
            match entry {
                WalEntry::Set { key, value } => {
                    data.insert(key, MemValue::Value(value));
                }
                WalEntry::Delete { key } => {
                    data.insert(key, MemValue::Tombstone);
                }
            }
        }

        Ok(Self {
            data,
            snapshot,
            wal,
            wal_compact_threshold_bytes,
        })
    }

    /// Sets a key-value pair in the storage engine. This will append a new entry to the WAL and
    /// apply the change to the in-memory data.
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let entry = WalEntry::Set {
            key: key.into(),
            value: value.into(),
        };
        self.wal.append(&entry)?;
        self.apply_wal(entry);
        self.maybe_compact();

        Ok(())
    }

    /// Gets the value of a key from the storage engine. Returns `None` if the key does not exist.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.data.get(key).and_then(|value| match value {
            MemValue::Value(value) => Some(value.as_str()),
            MemValue::Tombstone => None,
        })
    }

    /// Deletes a key from the storage engine. Returns `true` if the key was deleted, `false` if the
    /// key did not exist.
    pub fn delete(&mut self, key: &str) -> Result<bool> {
        if matches!(self.data.get(key), None | Some(&MemValue::Tombstone)) {
            return Ok(false);
        }

        let entry = WalEntry::Delete { key: key.into() };
        self.wal.append(&entry)?;
        self.apply_wal(entry);
        self.maybe_compact();

        Ok(true)
    }

    /// Saving the current state to a snapshot and clearing the WAL.
    fn compact(&mut self) -> Result<()> {
        self.snapshot.save(&self.data)?;
        self.wal.reset()?;
        Ok(())
    }

    /// Checks if the WAL size exceeds the compact threshold, and if so, triggers a compaction. This
    /// should be called after every write operation to ensure that the WAL does not grow
    /// indefinitely.
    fn maybe_compact(&mut self) {
        if self.wal.should_compact(self.wal_compact_threshold_bytes)
            && let Err(err) = self.compact()
        {
            tracing::error!(error = %err, "failed to compact data");
        }
    }

    /// Applies a WAL entry to the in-memory data. This is called after appending a new entry to the
    /// WAL, and also during startup when replaying the WAL.
    fn apply_wal(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Set { key, value } => {
                self.data.insert(key, MemValue::Value(value));
            }
            WalEntry::Delete { key } => {
                self.data.insert(key, MemValue::Tombstone);
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

    use super::StorageEngine;
    use crate::{
        error::{
            AcorusError,
            Result,
        },
        wal::WalEntry,
    };

    #[test]
    fn recovers_value_from_wal_after_restart() -> Result<()> {
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
    fn recovers_delete_from_wal_after_restart() -> Result<()> {
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
    fn compaction_persists_snapshot_and_clears_wal() -> Result<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("color", "blue")?;
        }

        assert!(paths.snapshot_path.exists());
        assert_eq!(fs::metadata(&paths.wal_path)?.len(), 0);

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("color"), Some("blue"));

        Ok(())
    }

    #[test]
    fn replays_wal_on_top_of_snapshot_during_recovery() -> Result<()> {
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
    fn ignores_malformed_last_wal_line_during_recovery() -> Result<()> {
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
    fn rejects_malformed_non_final_wal_line_during_recovery() -> Result<()> {
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

    fn open_engine(paths: &TestPaths, wal_compact_threshold_bytes: usize) -> Result<StorageEngine> {
        StorageEngine::open(
            paths.snapshot_path.as_path(),
            paths.wal_path.as_path(),
            wal_compact_threshold_bytes,
        )
    }

    struct TestPaths {
        root_dir: PathBuf,
        snapshot_path: PathBuf,
        wal_path: PathBuf,
    }

    impl TestPaths {
        fn new() -> Result<Self> {
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
                snapshot_path: root_dir.join("data.snapshot"),
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
