use std::{
    collections::HashMap,
    io::Result,
    path::Path,
};

use crate::{
    snapshot::Snapshot,
    wal::{
        Wal,
        WalEntry,
    },
};

use tracing::error;

const DEFAULT_WAL_COMPACT_THRESHOLD_BYTES: usize = 1024;

/// The `StorageEngine` is responsible for managing the in-memory data, the snapshot, and the WAL.
/// It provides methods to set, get, and delete key-value pairs, as well as to compact the data by
/// saving a new snapshot and clearing the WAL.
pub struct StorageEngine {
    data: HashMap<String, String>,
    wal_compact_threshold_bytes: usize,
    snapshot: Snapshot,
    wal: Wal,
}

impl StorageEngine {
    /// Opening the storage engine by loading the snapshot and replaying the WAL. This should be
    /// called during startup to restore the state.
    pub fn open(snapshot_path: &Path, wal_path: &Path) -> Result<Self> {
        let mut snapshot = Snapshot::open(snapshot_path)?;
        let mut wal = Wal::open(wal_path)?;

        let mut data = snapshot.load()?;

        for entry in wal.read_entries()? {
            match entry {
                WalEntry::Set { key, value } => {
                    data.insert(key, value);
                }
                WalEntry::Delete { key } => {
                    data.remove(&key);
                }
            }
        }

        Ok(Self {
            data,
            snapshot,
            wal,
            wal_compact_threshold_bytes: DEFAULT_WAL_COMPACT_THRESHOLD_BYTES,
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
        self.data.get(key).map(|v| v.as_str())
    }

    /// Deletes a key from the storage engine. Returns `true` if the key was deleted, `false` if the
    /// key did not exist.
    pub fn delete(&mut self, key: &str) -> Result<bool> {
        if !self.data.contains_key(key) {
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
        if self.wal.should_compact(self.wal_compact_threshold_bytes) {
            if let Err(err) = self.compact() {
                error!(error = %err, "failed to compact data");
            }
        }
    }

    /// Applies a WAL entry to the in-memory data. This is called after appending a new entry to the
    /// WAL, and also during startup when replaying the WAL.
    fn apply_wal(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Set { key, value } => {
                self.data.insert(key, value);
            }
            WalEntry::Delete { key } => {
                self.data.remove(&key);
            }
        }
    }
}
