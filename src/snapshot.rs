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
        Result,
    },
    fs_utils::{
        ensure_parent_dir,
        parent_dir_for_sync,
    },
    storage_engine::MemValue,
};

pub struct Snapshot {
    path: PathBuf,
}

impl Snapshot {
    /// Opens the snapshot path and ensures its parent directory exists.
    pub fn open(path: &Path) -> Result<Self> {
        ensure_parent_dir(path)?;

        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    /// Saves the current memtable to a snapshot file. Tombstones are persisted as part of the
    /// snapshot so delete semantics survive compaction and restart.
    pub fn save(&mut self, mem_table: &BTreeMap<String, MemValue>) -> Result<()> {
        let snapshot_path = self.path.clone();
        ensure_parent_dir(&snapshot_path)?;

        // 1. generate temp file path
        let tmp_path = snapshot_path.with_extension("snapshot.tmp");

        // 2. serialize the memtable
        let bytes = rmp_serde::to_vec(mem_table).map_err(|error| AcorusError::SnapshotEncode {
            path: snapshot_path.clone(),
            message: error.to_string(),
        })?;

        // 3. write to temp file
        fs::write(&tmp_path, &bytes).map_err(|source| AcorusError::SnapshotWrite {
            path: tmp_path.clone(),
            source,
        })?;

        // 4. sync temp file to disk
        let file = File::open(&tmp_path).map_err(|source| AcorusError::SnapshotWrite {
            path: tmp_path.clone(),
            source,
        })?;
        file.sync_all()
            .map_err(|source| AcorusError::SnapshotWrite {
                path: tmp_path.clone(),
                source,
            })?;

        // 5. atomically rename temp file to snapshot file
        std::fs::rename(&tmp_path, &snapshot_path).map_err(|source| {
            AcorusError::SnapshotWrite {
                path: snapshot_path.clone(),
                source,
            }
        })?;

        // 6. sync directory to ensure the rename is persisted
        let dir = parent_dir_for_sync(&snapshot_path);
        let dir_path = dir.to_path_buf();
        let dir_file = File::open(dir).map_err(|source| AcorusError::SnapshotWrite {
            path: dir_path.clone(),
            source,
        })?;
        dir_file
            .sync_all()
            .map_err(|source| AcorusError::SnapshotWrite {
                path: dir_path,
                source,
            })?;

        Ok(())
    }

    /// Loads the snapshot into the memtable before WAL replay during startup recovery.
    pub fn load(&mut self) -> Result<BTreeMap<String, MemValue>> {
        let snapshot_path = self.path.clone();

        // 1. check if snapshot file exists, remove temp file if it exists
        let tmp_path = snapshot_path.with_extension("snapshot.tmp");
        if tmp_path.exists() {
            fs::remove_file(&tmp_path).map_err(|source| AcorusError::SnapshotRead {
                path: tmp_path.clone(),
                source,
            })?;
        }

        // 2. check if snapshot file exists, if not return an empty memtable
        if !snapshot_path.exists() {
            return Ok(BTreeMap::new());
        }

        // 3. read snapshot file
        let bytes = fs::read(&snapshot_path).map_err(|source| AcorusError::SnapshotRead {
            path: snapshot_path.clone(),
            source,
        })?;
        let mem_table: BTreeMap<String, MemValue> =
            rmp_serde::from_slice(&bytes).map_err(|error| AcorusError::SnapshotDecode {
                path: snapshot_path,
                message: error.to_string(),
            })?;

        Ok(mem_table)
    }
}
