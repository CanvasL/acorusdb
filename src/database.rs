use std::path::Path;

use tokio::sync::Mutex;

use crate::{
    command::Command,
    error::AcorusResult,
    storage_engine::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteResult {
    Set,
    Get(Option<String>),
    Exists(bool),
    Delete(bool),
}

pub struct Database {
    storage_engine: Mutex<StorageEngine>,
}

impl Database {
    pub fn open(
        sstable_path: &Path,
        wal_path: &Path,
        wal_compact_threshold_bytes: usize,
    ) -> AcorusResult<Self> {
        Ok(Self {
            storage_engine: Mutex::new(StorageEngine::open(
                sstable_path,
                wal_path,
                wal_compact_threshold_bytes,
            )?),
        })
    }

    pub async fn execute(&self, command: Command) -> AcorusResult<ExecuteResult> {
        let mut storage_engine = self.storage_engine.lock().await;

        match command {
            Command::Set { key, value } => {
                storage_engine.set(&key, &value)?;
                Ok(ExecuteResult::Set)
            }
            Command::Get { key } => Ok(ExecuteResult::Get(
                storage_engine.get(&key).map(str::to_owned),
            )),
            Command::Exists { key } => {
                Ok(ExecuteResult::Exists(storage_engine.get(&key).is_some()))
            }
            Command::Del { key } => Ok(ExecuteResult::Delete(storage_engine.delete(&key)?)),
        }
    }
}
