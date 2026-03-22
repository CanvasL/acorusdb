use std::{
    io::Result,
    path::Path,
};

use tokio::sync::Mutex;

use crate::{
    command::Command,
    storage_engine::StorageEngine,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteResult {
    Set,
    Get(Option<String>),
    Delete(bool),
}

pub struct Database {
    storage_engine: Mutex<StorageEngine>,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            storage_engine: Mutex::new(StorageEngine::open(path)?),
        })
    }

    pub async fn execute(&self, command: Command) -> Result<ExecuteResult> {
        let mut storage_engine = self.storage_engine.lock().await;

        match command {
            Command::Set { key, value } => {
                storage_engine.set(&key, &value)?;
                Ok(ExecuteResult::Set)
            }
            Command::Get { key } => Ok(ExecuteResult::Get(
                storage_engine.get(&key).map(str::to_owned),
            )),
            Command::Del { key } => Ok(ExecuteResult::Delete(storage_engine.delete(&key)?)),
        }
    }
}
