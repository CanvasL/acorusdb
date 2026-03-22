use std::{
    collections::HashMap,
    io::Result,
    path::Path,
};

use crate::wal_entry::{
    Wal,
    WalEntry,
};

pub struct StorageEngine {
    data: HashMap<String, String>,
    wal: Wal,
}

impl StorageEngine {
    pub fn open(path: &Path) -> Result<Self> {
        let wal = Wal::open(path)?;
        let mut data = HashMap::new();

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

        Ok(Self { data, wal })
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let entry = WalEntry::Set {
            key: key.into(),
            value: value.into(),
        };
        self.wal.append(&entry)?;
        self.apply(entry);

        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(|v| v.as_str())
    }

    pub fn delete(&mut self, key: &str) -> Result<bool> {
        if !self.data.contains_key(key) {
            return Ok(false);
        }

        let entry = WalEntry::Delete { key: key.into() };
        self.wal.append(&entry)?;
        self.apply(entry);

        Ok(true)
    }

    fn apply(&mut self, entry: WalEntry) {
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
