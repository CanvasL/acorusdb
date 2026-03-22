use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Result, Write},
    path::{Path, PathBuf},
};

use crate::wal_entry::WalEntry;

pub struct StorageEngine {
    data: HashMap<String, String>,
    wal_path: PathBuf,
    wal_file: File,
}

impl StorageEngine {
    pub fn open(path: &Path) -> Result<Self> {
        let wal_path = path.to_path_buf();

        let wal_file = OpenOptions::new().create(true).append(true).open(path)?;

        let mut storage_engine = StorageEngine {
            data: HashMap::new(),
            wal_path,
            wal_file,
        };

        storage_engine.recover()?;

        Ok(storage_engine)
    }

    pub fn recover(&mut self) -> Result<()> {
        let file = File::open(&self.wal_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if let Some(entry) = WalEntry::from_line(&line) {
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

        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // 1. Update in-memory data
        self.data.insert(key.into(), value.into());

        // 2. Append to WAL
        let entry = WalEntry::Set {
            key: key.into(),
            value: value.into(),
        };
        writeln!(self.wal_file, "{}", entry.to_line())?;
        self.wal_file.flush()?;

        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &str) -> Result<bool> {
        // 1. Update in-memory data
        let existed = self.data.remove(key).is_some();

        // 2. Append to WAL
        if existed {
            let entry = WalEntry::Delete { key: key.into() };
            writeln!(self.wal_file, "{}", entry.to_line())?;
            self.wal_file.flush()?;
        }

        Ok(existed)
    }
}
