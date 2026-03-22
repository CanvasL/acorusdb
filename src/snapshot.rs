use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Error, ErrorKind, Result},
    path::{Path, PathBuf},
};

pub struct Snapshot {
    path: PathBuf,
}

impl Snapshot {
    /// Opening the snapshot file. If the file does not exist, it will be created.
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    /// Saving the current state to a snapshot file. This should be called periodically to prevent the WAL from growing indefinitely.
    pub fn save(&mut self, data: &HashMap<String, String>) -> Result<()> {
        // 1. generate temp file path
        let tmp_path = self.path.with_extension("snapshot.tmp");

        // 2. serialize data
        let bytes = rmp_serde::to_vec(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        // 3. write to temp file
        fs::write(&tmp_path, &bytes)?;

        // 4. sync temp file to disk
        let file = File::open(&tmp_path)?;
        file.sync_all()?;

        // 5. atomically rename temp file to snapshot file
        std::fs::rename(&tmp_path, &self.path)?;

        Ok(())
    }

    /// Loading the snapshot from disk. This should be called during startup to restore the state before replaying the WAL.
    pub fn load(&mut self) -> Result<HashMap<String, String>> {
        // 1. check if snapshot file exists, remove temp file if it exists
        let tmp_path = self.path.with_extension("snapshot.tmp");
        if tmp_path.exists() {
            fs::remove_file(&tmp_path)?;
        }

        // 2. check if snapshot file exists, if not return empty data
        if !self.path.exists() {
            return Ok(HashMap::new());
        }

        // 3. read snapshot file
        let bytes = fs::read(&self.path)?;
        let data: HashMap<String, String> =
            rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        Ok(data)
    }
}
