use std::{
    fs::{
        File,
        OpenOptions,
    },
    io::{
        BufRead,
        BufReader,
        Write,
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
};

mod wal_prefix {
    pub const SET: &str = "SET";
    pub const DEL: &str = "DEL";
}

pub struct Wal {
    path: PathBuf,
    file: File,
    size_bytes: usize,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Self> {
        ensure_parent_dir(path)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|source| AcorusError::WalOpen {
                path: path.to_path_buf(),
                source,
            })?;

        Ok(Self {
            path: path.to_path_buf(),
            size_bytes: file
                .metadata()
                .map_err(|source| AcorusError::WalOpen {
                    path: path.to_path_buf(),
                    source,
                })?
                .len() as usize,
            file,
        })
    }

    pub fn read_entries(&mut self) -> Result<Vec<WalEntry>> {
        let read_file = File::open(&self.path).map_err(|source| AcorusError::WalRead {
            path: self.path.clone(),
            source,
        })?;
        let file_len = read_file
            .metadata()
            .map_err(|source| AcorusError::WalRead {
                path: self.path.clone(),
                source,
            })?
            .len() as usize;
        let reader = BufReader::new(read_file);
        let mut entries = Vec::new();

        let mut line_num = 0;
        let mut last_pos = 0;

        for line in reader.lines() {
            line_num += 1;
            let line = line.map_err(|source| AcorusError::WalRead {
                path: self.path.clone(),
                source,
            })?;
            let current_pos = last_pos + line.len() + 1;
            last_pos = current_pos;

            if line.trim().is_empty() {
                // skip empty lines
                continue;
            }

            if let Some(entry) = WalEntry::from_line(&line) {
                entries.push(entry);
            } else {
                let is_last_line = current_pos >= file_len;

                if is_last_line {
                    // if the line is last line and is malformed, we can ignore it since it may be a
                    // result of a crash during writing. maybe it is because the
                    // process crashed before the line was fully written
                    tracing::error!(
                        line_num,
                        "ignoring malformed last line in WAL file, likely due to crash during writing"
                    );
                    break;
                } else {
                    // if the line is not last line and is malformed, we should return an error
                    // since it indicates data corruption.
                    return Err(AcorusError::CorruptedWal {
                        path: self.path.clone(),
                        line: line_num,
                        message: format!("{line:?} is malformed and not the last line"),
                    });
                }
            }
        }

        self.size_bytes = file_len;

        Ok(entries)
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let line = entry.to_line();
        self.file
            .write_all(line.as_bytes())
            .map_err(|source| AcorusError::WalWrite {
                path: self.path.clone(),
                source,
            })?;
        self.file
            .write_all(b"\n")
            .map_err(|source| AcorusError::WalWrite {
                path: self.path.clone(),
                source,
            })?;
        // flush the file buffer to ensure the data is written to the OS
        self.file.flush().map_err(|source| AcorusError::WalWrite {
            path: self.path.clone(),
            source,
        })?;
        // and then call sync_all to ensure the data is flushed to disk.
        // This way we can guarantee that once append returns successfully,
        // the entry is safely stored in the WAL file, even in case of a crash.
        self.file
            .sync_all()
            .map_err(|source| AcorusError::WalWrite {
                path: self.path.clone(),
                source,
            })?;

        self.size_bytes += line.len() + 1;

        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        let mut reset_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|source| AcorusError::WalReset {
                path: self.path.clone(),
                source,
            })?;
        reset_file.flush().map_err(|source| AcorusError::WalReset {
            path: self.path.clone(),
            source,
        })?;
        reset_file
            .sync_all()
            .map_err(|source| AcorusError::WalReset {
                path: self.path.clone(),
                source,
            })?;
        let dir = parent_dir_for_sync(&self.path);
        let dir_path = dir.to_path_buf();
        File::open(dir)
            .map_err(|source| AcorusError::WalReset {
                path: dir_path.clone(),
                source,
            })?
            .sync_all()
            .map_err(|source| AcorusError::WalReset {
                path: dir_path,
                source,
            })?;

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)
            .map_err(|source| AcorusError::WalReset {
                path: self.path.clone(),
                source,
            })?;

        self.size_bytes = 0;

        Ok(())
    }

    pub fn should_compact(&self, threshold: usize) -> bool {
        self.size_bytes > threshold
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalEntry {
    Set { key: String, value: String },
    Delete { key: String },
}

impl WalEntry {
    /// Converts a WalEntry into a line that can be written to the WAL file.
    pub fn to_line(&self) -> String {
        match self {
            WalEntry::Set { key, value } => {
                format!(
                    "{}\t{}\t{}",
                    wal_prefix::SET,
                    encode_field(key),
                    encode_field(value)
                )
            }
            WalEntry::Delete { key } => format!("{}\t{}", wal_prefix::DEL, encode_field(key)),
        }
    }

    /// Parses a line from the WAL file into a WalEntry. Returns None if the line is malformed.
    pub fn from_line(line: &str) -> Option<Self> {
        let mut parts = line.trim_end().split('\t');
        match parts.next()? {
            wal_prefix::SET => {
                let key = decode_field(parts.next()?)?;
                let value = decode_field(parts.next()?)?;
                if parts.next().is_some() {
                    return None;
                }

                Some(WalEntry::Set { key, value })
            }
            wal_prefix::DEL => {
                let key = decode_field(parts.next()?)?;
                if parts.next().is_some() {
                    return None;
                }

                Some(WalEntry::Delete { key })
            }
            _ => None,
        }
    }
}

fn encode_field(field: &str) -> String {
    let mut encoded = String::with_capacity(field.len());

    for ch in field.chars() {
        match ch {
            '\\' => encoded.push_str("\\\\"),
            '\t' => encoded.push_str("\\t"),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            _ => encoded.push(ch),
        }
    }

    encoded
}

fn decode_field(field: &str) -> Option<String> {
    let mut decoded = String::with_capacity(field.len());
    let mut chars = field.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        let escaped = chars.next()?;
        match escaped {
            '\\' => decoded.push('\\'),
            't' => decoded.push('\t'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            _ => return None,
        }
    }

    Some(decoded)
}

#[cfg(test)]
mod tests {
    use super::WalEntry;

    #[test]
    fn wal_round_trip_preserves_special_characters() {
        let entry = WalEntry::Set {
            key: "tab\tkey".into(),
            value: "line1\nline2\\tail".into(),
        };

        assert_eq!(WalEntry::from_line(&entry.to_line()), Some(entry));
    }

    #[test]
    fn wal_line_does_not_embed_newline() {
        let entry = WalEntry::Delete { key: "name".into() };
        assert!(!entry.to_line().contains('\n'));
    }
}
