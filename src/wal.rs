use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Error, ErrorKind, Result, Write},
    path::{Path, PathBuf},
};

use tracing::error;

mod wal_prefix {
    pub const SET: &str = "SET";
    pub const DEL: &str = "DEL";
}

pub struct Wal {
    path: PathBuf,
    file: File,
    pub entries_len: usize,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            entries_len: 0,
        })
    }

    pub fn read_entries(&mut self) -> Result<Vec<WalEntry>> {
        let reader = BufReader::new(File::open(&self.path)?);
        let mut entries = Vec::new();

        let file_len = self.file.metadata()?.len();

        let mut line_num = 0;
        let mut last_pos = 0;

        for line in reader.lines() {
            line_num += 1;
            let line = line?;
            let current_pos = last_pos + line.len() + 1;

            if line.trim().is_empty() {
                last_pos = current_pos;
                // skip empty lines
                continue;
            }

            if let Some(entry) = WalEntry::from_line(&line) {
                entries.push(entry);
            } else {
                let is_last_line = current_pos as u64 >= file_len;

                if is_last_line {
                    // if the line is last line and is malformed, we can ignore it since it may be a result of a crash during writing.
                    // maybe it is because the process crashed before the line was fully written
                    error!(
                        line_num,
                        "ignoring malformed last line in WAL file, likely due to crash during writing"
                    );
                    break;
                } else {
                    // if the line is not last line and is malformed, we should return an error since it indicates data corruption.
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "Corrupted WAL at line {}: '{}' (not the last line)",
                            line_num, line
                        ),
                    ));
                }
            }
        }

        self.entries_len = entries.len();

        Ok(entries)
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.file.write_all(entry.to_line().as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;

        self.entries_len += 1;

        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        self.entries_len = 0;

        Ok(())
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
