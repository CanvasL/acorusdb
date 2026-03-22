use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Result, Write},
    path::{Path, PathBuf},
};

mod wal_prefix {
    pub const SET: &str = "SET";
    pub const DEL: &str = "DEL";
}

pub struct Wal {
    path: PathBuf,
    file: File,
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
        })
    }

    pub fn read_entries(&self) -> Result<Vec<WalEntry>> {
        let reader = BufReader::new(File::open(&self.path)?);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                // skip empty lines
                continue;
            }

            if let Some(entry) = WalEntry::from_line(&line) {
                entries.push(entry);
            } 
        }

        Ok(entries)
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.file.write_all(entry.to_line().as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.flush()
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

        Ok(())
    }

    pub fn count_entries(&self) -> usize {
        match self.read_entries() {
            Ok(entries) => entries.len(),
            Err(_) => 0,
        }
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
