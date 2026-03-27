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
        AcorusResult,
    },
    fs_utils::{
        ensure_parent_dir,
        parent_dir_for_sync,
    },
};

pub struct Wal {
    path: PathBuf,
    file: File,
    size_bytes: usize,
}

struct WalReader<'a, R> {
    path: &'a Path,
    reader: R,
    file_len: usize,
    line_num: usize,
    last_pos: usize,
}

impl<'a, R: BufRead> WalReader<'a, R> {
    fn new(path: &'a Path, reader: R, file_len: usize) -> Self {
        Self {
            path,
            reader,
            file_len,
            line_num: 0,
            last_pos: 0,
        }
    }

    fn read_entries(&mut self) -> AcorusResult<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = self
                .reader
                .read_line(&mut line)
                .map_err(|source| wal_read_error(self.path, source))?;
            if bytes_read == 0 {
                break;
            }

            self.line_num += 1;
            self.last_pos += bytes_read;

            if line.trim().is_empty() {
                continue;
            }

            match WalEntry::from_line(trim_line_ending(&line)) {
                Ok(entry) => entries.push(entry),
                Err(_) if self.is_last_line() => {
                    tracing::error!(
                        line_num = self.line_num,
                        "ignoring malformed last line in WAL file, likely due to crash during writing"
                    );
                    break;
                }
                Err(error) => {
                    return Err(corrupted_wal(
                        self.path,
                        format!("line {}.{}", self.line_num, error.field),
                        error.message,
                    ));
                }
            }
        }

        Ok(entries)
    }

    fn is_last_line(&self) -> bool {
        self.last_pos >= self.file_len
    }
}

struct WalWriter<'a, 'b> {
    path: &'a Path,
    file: &'b mut File,
}

impl<'a, 'b> WalWriter<'a, 'b> {
    fn new(path: &'a Path, file: &'b mut File) -> Self {
        Self { path, file }
    }

    fn append_entry(&mut self, entry: &WalEntry) -> AcorusResult<usize> {
        let line = entry.to_line();
        self.write_all(line.as_bytes())?;
        self.write_all(b"\n")?;
        self.flush()?;
        self.sync_all()?;
        Ok(line.len() + 1)
    }

    fn write_all(&mut self, bytes: &[u8]) -> AcorusResult<()> {
        self.file
            .write_all(bytes)
            .map_err(|source| wal_write_error(self.path, source))
    }

    fn flush(&mut self) -> AcorusResult<()> {
        self.file
            .flush()
            .map_err(|source| wal_write_error(self.path, source))
    }

    fn sync_all(&mut self) -> AcorusResult<()> {
        self.file
            .sync_all()
            .map_err(|source| wal_write_error(self.path, source))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalOpcode {
    Set,
    Delete,
}

impl WalOpcode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Set => "SET",
            Self::Delete => "DEL",
        }
    }

    fn parse(raw: &str) -> Result<Self, WalDecodeError> {
        match raw {
            "SET" => Ok(Self::Set),
            "DEL" => Ok(Self::Delete),
            other => Err(WalDecodeError::new(
                "command",
                format!("unknown command {other:?}"),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalDecodeError {
    field: &'static str,
    message: String,
}

impl WalDecodeError {
    fn new(field: &'static str, message: impl Into<String>) -> Self {
        Self {
            field,
            message: message.into(),
        }
    }
}

impl Wal {
    pub fn open(path: &Path) -> AcorusResult<Self> {
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

    pub fn read_entries(&mut self) -> AcorusResult<Vec<WalEntry>> {
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
        let mut reader = WalReader::new(&self.path, BufReader::new(read_file), file_len);
        let entries = reader.read_entries()?;

        self.size_bytes = file_len;

        Ok(entries)
    }

    pub fn append(&mut self, entry: &WalEntry) -> AcorusResult<()> {
        let mut writer = WalWriter::new(&self.path, &mut self.file);
        let bytes_written = writer.append_entry(entry)?;

        self.size_bytes += bytes_written;

        Ok(())
    }

    pub fn reset(&mut self) -> AcorusResult<()> {
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
    /// Encodes a WAL record into a single escaped line.
    pub fn to_line(&self) -> String {
        match self {
            WalEntry::Set { key, value } => {
                format!(
                    "{}\t{}\t{}",
                    WalOpcode::Set.as_str(),
                    encode_field(key),
                    encode_field(value)
                )
            }
            WalEntry::Delete { key } => {
                format!("{}\t{}", WalOpcode::Delete.as_str(), encode_field(key))
            }
        }
    }

    /// Decodes one WAL line back into a record.
    fn from_line(line: &str) -> Result<Self, WalDecodeError> {
        let mut parts = line.split('\t');
        let opcode = WalOpcode::parse(
            parts
                .next()
                .ok_or_else(|| WalDecodeError::new("command", "missing command"))?,
        )?;

        match opcode {
            WalOpcode::Set => {
                let key = decode_field(
                    parts
                        .next()
                        .ok_or_else(|| WalDecodeError::new("key", "missing key field"))?,
                )
                .map_err(|message| WalDecodeError::new("key", message))?;
                let value = decode_field(
                    parts
                        .next()
                        .ok_or_else(|| WalDecodeError::new("value", "missing value field"))?,
                )
                .map_err(|message| WalDecodeError::new("value", message))?;
                if parts.next().is_some() {
                    return Err(WalDecodeError::new(
                        "trailing_fields",
                        "unexpected trailing fields",
                    ));
                }

                Ok(WalEntry::Set { key, value })
            }
            WalOpcode::Delete => {
                let key = decode_field(
                    parts
                        .next()
                        .ok_or_else(|| WalDecodeError::new("key", "missing key field"))?,
                )
                .map_err(|message| WalDecodeError::new("key", message))?;
                if parts.next().is_some() {
                    return Err(WalDecodeError::new(
                        "trailing_fields",
                        "unexpected trailing fields",
                    ));
                }

                Ok(WalEntry::Delete { key })
            }
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

fn decode_field(field: &str) -> Result<String, String> {
    let mut decoded = String::with_capacity(field.len());
    let mut chars = field.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        let escaped = chars
            .next()
            .ok_or_else(|| "trailing escape sequence".to_string())?;
        match escaped {
            '\\' => decoded.push('\\'),
            't' => decoded.push('\t'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            other => {
                return Err(format!("unknown escape sequence: \\{other}"));
            }
        }
    }

    Ok(decoded)
}

fn trim_line_ending(line: &str) -> &str {
    let line = line.strip_suffix('\n').unwrap_or(line);
    line.strip_suffix('\r').unwrap_or(line)
}

fn wal_read_error(path: &Path, source: std::io::Error) -> AcorusError {
    AcorusError::WalRead {
        path: path.to_path_buf(),
        source,
    }
}

fn wal_write_error(path: &Path, source: std::io::Error) -> AcorusError {
    AcorusError::WalWrite {
        path: path.to_path_buf(),
        source,
    }
}

fn corrupted_wal(
    path: &Path,
    location: impl Into<String>,
    message: impl Into<String>,
) -> AcorusError {
    AcorusError::CorruptedWal {
        path: path.to_path_buf(),
        location: location.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{
            AtomicU64,
            Ordering,
        },
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use super::{
        Wal,
        WalEntry,
    };
    use crate::error::{
        AcorusError,
        AcorusResult,
    };

    #[test]
    fn wal_round_trip_preserves_special_characters() {
        let entry = WalEntry::Set {
            key: "tab\tkey".into(),
            value: "line1\nline2\\tail".into(),
        };

        assert_eq!(WalEntry::from_line(&entry.to_line()), Ok(entry));
    }

    #[test]
    fn wal_line_does_not_embed_newline() {
        let entry = WalEntry::Delete { key: "name".into() };
        assert!(!entry.to_line().contains('\n'));
    }

    #[test]
    fn wal_round_trip_preserves_empty_value() {
        let entry = WalEntry::Set {
            key: "name".into(),
            value: "".into(),
        };

        assert_eq!(WalEntry::from_line(&entry.to_line()), Ok(entry));
    }

    #[test]
    fn reports_field_location_for_corrupted_non_final_line() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        fs::write(&paths.wal_path, "SET\tkey\tbad\\x\nDEL\tname\n")?;

        let err = Wal::open(paths.wal_path.as_path())?
            .read_entries()
            .expect_err("expected corrupted wal to fail");

        assert!(matches!(
            err,
            AcorusError::CorruptedWal { ref location, .. } if location == "line 1.value"
        ));

        Ok(())
    }

    #[test]
    fn reports_command_location_for_unknown_opcode() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        fs::write(&paths.wal_path, "BAD\tkey\tvalue\nDEL\tname\n")?;

        let err = Wal::open(paths.wal_path.as_path())?
            .read_entries()
            .expect_err("unknown opcode should fail");

        assert!(matches!(
            err,
            AcorusError::CorruptedWal { ref location, .. } if location == "line 1.command"
        ));

        Ok(())
    }

    #[test]
    fn reports_trailing_fields_location() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        fs::write(&paths.wal_path, "DEL\tname\textra\nSET\tkey\tvalue\n")?;

        let err = Wal::open(paths.wal_path.as_path())?
            .read_entries()
            .expect_err("trailing fields should fail");

        assert!(matches!(
            err,
            AcorusError::CorruptedWal { ref location, .. } if location == "line 1.trailing_fields"
        ));

        Ok(())
    }

    struct TestPaths {
        root_dir: PathBuf,
        wal_path: PathBuf,
    }

    impl TestPaths {
        fn new() -> AcorusResult<Self> {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let root_dir = std::env::temp_dir().join(format!(
                "acorusdb-wal-tests-{}-{timestamp}-{sequence}",
                std::process::id()
            ));

            fs::create_dir_all(&root_dir)?;

            Ok(Self {
                wal_path: root_dir.join("data.wal"),
                root_dir,
            })
        }
    }

    impl Drop for TestPaths {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }
}
