use std::{
    collections::BTreeMap,
    fs::{
        self,
        File,
    },
    io::{
        self,
        BufReader,
        BufWriter,
        Read,
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
    storage::MemValue,
};

mod format {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum ValueTag {
        Value,
        Tombstone,
    }

    impl ValueTag {
        pub(super) const fn to_byte(self) -> u8 {
            match self {
                Self::Value => 0,
                Self::Tombstone => 1,
            }
        }

        pub(super) const fn from_byte(byte: u8) -> Option<Self> {
            match byte {
                0 => Some(Self::Value),
                1 => Some(Self::Tombstone),
                _ => None,
            }
        }
    }

    pub(super) const MAGIC: [u8; 4] = *b"ACSS";
    pub(super) const VERSION: u8 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTable {
    path: PathBuf,
}

struct SSTableWriter<'a, W> {
    path: &'a Path,
    writer: W,
}

impl<'a, W: Write> SSTableWriter<'a, W> {
    fn new(path: &'a Path, writer: W) -> Self {
        Self { path, writer }
    }

    fn write_header(&mut self, entry_count: u64) -> AcorusResult<()> {
        self.write_all(&format::MAGIC)?;
        self.write_u8(format::VERSION)?;
        self.write_u64(entry_count)?;
        Ok(())
    }

    fn write_entry(&mut self, key: &str, value: &MemValue) -> AcorusResult<()> {
        self.write_bytes(key.as_bytes(), "key")?;

        match value {
            MemValue::Value(value) => {
                self.write_u8(format::ValueTag::Value.to_byte())?;
                self.write_bytes(value.as_bytes(), "value")?;
            }
            MemValue::Tombstone => self.write_u8(format::ValueTag::Tombstone.to_byte())?,
        }

        Ok(())
    }

    fn flush(&mut self) -> AcorusResult<()> {
        self.writer
            .flush()
            .map_err(|source| AcorusError::SSTableWrite {
                path: self.path.to_path_buf(),
                source,
            })
    }

    fn write_all(&mut self, bytes: &[u8]) -> AcorusResult<()> {
        self.writer
            .write_all(bytes)
            .map_err(|source| AcorusError::SSTableWrite {
                path: self.path.to_path_buf(),
                source,
            })
    }

    fn write_u8(&mut self, value: u8) -> AcorusResult<()> {
        self.write_all(&[value])
    }

    fn write_u32(&mut self, value: u32) -> AcorusResult<()> {
        self.write_all(&value.to_be_bytes())
    }

    fn write_u64(&mut self, value: u64) -> AcorusResult<()> {
        self.write_all(&value.to_be_bytes())
    }

    fn write_bytes(&mut self, bytes: &[u8], field_name: &'static str) -> AcorusResult<()> {
        let len = u32::try_from(bytes.len()).map_err(|_| AcorusError::SSTableEncode {
            path: self.path.to_path_buf(),
            message: format!("{field_name} is too large to encode into SSTable V1"),
        })?;
        self.write_u32(len)?;
        self.write_all(bytes)
    }
}

struct SSTableReader<'a, R> {
    path: &'a Path,
    reader: R,
}

impl<'a, R: Read> SSTableReader<'a, R> {
    fn new(path: &'a Path, reader: R) -> Self {
        Self { path, reader }
    }

    fn read_header(&mut self) -> AcorusResult<u64> {
        let magic = self.read_exact_array::<4>("header.magic")?;
        if magic != format::MAGIC {
            return Err(AcorusError::CorruptedSSTable {
                path: self.path.to_path_buf(),
                location: "header.magic".to_string(),
                message: format!(
                    "invalid magic number: expected {:?}, got {:?}",
                    format::MAGIC,
                    magic
                ),
            });
        }

        let version = self.read_u8("header.version")?;
        if version != format::VERSION {
            return Err(AcorusError::CorruptedSSTable {
                path: self.path.to_path_buf(),
                location: "header.version".to_string(),
                message: format!("unsupported sstable version: {version}"),
            });
        }

        self.read_u64("header.entry_count")
    }

    fn read_entry(&mut self, entry_index: u64) -> AcorusResult<(String, MemValue)> {
        let key_len = self.read_u32(&entry_location(entry_index, "key_length"))?;
        let key_bytes =
            self.read_exact_vec(&entry_location(entry_index, "key_bytes"), key_len as usize)?;
        let key = String::from_utf8(key_bytes).map_err(|error| AcorusError::CorruptedSSTable {
            path: self.path.to_path_buf(),
            location: entry_location(entry_index, "key_bytes"),
            message: format!("invalid UTF-8 sequence in key: {error}"),
        })?;

        let value_tag = self
            .read_u8(&entry_location(entry_index, "value_tag"))
            .and_then(|byte| {
                format::ValueTag::from_byte(byte).ok_or_else(|| AcorusError::CorruptedSSTable {
                    path: self.path.to_path_buf(),
                    location: entry_location(entry_index, "value_tag"),
                    message: format!("unknown value tag: {byte}"),
                })
            })?;

        let value = match value_tag {
            format::ValueTag::Value => {
                let value_len = self.read_u32(&entry_location(entry_index, "value_length"))?;
                let value_bytes = self.read_exact_vec(
                    &entry_location(entry_index, "value_bytes"),
                    value_len as usize,
                )?;
                let value = String::from_utf8(value_bytes).map_err(|error| {
                    AcorusError::CorruptedSSTable {
                        path: self.path.to_path_buf(),
                        location: entry_location(entry_index, "value_bytes"),
                        message: format!("invalid UTF-8 sequence in value: {error}"),
                    }
                })?;
                MemValue::Value(value)
            }
            format::ValueTag::Tombstone => MemValue::Tombstone,
        };

        Ok((key, value))
    }

    fn ensure_eof(&mut self) -> AcorusResult<()> {
        let mut trailing = [0u8; 1];
        match self.reader.read(&mut trailing) {
            Ok(0) => Ok(()),
            Ok(_) => Err(AcorusError::CorruptedSSTable {
                path: self.path.to_path_buf(),
                location: "trailer".to_string(),
                message: "unexpected trailing bytes after final entry".to_string(),
            }),
            Err(source) => Err(AcorusError::SSTableRead {
                path: self.path.to_path_buf(),
                source,
            }),
        }
    }

    fn read_u8(&mut self, context: &str) -> AcorusResult<u8> {
        Ok(self.read_exact_array::<1>(context)?[0])
    }

    fn read_u32(&mut self, context: &str) -> AcorusResult<u32> {
        Ok(u32::from_be_bytes(self.read_exact_array::<4>(context)?))
    }

    fn read_u64(&mut self, context: &str) -> AcorusResult<u64> {
        Ok(u64::from_be_bytes(self.read_exact_array::<8>(context)?))
    }

    fn read_exact_array<const N: usize>(&mut self, context: &str) -> AcorusResult<[u8; N]> {
        let mut buf = [0u8; N];
        self.reader
            .read_exact(&mut buf)
            .map_err(|source| map_sstable_read_step_error(self.path, context, source))?;
        Ok(buf)
    }

    fn read_exact_vec(&mut self, context: &str, len: usize) -> AcorusResult<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.reader
            .read_exact(&mut buf)
            .map_err(|source| map_sstable_read_step_error(self.path, context, source))?;
        Ok(buf)
    }
}

impl SSTable {
    const TMP_EXTENSION: &str = "sst.tmp";

    pub fn at_path(path: &Path) -> AcorusResult<Self> {
        ensure_parent_dir(path)?;

        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load_value(&self, key: &str) -> AcorusResult<Option<MemValue>> {
        let memtable = self.load_to_memtable()?;
        Ok(memtable.get(key).cloned())
    }

    pub fn size_bytes(&self) -> AcorusResult<u64> {
        fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .map_err(|source| AcorusError::SSTableRead {
                path: self.path.clone(),
                source,
            })
    }

    pub fn write_from_memtable(&self, memtable: &BTreeMap<String, MemValue>) -> AcorusResult<()> {
        let sst_path = self.path.clone();

        // Write into a temp file first so the final rename is atomic.
        let tmp_path = sst_path.with_extension(Self::TMP_EXTENSION);

        let entry_count =
            u64::try_from(memtable.len()).map_err(|_| AcorusError::SSTableEncode {
                path: tmp_path.clone(),
                message: "too many entries to encode into a single sstable".to_string(),
            })?;

        let tmp_file = File::create(&tmp_path).map_err(|source| AcorusError::SSTableWrite {
            path: tmp_path.clone(),
            source,
        })?;
        let mut writer = SSTableWriter::new(&tmp_path, BufWriter::new(tmp_file));

        writer.write_header(entry_count)?;

        for (key, value) in memtable {
            writer.write_entry(key, value)?;
        }

        writer.flush()?;
        drop(writer);

        // Make the temp file contents durable before publishing it.
        let file = File::open(&tmp_path).map_err(|source| AcorusError::SSTableWrite {
            path: tmp_path.clone(),
            source,
        })?;
        file.sync_all()
            .map_err(|source| AcorusError::SSTableWrite {
                path: tmp_path.clone(),
                source,
            })?;

        // Publish the new table atomically.
        std::fs::rename(&tmp_path, &sst_path).map_err(|source| AcorusError::SSTableWrite {
            path: sst_path.clone(),
            source,
        })?;

        // Sync the parent directory so the rename itself is durable.
        let dir = parent_dir_for_sync(&sst_path);
        let dir_path = dir.to_path_buf();
        let dir_file = File::open(dir).map_err(|source| AcorusError::SSTableWrite {
            path: dir_path.clone(),
            source,
        })?;
        dir_file
            .sync_all()
            .map_err(|source| AcorusError::SSTableWrite {
                path: dir_path,
                source,
            })?;

        Ok(())
    }

    pub fn load_to_memtable(&self) -> AcorusResult<BTreeMap<String, MemValue>> {
        let sst_path = self.path.clone();

        // Ignore stale temp output from an interrupted previous write.
        let tmp_path = sst_path.with_extension(Self::TMP_EXTENSION);
        if tmp_path.exists() {
            fs::remove_file(&tmp_path).map_err(|source| AcorusError::SSTableRead {
                path: tmp_path.clone(),
                source,
            })?;
        }

        // This specific table file may not exist yet.
        if !sst_path.exists() {
            return Ok(BTreeMap::new());
        }

        // Decode the SSTable file into an ordered in-memory map.
        let reader = File::open(&sst_path).map_err(|source| AcorusError::SSTableRead {
            path: sst_path.clone(),
            source,
        })?;
        let mut reader = SSTableReader::new(&sst_path, BufReader::new(reader));

        let entry_count = reader.read_header()?;

        let mut memtable = BTreeMap::new();
        let mut last_key: Option<String> = None;
        for entry_index in 0..entry_count {
            let (key, value) = reader.read_entry(entry_index)?;

            if let Some(previous_key) = &last_key
                && key <= *previous_key
            {
                return Err(AcorusError::CorruptedSSTable {
                    path: sst_path.clone(),
                    location: entry_location(entry_index, "key"),
                    message: format!(
                        "expected strictly increasing keys, got {key:?} after {previous_key:?}"
                    ),
                });
            }

            last_key = Some(key.clone());
            memtable.insert(key, value);
        }

        reader.ensure_eof()?;

        Ok(memtable)
    }
}

fn entry_location(entry_index: u64, field: &'static str) -> String {
    format!("entry {entry_index}.{field}")
}

fn map_sstable_read_step_error(path: &Path, context: &str, source: io::Error) -> AcorusError {
    if source.kind() == io::ErrorKind::UnexpectedEof {
        return AcorusError::CorruptedSSTable {
            path: path.to_path_buf(),
            location: context.to_string(),
            message: format!("truncated sstable while reading {context}"),
        };
    }

    AcorusError::SSTableRead {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs::{
            self,
            File,
            OpenOptions,
        },
        io::{
            BufReader,
            BufWriter,
            Write,
        },
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
        SSTable,
        SSTableReader,
        SSTableWriter,
        format::{
            self,
            ValueTag,
        },
    };
    use crate::{
        error::{
            AcorusError,
            AcorusResult,
        },
        storage::MemValue,
    };

    #[test]
    fn missing_file_returns_empty_memtable() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::at_path(paths.sstable_path.as_path())?;

        assert!(sstable.load_to_memtable()?.is_empty());

        Ok(())
    }

    #[test]
    fn write_then_load_round_trip() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::at_path(paths.sstable_path.as_path())?;
        let memtable = BTreeMap::from([
            ("language".to_string(), MemValue::Value("rust".to_string())),
            ("name".to_string(), MemValue::Value("acorus".to_string())),
        ]);

        sstable.write_from_memtable(&memtable)?;

        let loaded = SSTable::at_path(paths.sstable_path.as_path())?.load_to_memtable()?;
        assert_eq!(loaded, memtable);

        Ok(())
    }

    #[test]
    fn preserves_tombstone_during_round_trip() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::at_path(paths.sstable_path.as_path())?;
        let memtable = BTreeMap::from([
            ("deleted".to_string(), MemValue::Tombstone),
            ("live".to_string(), MemValue::Value("visible".to_string())),
        ]);

        sstable.write_from_memtable(&memtable)?;

        let loaded = SSTable::at_path(paths.sstable_path.as_path())?.load_to_memtable()?;
        assert_eq!(loaded, memtable);

        Ok(())
    }

    #[test]
    fn writes_entries_in_sorted_key_order() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::at_path(paths.sstable_path.as_path())?;
        let mut memtable = BTreeMap::new();
        memtable.insert("c".to_string(), MemValue::Value("3".to_string()));
        memtable.insert("a".to_string(), MemValue::Value("1".to_string()));
        memtable.insert("b".to_string(), MemValue::Value("2".to_string()));

        sstable.write_from_memtable(&memtable)?;

        let file = File::open(paths.sstable_path.as_path())?;
        let mut reader = SSTableReader::new(paths.sstable_path.as_path(), BufReader::new(file));
        let entry_count = reader.read_header()?;
        let mut keys = Vec::new();
        for entry_index in 0..entry_count {
            let (key, _) = reader.read_entry(entry_index)?;
            keys.push(key);
        }
        reader.ensure_eof()?;

        assert_eq!(
            keys,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );

        Ok(())
    }

    #[test]
    fn rejects_invalid_magic() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        fs::write(paths.sstable_path.as_path(), b"BADC\x01\0\0\0\0\0\0\0\0")?;

        let err = SSTable::at_path(paths.sstable_path.as_path())?
            .load_to_memtable()
            .expect_err("invalid magic should fail");
        assert!(matches!(
            err,
            AcorusError::CorruptedSSTable { ref location, .. } if location == "header.magic"
        ));

        Ok(())
    }

    #[test]
    fn rejects_unsupported_version() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&format::MAGIC);
        bytes.push(format::VERSION + 1);
        bytes.extend_from_slice(&0u64.to_be_bytes());
        fs::write(paths.sstable_path.as_path(), bytes)?;

        let err = SSTable::at_path(paths.sstable_path.as_path())?
            .load_to_memtable()
            .expect_err("unsupported version should fail");
        assert!(matches!(
            err,
            AcorusError::CorruptedSSTable { ref location, .. } if location == "header.version"
        ));

        Ok(())
    }

    #[test]
    fn rejects_out_of_order_keys() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let file = File::create(paths.sstable_path.as_path())?;
        let mut writer = SSTableWriter::new(paths.sstable_path.as_path(), BufWriter::new(file));

        writer.write_header(2)?;
        writer.write_entry("c", &MemValue::Value("3".to_string()))?;
        writer.write_entry("a", &MemValue::Value("1".to_string()))?;
        writer.flush()?;

        let err = SSTable::at_path(paths.sstable_path.as_path())?
            .load_to_memtable()
            .expect_err("out of order keys should fail");
        assert!(matches!(
            err,
            AcorusError::CorruptedSSTable { ref location, .. } if location == "entry 1.key"
        ));

        Ok(())
    }

    #[test]
    fn rejects_trailing_bytes_after_entries() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let sstable = SSTable::at_path(paths.sstable_path.as_path())?;
        let memtable =
            BTreeMap::from([("name".to_string(), MemValue::Value("acorus".to_string()))]);

        sstable.write_from_memtable(&memtable)?;

        let mut file = OpenOptions::new()
            .append(true)
            .open(paths.sstable_path.as_path())?;
        file.write_all(b"\xff")?;
        file.flush()?;

        let err = SSTable::at_path(paths.sstable_path.as_path())?
            .load_to_memtable()
            .expect_err("trailing bytes should fail");
        assert!(matches!(
            err,
            AcorusError::CorruptedSSTable { ref location, .. } if location == "trailer"
        ));

        Ok(())
    }

    #[test]
    fn rejects_unknown_value_tag() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let file = File::create(paths.sstable_path.as_path())?;
        let mut writer = SSTableWriter::new(paths.sstable_path.as_path(), BufWriter::new(file));

        writer.write_header(1)?;
        writer.write_bytes(b"name", "key")?;
        writer.write_u8(7)?;
        writer.flush()?;

        let err = SSTable::at_path(paths.sstable_path.as_path())?
            .load_to_memtable()
            .expect_err("unknown value tag should fail");
        assert!(matches!(
            err,
            AcorusError::CorruptedSSTable { ref location, .. } if location == "entry 0.value_tag"
        ));

        Ok(())
    }

    #[test]
    fn rejects_truncated_value_bytes() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let file = File::create(paths.sstable_path.as_path())?;
        let mut writer = SSTableWriter::new(paths.sstable_path.as_path(), BufWriter::new(file));

        writer.write_header(1)?;
        write_entry_prefix_with_tag(&mut writer, "name", ValueTag::Value)?;
        writer.write_u32(5)?;
        writer.write_all(b"ac")?;
        writer.flush()?;

        let err = SSTable::at_path(paths.sstable_path.as_path())?
            .load_to_memtable()
            .expect_err("truncated value bytes should fail");
        assert!(matches!(
            err,
            AcorusError::CorruptedSSTable { ref location, .. } if location == "entry 0.value_bytes"
        ));

        Ok(())
    }

    fn write_entry_prefix_with_tag<W: Write>(
        writer: &mut SSTableWriter<'_, W>,
        key: &str,
        tag: ValueTag,
    ) -> AcorusResult<()> {
        writer.write_bytes(key.as_bytes(), "key")?;
        writer.write_u8(tag.to_byte())?;
        Ok(())
    }

    struct TestPaths {
        root_dir: PathBuf,
        sstable_path: PathBuf,
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
                "acorusdb-sstable-tests-{}-{timestamp}-{sequence}",
                std::process::id()
            ));
            let sstable_path = root_dir.join("data.sst");

            fs::create_dir_all(&root_dir)?;

            Ok(Self {
                root_dir,
                sstable_path,
            })
        }
    }

    impl Drop for TestPaths {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }
}
