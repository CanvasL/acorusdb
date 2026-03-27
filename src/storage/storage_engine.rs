use std::{
    collections::BTreeMap,
    fs,
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
    fs_utils::parent_dir_for_sync,
    manifest::Manifest,
    sstable::SSTable,
    wal::{
        Wal,
        WalEntry,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MemValue {
    Value(String),
    Tombstone,
}

#[derive(Debug, Clone)]
struct SSTableLayout {
    dir: PathBuf,
    legacy_file_name: String,
    numbered_prefix: String,
}

impl SSTableLayout {
    fn from_base_path(base_path: &Path) -> AcorusResult<Self> {
        let dir = parent_dir_for_sync(base_path).to_path_buf();
        fs::create_dir_all(&dir).map_err(|source| AcorusError::CreateParentDir {
            path: dir.clone(),
            source,
        })?;

        let legacy_file_name = base_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid_sstable_filename(base_path, "invalid UTF-8 sstable filename"))?
            .to_string();
        let numbered_prefix = base_path
            .file_stem()
            .or_else(|| base_path.file_name())
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| invalid_sstable_filename(base_path, "invalid UTF-8 sstable prefix"))?
            .to_string();

        Ok(Self {
            dir,
            legacy_file_name,
            numbered_prefix,
        })
    }

    fn numbered_path(&self, id: u64) -> PathBuf {
        self.dir
            .join(format!("{}-{id:06}.sst", self.numbered_prefix))
    }

    fn parse_table_id(&self, path: &Path) -> AcorusResult<Option<u64>> {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return Ok(None);
        };

        if file_name == self.legacy_file_name {
            return Ok(Some(0));
        }

        if path.extension().and_then(|ext| ext.to_str()) != Some("sst") {
            return Ok(None);
        }

        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            return Ok(None);
        };

        let Some(raw_id) = stem.strip_prefix(&format!("{}-", self.numbered_prefix)) else {
            return Ok(None);
        };

        let id = raw_id.parse::<u64>().map_err(|_| {
            invalid_sstable_filename(
                path,
                format!(
                    "expected sstable filename like {}-000001.sst",
                    self.numbered_prefix
                ),
            )
        })?;

        Ok(Some(id))
    }
}

/// Coordinates the active memtable, the on-disk SSTable set, and the WAL.
///
/// Startup recovery loads only the SSTables referenced by the manifest and then rebuilds the
/// active memtable from the WAL. Reads check the memtable first and then walk SSTables from
/// newest to oldest so newer values and tombstones mask older tables.
pub struct StorageEngine {
    sstable_layout: SSTableLayout,
    next_sstable_id: u64,
    memtable: BTreeMap<String, MemValue>,
    sstables: Vec<SSTable>,
    flush_threshold_entries: usize,
    wal: Wal,
    manifest: Manifest,
}

impl StorageEngine {
    /// Opens the engine by loading the manifest's SSTables and replaying the WAL into the active
    /// memtable.
    pub fn open(
        manifest_path: &Path,
        sstable_base_path: &Path,
        wal_path: &Path,
        flush_threshold_entries: usize,
    ) -> AcorusResult<Self> {
        let manifest = Manifest::load(manifest_path)?;

        let (sstable_layout, sstables, next_sstable_id) =
            load_sstables(sstable_base_path, &manifest)?;

        let mut wal = Wal::open(wal_path)?;
        let entries = wal.read_entries()?;

        let mut engine = Self {
            sstable_layout,
            next_sstable_id,
            memtable: BTreeMap::new(),
            sstables,
            flush_threshold_entries,
            wal,
            manifest,
        };

        for entry in entries {
            engine.apply_wal(entry);
        }

        Ok(engine)
    }

    /// Appends a `SET` record to the WAL and then applies the visible value to the active
    /// memtable.
    pub fn set(&mut self, key: &str, value: &str) -> AcorusResult<()> {
        let entry = WalEntry::Set {
            key: key.into(),
            value: value.into(),
        };
        self.wal.append(&entry)?;
        self.apply_wal(entry);
        self.maybe_flush();

        Ok(())
    }

    /// Returns the current visible value for a key.
    ///
    /// Keys that are absent or currently masked by a tombstone both read as `None`.
    pub fn get(&self, key: &str) -> AcorusResult<Option<String>> {
        match self.lookup(key)? {
            Some(MemValue::Value(value)) => Ok(Some(value)),
            Some(MemValue::Tombstone) | None => Ok(None),
        }
    }

    /// Appends a `DEL` record and marks the key as a tombstone in the active memtable.
    ///
    /// Returns `true` only when the key previously held a visible value.
    pub fn delete(&mut self, key: &str) -> AcorusResult<bool> {
        if !self.contains_visible_key(key)? {
            return Ok(false);
        }

        let entry = WalEntry::Delete { key: key.into() };
        self.wal.append(&entry)?;
        self.apply_wal(entry);
        self.maybe_flush();

        Ok(true)
    }

    /// Flushes the active memtable into a new immutable SSTable and then clears the WAL.
    fn flush(&mut self) -> AcorusResult<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let new_path = self.next_sstable_path();
        let new_table = SSTable::open(&new_path)?;

        new_table.write_from_memtable(&self.memtable)?;

        self.manifest
            .current_sstables
            .push(new_path.to_string_lossy().to_string());
        self.manifest.save_atomically()?;

        self.wal.reset()?;

        self.sstables.insert(0, new_table);
        self.next_sstable_id += 1;
        self.memtable.clear();

        Ok(())
    }

    fn maybe_flush(&mut self) {
        if self.memtable.len() >= self.flush_threshold_entries
            && let Err(err) = self.flush()
        {
            tracing::error!(error = %err, "failed to flush memtable");
        }
    }

    fn contains_visible_key(&self, key: &str) -> AcorusResult<bool> {
        Ok(matches!(self.lookup(key)?, Some(MemValue::Value(_))))
    }

    fn lookup(&self, key: &str) -> AcorusResult<Option<MemValue>> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value.clone()));
        }

        for sstable in &self.sstables {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn next_sstable_path(&self) -> PathBuf {
        self.sstable_layout.numbered_path(self.next_sstable_id)
    }

    /// Applies a decoded WAL record to the active memtable.
    ///
    /// This is shared by both startup recovery and the live write path so the two paths keep the
    /// same state transition rules.
    fn apply_wal(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Set { key, value } => {
                self.memtable.insert(key, MemValue::Value(value));
            }
            WalEntry::Delete { key } => {
                self.memtable.insert(key, MemValue::Tombstone);
            }
        }
    }
}

fn load_sstables(
    base_path: &Path,
    manifest: &Manifest,
) -> AcorusResult<(SSTableLayout, Vec<SSTable>, u64)> {
    let layout = SSTableLayout::from_base_path(base_path)?;
    let mut files = Vec::new();
    let mut max_id = 0_u64;

    for raw_path in &manifest.current_sstables {
        let path = PathBuf::from(raw_path);
        let id = layout.parse_table_id(&path)?.ok_or_else(|| {
            invalid_sstable_filename(
                &path,
                format!(
                    "manifest referenced unexpected sstable path {}",
                    path.display()
                ),
            )
        })?;

        max_id = max_id.max(id);
        files.push((id, path));
    }

    files.sort_by(|(left, _), (right, _)| right.cmp(left));

    let sstables = files
        .into_iter()
        .map(|(_, path)| SSTable::open(&path))
        .collect::<AcorusResult<Vec<_>>>()?;
    let next_sstable_id = max_id.saturating_add(1).max(1);

    Ok((layout, sstables, next_sstable_id))
}

fn invalid_sstable_filename(path: &Path, message: impl Into<String>) -> AcorusError {
    AcorusError::CorruptedSSTable {
        path: path.to_path_buf(),
        location: "filename".to_string(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        path::{
            Path,
            PathBuf,
        },
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
        MemValue,
        StorageEngine,
    };
    use crate::{
        error::{
            AcorusError,
            AcorusResult,
        },
        manifest::Manifest,
        sstable::SSTable,
        wal::WalEntry,
    };

    #[test]
    fn recovers_value_from_wal_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "acorus db")?;
            assert_eq!(engine.get("name")?, Some("acorus db".to_string()));
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, Some("acorus db".to_string()));

        Ok(())
    }

    #[test]
    fn recovers_delete_from_wal_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, None);

        Ok(())
    }

    #[test]
    fn delete_twice_returns_false_on_second_call() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let mut engine = open_engine(&paths, usize::MAX)?;

        engine.set("name", "fan")?;

        assert!(engine.delete("name")?);
        assert!(!engine.delete("name")?);

        Ok(())
    }

    #[test]
    fn set_after_tombstone_revives_key_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
            engine.set("name", "acorus")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, Some("acorus".to_string()));

        Ok(())
    }

    #[test]
    fn restart_preserves_tombstone_from_wal() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, None);
        assert!(matches!(
            engine.memtable.get("name"),
            Some(MemValue::Tombstone)
        ));

        Ok(())
    }

    #[test]
    fn flush_preserves_tombstone_after_restart() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("name", "fan")?;
            assert!(engine.delete("name")?);
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, None);
        assert!(engine.memtable.is_empty());
        assert_eq!(paths.sstable_files()?.len(), 2);

        Ok(())
    }

    #[test]
    fn flush_persists_sstable_and_clears_wal() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("color", "blue")?;
        }

        assert_eq!(paths.sstable_files()?.len(), 1);
        assert_eq!(fs::metadata(&paths.wal_path)?.len(), 0);

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("color")?, Some("blue".to_string()));

        Ok(())
    }

    #[test]
    fn restart_keeps_sorted_iteration_order() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("c", "3")?;
            engine.set("a", "1")?;
            engine.set("b", "2")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(memtable_key_order(&engine), vec!["a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn flush_then_restart_keeps_sorted_visible_key_order() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 0)?;
            engine.set("c", "3")?;
            engine.set("a", "1")?;
            engine.set("b", "2")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(visible_key_order(&engine)?, vec!["a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn replays_wal_on_top_of_sstable_during_recovery() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        {
            let mut engine = open_engine(&paths, 10)?;
            engine.set("shared", "old")?;
            engine.set("keep", "yes")?;
            engine.flush()?;
        }

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            engine.set("shared", "new")?;
            assert!(engine.delete("keep")?);
            engine.set("overlay", "present")?;
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("shared")?, Some("new".to_string()));
        assert_eq!(engine.get("keep")?, None);
        assert_eq!(engine.get("overlay")?, Some("present".to_string()));

        Ok(())
    }

    #[test]
    fn loads_all_sstables_listed_in_manifest_during_recovery() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        let old_path = paths.numbered_sstable_path(1);
        let old_table = SSTable::open(old_path.as_path())?;
        old_table.write_from_memtable(&BTreeMap::from([
            ("name".to_string(), MemValue::Value("old".to_string())),
            (
                "deleted".to_string(),
                MemValue::Value("visible".to_string()),
            ),
        ]))?;

        let new_path = paths.numbered_sstable_path(2);
        let new_table = SSTable::open(new_path.as_path())?;
        new_table.write_from_memtable(&BTreeMap::from([
            ("name".to_string(), MemValue::Value("new".to_string())),
            ("deleted".to_string(), MemValue::Tombstone),
        ]))?;

        paths.write_manifest([old_path.as_path(), new_path.as_path()])?;

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, Some("new".to_string()));
        assert_eq!(engine.get("deleted")?, None);

        Ok(())
    }

    #[test]
    fn ignores_sstables_not_listed_in_manifest() -> AcorusResult<()> {
        let paths = TestPaths::new()?;

        let visible_path = paths.numbered_sstable_path(1);
        let visible_table = SSTable::open(visible_path.as_path())?;
        visible_table.write_from_memtable(&BTreeMap::from([(
            "name".to_string(),
            MemValue::Value("visible".to_string()),
        )]))?;

        let orphan_path = paths.numbered_sstable_path(2);
        let orphan_table = SSTable::open(orphan_path.as_path())?;
        orphan_table.write_from_memtable(&BTreeMap::from([(
            "name".to_string(),
            MemValue::Value("orphan".to_string()),
        )]))?;

        paths.write_manifest([visible_path.as_path()])?;

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, Some("visible".to_string()));

        Ok(())
    }

    #[test]
    fn delete_can_target_key_only_present_in_older_sstable() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let table_path = paths.numbered_sstable_path(1);
        let table = SSTable::open(table_path.as_path())?;
        table.write_from_memtable(&BTreeMap::from([(
            "name".to_string(),
            MemValue::Value("fan".to_string()),
        )]))?;
        paths.write_manifest([table_path.as_path()])?;

        {
            let mut engine = open_engine(&paths, usize::MAX)?;
            assert!(engine.delete("name")?);
        }

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, None);

        Ok(())
    }

    #[test]
    fn ignores_malformed_last_wal_line_during_recovery() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let valid = WalEntry::Set {
            key: "name".into(),
            value: "fan".into(),
        }
        .to_line();

        fs::write(&paths.wal_path, format!("{valid}\nBROKEN"))?;

        let engine = open_engine(&paths, usize::MAX)?;
        assert_eq!(engine.get("name")?, Some("fan".to_string()));

        Ok(())
    }

    #[test]
    fn rejects_malformed_non_final_wal_line_during_recovery() -> AcorusResult<()> {
        let paths = TestPaths::new()?;
        let first = WalEntry::Set {
            key: "first".into(),
            value: "1".into(),
        }
        .to_line();
        let last = WalEntry::Set {
            key: "last".into(),
            value: "2".into(),
        }
        .to_line();

        fs::write(&paths.wal_path, format!("{first}\nBROKEN\n{last}\n"))?;

        let err = open_engine(&paths, usize::MAX)
            .err()
            .expect("expected WAL corruption to fail recovery");
        assert!(matches!(err, AcorusError::CorruptedWal { .. }));

        Ok(())
    }

    fn open_engine(
        paths: &TestPaths,
        flush_threshold_entries: usize,
    ) -> AcorusResult<StorageEngine> {
        StorageEngine::open(
            paths.manifest_path.as_path(),
            paths.sstable_base_path.as_path(),
            paths.wal_path.as_path(),
            flush_threshold_entries,
        )
    }

    fn memtable_key_order(engine: &StorageEngine) -> Vec<&str> {
        engine.memtable.keys().map(|key| key.as_str()).collect()
    }

    fn visible_key_order(engine: &StorageEngine) -> AcorusResult<Vec<String>> {
        let mut visible = BTreeMap::new();

        for sstable in engine.sstables.iter().rev() {
            for (key, value) in sstable.load_to_memtable()? {
                visible.insert(key, value);
            }
        }

        for (key, value) in &engine.memtable {
            visible.insert(key.clone(), value.clone());
        }

        Ok(visible
            .into_iter()
            .filter_map(|(key, value)| match value {
                MemValue::Value(_) => Some(key),
                MemValue::Tombstone => None,
            })
            .collect())
    }

    struct TestPaths {
        root_dir: PathBuf,
        manifest_path: PathBuf,
        sstable_base_path: PathBuf,
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
                "acorusdb-storage-engine-tests-{}-{timestamp}-{sequence}",
                std::process::id()
            ));

            fs::create_dir_all(&root_dir)?;

            Ok(Self {
                manifest_path: root_dir.join("manifest.toml"),
                sstable_base_path: root_dir.join("data.sst"),
                wal_path: root_dir.join("data.wal"),
                root_dir,
            })
        }

        fn numbered_sstable_path(&self, id: u64) -> PathBuf {
            self.root_dir.join(format!("data-{id:06}.sst"))
        }

        fn write_manifest<'a>(
            &self,
            current_sstables: impl IntoIterator<Item = &'a Path>,
        ) -> AcorusResult<()> {
            let mut manifest = Manifest::new(&self.manifest_path);
            manifest.current_sstables = current_sstables
                .into_iter()
                .map(|path| path.to_string_lossy().to_string())
                .collect();
            manifest.save_atomically()
        }

        fn sstable_files(&self) -> AcorusResult<Vec<PathBuf>> {
            let mut files = fs::read_dir(&self.root_dir)?
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| {
                    path.extension().and_then(|ext| ext.to_str()) == Some("sst")
                        && path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name == "data.sst" || name.starts_with("data-"))
                })
                .collect::<Vec<_>>();
            files.sort();
            Ok(files)
        }
    }

    impl Drop for TestPaths {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }
}
