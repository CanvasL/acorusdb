use std::{
    fs,
    path::Path,
};

use crate::error::{
    AcorusError,
    Result,
};

pub fn parent_dir_for_sync(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };

    fs::create_dir_all(parent).map_err(|source| AcorusError::CreateParentDir {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::{
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
        ensure_parent_dir,
        parent_dir_for_sync,
    };

    #[test]
    fn uses_current_directory_for_relative_file_path() {
        assert_eq!(
            parent_dir_for_sync(Path::new("acorusdb.snapshot")),
            Path::new(".")
        );
    }

    #[test]
    fn keeps_explicit_parent_directory() {
        assert_eq!(
            parent_dir_for_sync(Path::new("data/acorusdb.snapshot")),
            Path::new("data")
        );
    }

    #[test]
    fn creates_missing_parent_directories() {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let root_dir = std::env::temp_dir().join(format!(
            "acorusdb-fs-utils-tests-{}-{timestamp}-{sequence}",
            std::process::id()
        ));
        let file_path: PathBuf = root_dir.join("nested/data/acorusdb.snapshot");

        ensure_parent_dir(&file_path).expect("parent directory should be created");

        assert!(root_dir.join("nested/data").exists());

        let _ = std::fs::remove_dir_all(root_dir);
    }
}
