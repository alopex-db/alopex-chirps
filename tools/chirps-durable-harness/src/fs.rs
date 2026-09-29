//! Private filesystem durability primitives shared by the verification oracles.

use std::{fs::OpenOptions, io, path::Path};

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        // Opening a directory needs BACKUP_SEMANTICS; flushing that handle
        // needs GENERIC_WRITE. Propagate both open and flush failures.
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        options.write(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    }
    options.open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::sync_directory;
    use std::{fs, io, path::PathBuf, time::SystemTime};

    #[test]
    fn syncs_existing_directory_and_reports_missing_directory() {
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!(
            "chirps-oracle-directory-sync-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&path).unwrap();
        let directory = Cleanup(path);
        fs::write(directory.0.join("entry"), b"oracle durability").unwrap();
        sync_directory(&directory.0).unwrap();
        assert_eq!(
            sync_directory(&directory.0.join("missing"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
    }
}
