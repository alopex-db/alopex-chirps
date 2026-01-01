use alopex_chirps_file_transfer::{FileTransferError, PathValidator};
use std::path::Path;
use tempfile::tempdir;

#[test]
fn path_validator_allows_within_base() {
    let dir = tempdir().expect("tempdir");
    let validator = PathValidator::new(dir.path().to_path_buf(), false);
    let resolved = validator
        .validate(Path::new("nested/file.txt"))
        .expect("validate path");
    assert!(resolved.starts_with(dir.path()));
}

#[test]
fn path_validator_blocks_traversal() {
    let dir = tempdir().expect("tempdir");
    let validator = PathValidator::new(dir.path().to_path_buf(), false);
    let result = validator.validate(Path::new("../evil"));
    assert!(matches!(result, Err(FileTransferError::PathTraversal(_))));
}

#[test]
fn path_validator_blocks_absolute_outside_base() {
    let dir = tempdir().expect("tempdir");
    let other = tempdir().expect("tempdir");
    let validator = PathValidator::new(dir.path().to_path_buf(), false);
    let result = validator.validate(other.path());
    assert!(matches!(result, Err(FileTransferError::PathTraversal(_))));
}

#[cfg(unix)]
#[test]
fn path_validator_rejects_symlink_components() {
    use std::os::unix::fs as unix_fs;
    let dir = tempdir().expect("tempdir");
    let target = dir.path().join("target");
    std::fs::create_dir_all(&target).expect("create target");
    let link = dir.path().join("link");
    unix_fs::symlink(&target, &link).expect("symlink");

    let validator = PathValidator::new(dir.path().to_path_buf(), false);
    let result = validator.validate(Path::new("link/file.txt"));
    assert!(matches!(result, Err(FileTransferError::PathTraversal(_))));

    let follow = PathValidator::new(dir.path().to_path_buf(), true);
    let resolved = follow
        .validate(Path::new("link/file.txt"))
        .expect("validate symlink");
    assert!(resolved.starts_with(&target));
}
