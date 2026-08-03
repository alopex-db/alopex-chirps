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

#[cfg(unix)]
#[test]
fn path_validator_accepts_configured_base_path_alias_but_rejects_child_escape() {
    use std::os::unix::fs as unix_fs;

    let root = tempdir().expect("tempdir");
    let physical_base = root.path().join("physical-base");
    std::fs::create_dir_all(&physical_base).expect("create base");
    let base_alias = root.path().join("base-alias");
    unix_fs::symlink(&physical_base, &base_alias).expect("create base alias");

    let validator = PathValidator::new(base_alias.clone(), false);
    let accepted = validator
        .validate(&base_alias.join("nested/destination.bin"))
        .expect("configured base alias is trusted");
    assert_eq!(accepted, physical_base.join("nested/destination.bin"));

    let outside = tempdir().expect("outside tempdir");
    unix_fs::symlink(outside.path(), physical_base.join("escape"))
        .expect("create child escape symlink");
    let rejected = validator.validate(&base_alias.join("escape/destination.bin"));
    assert!(matches!(rejected, Err(FileTransferError::PathTraversal(_))));
}

#[cfg(unix)]
#[test]
fn path_validator_follow_symlink_rejects_escape() {
    use std::os::unix::fs as unix_fs;

    let dir = tempdir().expect("tempdir");
    let outside = tempdir().expect("outside tempdir");
    let link = dir.path().join("escape");
    unix_fs::symlink(outside.path(), &link).expect("symlink");

    let validator = PathValidator::new(dir.path().to_path_buf(), true);
    let result = validator.validate(Path::new("escape/new.txt"));
    assert!(matches!(result, Err(FileTransferError::PathTraversal(_))));
}
