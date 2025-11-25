use alopex_chirps::node_id::load_or_create_node_id;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn persists_and_reloads_same_node_id() {
    let dir = tempdir().unwrap();
    let path: PathBuf = dir.path().join("nested").join("node_id.bin");

    let (first, inc1) = load_or_create_node_id(&path).expect("create node id");
    assert_eq!(inc1, 0);

    // confirm file exists and permissions are secure on unix
    #[cfg(unix)]
    {
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    let (second, inc2) = load_or_create_node_id(&path).expect("reload node id");
    assert_eq!(inc2, 0);
    assert_eq!(first, second);
}
