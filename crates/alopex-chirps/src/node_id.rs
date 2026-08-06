use crate::error::MeshError;
pub use alopex_chirps_wire::node_id::NodeId;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Loads a `NodeId` from the given path, or creates a new one if it doesn't exist.
///
/// The file is created with permissions 600.
///
/// # Returns
///
/// A tuple containing the stable `NodeId` and the incarnation allocated to
/// this process start. Existing legacy 16-byte files are upgraded in place.
pub fn load_or_create_node_id(path: &Path) -> Result<(NodeId, u64), MeshError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    if path.exists() {
        let mut bytes = Vec::new();
        fs::File::open(path)?.read_to_end(&mut bytes)?;
        if bytes.len() != 16 && bytes.len() != 24 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "node identity file must contain 16 or 24 bytes",
            )
            .into());
        }
        let mut id = [0u8; 16];
        id.copy_from_slice(&bytes[..16]);
        let previous = if bytes.len() == 24 {
            u64::from_be_bytes(bytes[16..24].try_into().expect("length checked"))
        } else {
            0
        };
        let incarnation = previous.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "node incarnation exhausted")
        })?;
        persist_identity(path, &id, incarnation)?;
        Ok((NodeId::from(id), incarnation))
    } else {
        let node_id = NodeId::new();
        persist_identity(path, node_id.as_bytes(), 0)?;
        Ok((node_id, 0))
    }
}

fn persist_identity(path: &Path, node_id: &[u8; 16], incarnation: u64) -> io::Result<()> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    set_secure_permissions(&file)?;
    file.write_all(node_id)?;
    file.write_all(&incarnation.to_be_bytes())?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent()
        && let Ok(directory) = fs::File::open(parent)
    {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn set_secure_permissions(_file: &fs::File) -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut perms = _file.metadata()?.permissions();
        perms.set_mode(0o600);
        _file.set_permissions(perms)?;
    }
    // On non-unix platforms, leave default permissions.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_load_or_create_node_id() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_node_id");

        // 1. Create a new NodeId
        let (node_id1, incarnation1) = load_or_create_node_id(&path).unwrap();
        assert_eq!(incarnation1, 0);

        // Check file permissions on unix-like systems
        #[cfg(unix)]
        {
            let metadata = fs::metadata(&path).unwrap();
            let perms = metadata.permissions();
            assert_eq!(perms.mode() & 0o777, 0o600);
        }

        // 2. Load the existing NodeId
        let (node_id2, incarnation2) = load_or_create_node_id(&path).unwrap();
        assert_eq!(node_id1, node_id2);
        assert_eq!(incarnation2, 1);

        let (node_id3, incarnation3) = load_or_create_node_id(&path).unwrap();
        assert_eq!(node_id1, node_id3);
        assert_eq!(incarnation3, 2);

        // 3. Ensure content matches
        let mut file = fs::File::open(&path).unwrap();
        let mut buffer = [0u8; 24];
        file.read_exact(&mut buffer).unwrap();
        assert_eq!(node_id1.as_bytes(), &buffer[..16]);
        assert_eq!(u64::from_be_bytes(buffer[16..].try_into().unwrap()), 2);
    }

    #[test]
    fn legacy_identity_is_upgraded_and_rejoined() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-node-id");
        let original = NodeId::new();
        fs::write(&path, original.as_bytes()).unwrap();

        let (loaded, incarnation) = load_or_create_node_id(&path).unwrap();
        assert_eq!(loaded, original);
        assert_eq!(incarnation, 1);
        assert_eq!(fs::metadata(path).unwrap().len(), 24);
    }

    #[test]
    fn incarnation_exhaustion_is_rejected_without_identity_change() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("exhausted-node-id");
        let original = NodeId::new();
        let mut bytes = original.as_bytes().to_vec();
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        fs::write(&path, &bytes).unwrap();

        assert!(load_or_create_node_id(&path).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn test_from_bytes() {
        let bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let node_id = NodeId::from_bytes(&bytes).unwrap();
        assert_eq!(node_id.as_bytes(), &bytes);

        let invalid_bytes = &[0, 1, 2, 3];
        assert!(NodeId::from_bytes(invalid_bytes).is_err());
    }
}
