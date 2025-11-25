use crate::error::MeshError;
pub use chirps_wire::node_id::NodeId;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// Loads a `NodeId` from the given path, or creates a new one if it doesn't exist.
///
/// The file is created with permissions 600.
///
/// # Returns
///
/// A tuple containing the `NodeId` and the initial incarnation number.
pub fn load_or_create_node_id(path: &Path) -> Result<(NodeId, u64), MeshError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    if path.exists() {
        let mut file = fs::File::open(path)?;
        let mut buffer = [0u8; 16];
        file.read_exact(&mut buffer)?;
        let node_id = NodeId::from(buffer);
        // For now, incarnation is always 0 on load. This might change.
        Ok((node_id, 0))
    } else {
        let node_id = NodeId::new();
        let mut file = fs::File::create(path)?;
        set_secure_permissions(&file)?;
        file.write_all(node_id.as_bytes())?;
        Ok((node_id, 0))
    }
}

fn set_secure_permissions(file: &fs::File) -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut perms = file.metadata()?.permissions();
        perms.set_mode(0o600);
        file.set_permissions(perms)?;
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
        assert_eq!(incarnation2, 0);

        // 3. Ensure content matches
        let mut file = fs::File::open(&path).unwrap();
        let mut buffer = [0u8; 16];
        file.read_exact(&mut buffer).unwrap();
        assert_eq!(node_id1.as_bytes(), &buffer);
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
