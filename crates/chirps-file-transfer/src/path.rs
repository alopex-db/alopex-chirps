use crate::error::FileTransferError;
use std::io;
use std::path::{Component, Path, PathBuf};

/// Validates paths against a base directory and traversal rules.
#[derive(Debug, Clone)]
pub struct PathValidator {
    base_path: PathBuf,
    follow_symlinks: bool,
}

impl PathValidator {
    /// Creates a validator with a base path and symlink policy.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn new(base_path: PathBuf, follow_symlinks: bool) -> Self {
        let base_path = std::fs::canonicalize(&base_path).unwrap_or(base_path);
        PathValidator {
            base_path,
            follow_symlinks,
        }
    }

    /// Validates a path and returns the resolved absolute path.
    ///
    /// Returns a [`FileTransferError::PathTraversal`] when the path escapes the base.
    ///
    /// # Errors
    /// Returns `FileTransferError::PathTraversal` when the path includes `..`, contains
    /// disallowed symlinks, or resolves outside the base directory. Returns
    /// `FileTransferError::Io` when resolving symlinks fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn validate(&self, path: &Path) -> Result<PathBuf, FileTransferError> {
        if contains_parent_dir(path) {
            return Err(FileTransferError::PathTraversal(path.display().to_string()));
        }

        let candidate = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_path.join(path)
        };

        let resolved = if self.follow_symlinks {
            self.resolve_symlink(&candidate)
                .map_err(FileTransferError::Io)?
        } else {
            self.reject_symlink_components(&candidate)?;
            candidate
        };

        if !self.is_within_base(&resolved) {
            return Err(FileTransferError::PathTraversal(
                resolved.display().to_string(),
            ));
        }

        Ok(resolved)
    }

    /// Returns true if the path is within the configured base path.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn is_within_base(&self, path: &Path) -> bool {
        path.starts_with(&self.base_path)
    }

    /// Resolves symlinks using `std::fs::canonicalize`.
    ///
    /// # Errors
    /// Returns an `io::Error` if canonicalization fails.
    ///
    /// # Panics
    /// This method does not panic.
    pub fn resolve_symlink(&self, path: &Path) -> io::Result<PathBuf> {
        std::fs::canonicalize(path)
    }

    fn reject_symlink_components(&self, path: &Path) -> Result<(), FileTransferError> {
        let mut current = PathBuf::new();
        for component in path.components() {
            current.push(component);
            if let Ok(metadata) = std::fs::symlink_metadata(&current)
                && metadata.file_type().is_symlink()
            {
                return Err(FileTransferError::PathTraversal(
                    current.display().to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn contains_parent_dir(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}
