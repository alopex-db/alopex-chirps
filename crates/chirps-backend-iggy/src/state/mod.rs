//! Crash-consistent canonical local state for Durable subscriptions.

pub(crate) mod creation;
pub(crate) mod owner;

use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

const FRAME_MAGIC: &[u8; 8] = b"CHRPST07";
const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_LEN: usize = 16;
const FRAME_CHECKSUM_LEN: usize = 32;
const MAX_STATE_FRAME_LEN: usize = 64 * 1024;
const FRAME_DIGEST_DOMAIN: &[u8] = b"chirps-v0.7-state-frame-sha256\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateRecordKind {
    CreationUnit = 1,
    OwnerRecord = 2,
}

impl StateRecordKind {
    const fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::CreationUnit),
            2 => Some(Self::OwnerRecord),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum StateFrameError {
    #[error("local state frame exceeds the fixed bound")]
    TooLarge,
    #[error("local state frame is truncated")]
    Truncated,
    #[error("local state frame magic is invalid")]
    InvalidMagic,
    #[error("local state frame version is unsupported")]
    UnsupportedVersion,
    #[error("local state frame record kind is invalid")]
    InvalidKind,
    #[error("local state frame record kind does not match its file")]
    WrongKind,
    #[error("local state frame contains trailing bytes or an invalid length")]
    InvalidLength,
    #[error("local state frame checksum does not match")]
    ChecksumMismatch,
    #[error("local state frame body is invalid")]
    InvalidBody,
}

pub(crate) fn encode_state_frame(
    kind: StateRecordKind,
    body: &[u8],
) -> Result<Vec<u8>, StateFrameError> {
    let body_len = u32::try_from(body.len()).map_err(|_| StateFrameError::TooLarge)?;
    let total = FRAME_HEADER_LEN
        .checked_add(body.len())
        .and_then(|value| value.checked_add(FRAME_CHECKSUM_LEN))
        .ok_or(StateFrameError::TooLarge)?;
    if total > MAX_STATE_FRAME_LEN {
        return Err(StateFrameError::TooLarge);
    }

    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(FRAME_MAGIC);
    bytes.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    bytes.push(kind as u8);
    bytes.push(0);
    bytes.extend_from_slice(&body_len.to_be_bytes());
    bytes.extend_from_slice(body);
    let checksum = digest_parts(&[FRAME_DIGEST_DOMAIN, &bytes]);
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(crate) fn decode_state_frame(
    bytes: &[u8],
    expected_kind: StateRecordKind,
) -> Result<&[u8], StateFrameError> {
    if bytes.len() > MAX_STATE_FRAME_LEN {
        return Err(StateFrameError::TooLarge);
    }
    if bytes.len() < FRAME_HEADER_LEN + FRAME_CHECKSUM_LEN {
        return Err(StateFrameError::Truncated);
    }
    if &bytes[..8] != FRAME_MAGIC {
        return Err(StateFrameError::InvalidMagic);
    }
    if u16::from_be_bytes(bytes[8..10].try_into().expect("fixed frame slice")) != FRAME_VERSION {
        return Err(StateFrameError::UnsupportedVersion);
    }
    let kind = StateRecordKind::from_byte(bytes[10]).ok_or(StateFrameError::InvalidKind)?;
    if kind != expected_kind {
        return Err(StateFrameError::WrongKind);
    }
    if bytes[11] != 0 {
        return Err(StateFrameError::InvalidBody);
    }
    let body_len =
        u32::from_be_bytes(bytes[12..16].try_into().expect("fixed frame slice")) as usize;
    let checksum_start = FRAME_HEADER_LEN
        .checked_add(body_len)
        .ok_or(StateFrameError::InvalidLength)?;
    let expected_len = checksum_start
        .checked_add(FRAME_CHECKSUM_LEN)
        .ok_or(StateFrameError::InvalidLength)?;
    if bytes.len() != expected_len {
        return Err(StateFrameError::InvalidLength);
    }
    let expected_checksum = digest_parts(&[FRAME_DIGEST_DOMAIN, &bytes[..checksum_start]]);
    if bytes[checksum_start..] != expected_checksum {
        return Err(StateFrameError::ChecksumMismatch);
    }
    Ok(&bytes[FRAME_HEADER_LEN..checksum_start])
}

pub(crate) fn read_state_file(path: &Path) -> Result<Vec<u8>, StateReadError> {
    let mut file = File::open(path).map_err(StateReadError::Io)?;
    let declared = file.metadata().map_err(StateReadError::Io)?.len();
    if declared > MAX_STATE_FRAME_LEN as u64 {
        return Err(StateReadError::Frame(StateFrameError::TooLarge));
    }
    let mut bytes = Vec::with_capacity(declared as usize);
    file.read_to_end(&mut bytes).map_err(StateReadError::Io)?;
    if bytes.len() > MAX_STATE_FRAME_LEN {
        return Err(StateReadError::Frame(StateFrameError::TooLarge));
    }
    Ok(bytes)
}

#[derive(Debug, Error)]
pub(crate) enum StateReadError {
    #[error("local state file I/O failed: {0:?}")]
    Io(io::Error),
    #[error("local state frame is invalid: {0}")]
    Frame(#[from] StateFrameError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstallStage {
    Write,
    FileSync,
    Rename,
    DirectorySync,
    Response,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstallFault {
    None,
    BeforeWrite,
    AfterWrite,
    AfterFileSync,
    AfterRename,
    AfterDirectorySync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum InstallError {
    #[error("state install failed before canonical visibility at {stage:?}: {kind:?}")]
    KnownOld {
        stage: InstallStage,
        kind: io::ErrorKind,
    },
    #[error("state install reachability is unknown after {stage:?}: {kind:?}")]
    Unknown {
        stage: InstallStage,
        kind: io::ErrorKind,
    },
}

impl InstallError {
    const fn known_old(stage: InstallStage, kind: io::ErrorKind) -> Self {
        Self::KnownOld { stage, kind }
    }

    const fn unknown(stage: InstallStage, kind: io::ErrorKind) -> Self {
        Self::Unknown { stage, kind }
    }
}

pub(crate) fn durably_install(
    path: &Path,
    bytes: &[u8],
    fault: InstallFault,
) -> Result<(), InstallError> {
    let parent = path
        .parent()
        .ok_or_else(|| InstallError::known_old(InstallStage::Write, io::ErrorKind::InvalidInput))?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| InstallError::known_old(InstallStage::Write, io::ErrorKind::InvalidInput))?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let mut visible = false;

    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| InstallError::known_old(InstallStage::Write, error.kind()))?;
        set_owner_only_permissions(&file)
            .map_err(|error| InstallError::known_old(InstallStage::Write, error.kind()))?;
        if fault == InstallFault::BeforeWrite {
            return Err(InstallError::known_old(
                InstallStage::Write,
                io::ErrorKind::Interrupted,
            ));
        }
        file.write_all(bytes)
            .map_err(|error| InstallError::known_old(InstallStage::Write, error.kind()))?;
        if fault == InstallFault::AfterWrite {
            return Err(InstallError::known_old(
                InstallStage::Write,
                io::ErrorKind::Interrupted,
            ));
        }
        file.sync_all()
            .map_err(|error| InstallError::known_old(InstallStage::FileSync, error.kind()))?;
        if fault == InstallFault::AfterFileSync {
            return Err(InstallError::known_old(
                InstallStage::FileSync,
                io::ErrorKind::Interrupted,
            ));
        }
        drop(file);
        fs::rename(&temporary, path)
            .map_err(|error| InstallError::known_old(InstallStage::Rename, error.kind()))?;
        visible = true;
        if fault == InstallFault::AfterRename {
            return Err(InstallError::unknown(
                InstallStage::Rename,
                io::ErrorKind::Interrupted,
            ));
        }
        sync_directory(parent)
            .map_err(|error| InstallError::unknown(InstallStage::DirectorySync, error.kind()))?;
        if fault == InstallFault::AfterDirectorySync {
            return Err(InstallError::unknown(
                InstallStage::Response,
                io::ErrorKind::Interrupted,
            ));
        }
        Ok(())
    })();

    if !visible {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    OpenOptions::new().read(true).open(path)?.sync_all()
}

fn set_owner_only_permissions(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut permissions = file.metadata()?.permissions();
        permissions.set_mode(0o600);
        file.set_permissions(permissions)?;
    }
    Ok(())
}

pub(crate) fn digest(bytes: &[u8]) -> [u8; 32] {
    digest_parts(&[bytes])
}

pub(crate) fn digest_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(ALPHABET[(byte >> 4) as usize] as char);
        output.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        InstallError, InstallFault, InstallStage, StateRecordKind, decode_state_frame,
        durably_install, encode_state_frame,
    };
    use tempfile::tempdir;

    #[test]
    fn v07_task_4_1_state_frame_rejects_all_truncation_corruption_and_trailing_bytes() {
        let bytes = encode_state_frame(StateRecordKind::CreationUnit, b"creation").unwrap();
        assert_eq!(
            decode_state_frame(&bytes, StateRecordKind::CreationUnit).unwrap(),
            b"creation"
        );
        for length in 0..bytes.len() {
            assert!(decode_state_frame(&bytes[..length], StateRecordKind::CreationUnit).is_err());
        }
        let mut corrupt = bytes.clone();
        corrupt[20] ^= 1;
        assert!(decode_state_frame(&corrupt, StateRecordKind::CreationUnit).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_state_frame(&trailing, StateRecordKind::CreationUnit).is_err());
        assert!(decode_state_frame(&bytes, StateRecordKind::OwnerRecord).is_err());
    }

    #[test]
    fn v07_task_4_1_install_classifies_visibility_boundary_exactly() {
        for (fault, expected) in [
            (
                InstallFault::BeforeWrite,
                InstallError::KnownOld {
                    stage: InstallStage::Write,
                    kind: std::io::ErrorKind::Interrupted,
                },
            ),
            (
                InstallFault::AfterWrite,
                InstallError::KnownOld {
                    stage: InstallStage::Write,
                    kind: std::io::ErrorKind::Interrupted,
                },
            ),
            (
                InstallFault::AfterFileSync,
                InstallError::KnownOld {
                    stage: InstallStage::FileSync,
                    kind: std::io::ErrorKind::Interrupted,
                },
            ),
            (
                InstallFault::AfterRename,
                InstallError::Unknown {
                    stage: InstallStage::Rename,
                    kind: std::io::ErrorKind::Interrupted,
                },
            ),
            (
                InstallFault::AfterDirectorySync,
                InstallError::Unknown {
                    stage: InstallStage::Response,
                    kind: std::io::ErrorKind::Interrupted,
                },
            ),
        ] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("state.unit");
            assert_eq!(durably_install(&path, b"new", fault), Err(expected));
            assert_eq!(
                path.exists(),
                matches!(
                    fault,
                    InstallFault::AfterRename | InstallFault::AfterDirectorySync
                )
            );
        }
    }
}
