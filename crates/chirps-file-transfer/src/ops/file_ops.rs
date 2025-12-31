use crate::error::FileTransferError;
use crate::manifest::{FileMetadata, FileType};
use crate::options::{ListOptions, RemoveOptions, SortBy};
use crate::path::PathValidator;
use alopex_chirps_wire::file_transfer::{
    ExistsRequest, ExistsResponse, FileInfo, ListRequest, ListResponse, MetadataRequest,
    MetadataResponse, RemoveRequest, RemoveResponse,
};
use std::path::Path;
use std::time::UNIX_EPOCH;
use tokio::fs;

pub async fn exists(
    path_validator: &PathValidator,
    path: &Path,
) -> Result<bool, FileTransferError> {
    let path = path_validator.validate(path)?;
    Ok(fs::metadata(path).await.is_ok())
}

pub async fn remove(
    path_validator: &PathValidator,
    path: &Path,
    options: RemoveOptions,
) -> Result<(), FileTransferError> {
    let path = path_validator.validate(path)?;
    let metadata = match fs::metadata(&path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && options.ignore_not_found => {
            return Ok(());
        }
        Err(err) => return Err(map_io_error(err)),
    };
    if metadata.is_dir() {
        if options.recursive {
            if let Err(err) = fs::remove_dir_all(path).await
                && (err.kind() != std::io::ErrorKind::NotFound || !options.ignore_not_found)
            {
                return Err(map_io_error(err));
            }
        } else if let Err(err) = fs::remove_dir(path).await
            && (err.kind() != std::io::ErrorKind::NotFound || !options.ignore_not_found)
        {
            return Err(map_io_error(err));
        }
    } else if let Err(err) = fs::remove_file(path).await
        && (err.kind() != std::io::ErrorKind::NotFound || !options.ignore_not_found)
    {
        return Err(map_io_error(err));
    }
    Ok(())
}

pub async fn metadata(
    path_validator: &PathValidator,
    path: &Path,
) -> Result<FileMetadata, FileTransferError> {
    let (metadata, _) = metadata_with_size(path_validator, path).await?;
    Ok(metadata)
}

async fn metadata_with_size(
    path_validator: &PathValidator,
    path: &Path,
) -> Result<(FileMetadata, u64), FileTransferError> {
    let path = path_validator.validate(path)?;
    let metadata = fs::metadata(&path).await.map_err(map_io_error)?;
    Ok((file_metadata_from_fs(&metadata), metadata.len()))
}

pub async fn list_files(
    path_validator: &PathValidator,
    dir_path: &Path,
    options: ListOptions,
) -> Result<Vec<FileInfo>, FileTransferError> {
    let dir_path = path_validator.validate(dir_path)?;
    let mut entries = Vec::new();
    let mut stack = vec![dir_path];

    while let Some(dir) = stack.pop() {
        let mut read_dir = fs::read_dir(&dir).await.map_err(map_io_error)?;
        while let Some(entry) = read_dir.next_entry().await.map_err(map_io_error)? {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if !options.include_hidden && name.starts_with('.') {
                continue;
            }
            if let Some(pattern) = &options.pattern
                && !name.contains(pattern)
            {
                continue;
            }
            let metadata = entry.metadata().await.map_err(map_io_error)?;
            let is_dir = metadata.is_dir();
            if options.directories_only && !is_dir {
                continue;
            }
            if options.files_only && is_dir {
                continue;
            }
            if is_dir && options.recursive {
                stack.push(path.clone());
            }
            let info = FileInfo {
                path: path.display().to_string(),
                size: metadata.len(),
                modified_at: to_unix_seconds(metadata.modified())?,
                file_type: file_type_from_fs(&metadata),
            };
            entries.push(info);
        }
    }

    sort_files(&mut entries, options.sort_by);
    if options.limit > 0 && entries.len() > options.limit {
        entries.truncate(options.limit);
    }

    Ok(entries)
}

pub async fn handle_exists_request(
    path_validator: &PathValidator,
    request: ExistsRequest,
) -> Result<ExistsResponse, FileTransferError> {
    let path = Path::new(&request.path);
    let path = path_validator.validate(path)?;
    let metadata = fs::metadata(&path).await.ok();
    Ok(match metadata {
        Some(metadata) => ExistsResponse {
            exists: true,
            is_file: metadata.is_file(),
            is_directory: metadata.is_dir(),
        },
        None => ExistsResponse {
            exists: false,
            is_file: false,
            is_directory: false,
        },
    })
}

pub async fn handle_remove_request(
    path_validator: &PathValidator,
    request: RemoveRequest,
) -> Result<RemoveResponse, FileTransferError> {
    let path = Path::new(&request.path);
    let options = RemoveOptions {
        recursive: request.recursive,
        ignore_not_found: true,
    };
    match remove(path_validator, path, options).await {
        Ok(()) => Ok(RemoveResponse {
            success: true,
            error: None,
        }),
        Err(err) => Ok(RemoveResponse {
            success: false,
            error: Some(err.to_string()),
        }),
    }
}

pub async fn handle_metadata_request(
    path_validator: &PathValidator,
    request: MetadataRequest,
) -> Result<MetadataResponse, FileTransferError> {
    let path = Path::new(&request.path);
    match metadata_with_size(path_validator, path).await {
        Ok((meta, size)) => Ok(MetadataResponse {
            found: true,
            metadata: Some(to_wire_metadata(&meta)),
            size: Some(size),
            error: None,
        }),
        Err(FileTransferError::FileNotFound(_)) => Ok(MetadataResponse {
            found: false,
            metadata: None,
            size: None,
            error: None,
        }),
        Err(err) => Ok(MetadataResponse {
            found: false,
            metadata: None,
            size: None,
            error: Some(err.to_string()),
        }),
    }
}

pub async fn handle_list_request(
    path_validator: &PathValidator,
    request: ListRequest,
) -> Result<ListResponse, FileTransferError> {
    let options = ListOptions {
        recursive: request.recursive,
        include_hidden: request.include_hidden,
        files_only: false,
        directories_only: false,
        pattern: None,
        limit: 0,
        sort_by: SortBy::Name,
    };
    match list_files(path_validator, Path::new(&request.path), options).await {
        Ok(files) => Ok(ListResponse { files, error: None }),
        Err(err) => Ok(ListResponse {
            files: Vec::new(),
            error: Some(err.to_string()),
        }),
    }
}

fn file_metadata_from_fs(metadata: &std::fs::Metadata) -> FileMetadata {
    FileMetadata {
        created_at: to_unix_seconds(metadata.created()).ok(),
        modified_at: to_unix_seconds(metadata.modified()).ok(),
        permissions: permissions_to_u32(metadata),
        file_type: file_type_from_fs(metadata).into(),
    }
}

fn file_type_from_fs(metadata: &std::fs::Metadata) -> alopex_chirps_wire::file_transfer::FileType {
    if metadata.is_dir() {
        alopex_chirps_wire::file_transfer::FileType::Directory
    } else if metadata.is_file() {
        alopex_chirps_wire::file_transfer::FileType::File
    } else {
        alopex_chirps_wire::file_transfer::FileType::Symlink
    }
}

fn to_wire_metadata(metadata: &FileMetadata) -> alopex_chirps_wire::file_transfer::FileMetadata {
    alopex_chirps_wire::file_transfer::FileMetadata {
        created_at: metadata.created_at,
        modified_at: metadata.modified_at,
        permissions: metadata.permissions,
        file_type: match metadata.file_type {
            FileType::File => alopex_chirps_wire::file_transfer::FileType::File,
            FileType::Directory => alopex_chirps_wire::file_transfer::FileType::Directory,
            FileType::Symlink => alopex_chirps_wire::file_transfer::FileType::Symlink,
        },
    }
}

fn sort_files(files: &mut [FileInfo], sort_by: SortBy) {
    match sort_by {
        SortBy::Name => files.sort_by(|a, b| a.path.cmp(&b.path)),
        SortBy::Size => files.sort_by(|a, b| a.size.cmp(&b.size)),
        SortBy::ModifiedTime => files.sort_by(|a, b| a.modified_at.cmp(&b.modified_at)),
    }
}

fn to_unix_seconds(
    time: Result<std::time::SystemTime, std::io::Error>,
) -> Result<u64, FileTransferError> {
    time.ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .ok_or_else(|| FileTransferError::Internal("timestamp conversion failed".into()))
}

#[cfg(unix)]
fn permissions_to_u32(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode())
}

#[cfg(not(unix))]
fn permissions_to_u32(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

fn map_io_error(err: std::io::Error) -> FileTransferError {
    match err.kind() {
        std::io::ErrorKind::NotFound => FileTransferError::FileNotFound(err.to_string()),
        std::io::ErrorKind::PermissionDenied => {
            FileTransferError::PermissionDenied(err.to_string())
        }
        _ => FileTransferError::Io(err),
    }
}

impl From<alopex_chirps_wire::file_transfer::FileType> for FileType {
    fn from(file_type: alopex_chirps_wire::file_transfer::FileType) -> Self {
        match file_type {
            alopex_chirps_wire::file_transfer::FileType::File => FileType::File,
            alopex_chirps_wire::file_transfer::FileType::Directory => FileType::Directory,
            alopex_chirps_wire::file_transfer::FileType::Symlink => FileType::Symlink,
        }
    }
}
