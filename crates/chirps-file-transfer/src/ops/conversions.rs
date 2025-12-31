use crate::chunk::ChunkMeta;
use crate::manifest::{FileMetadata, FileType, TransferManifest};
use crate::options::{
    CompressionAlgorithm, HashAlgorithm, RetryPolicy, TransferMode, TransferOptions,
};
use alopex_chirps_wire::file_transfer as wire;

pub(crate) fn to_wire_transfer_options(options: &TransferOptions) -> wire::TransferOptions {
    wire::TransferOptions {
        chunk_size: options.chunk_size,
        concurrency: options.concurrency,
        compression: to_wire_compression(options.compression),
        bandwidth_limit: options.bandwidth_limit,
        retry_policy: to_wire_retry_policy(&options.retry_policy),
        verify_on_complete: options.verify_on_complete,
        hash_algorithm: to_wire_hash_algorithm(options.hash_algorithm),
        resumable: options.resumable,
        overwrite: options.overwrite,
        mode: to_wire_transfer_mode(options.mode),
        preserve_metadata: options.preserve_metadata,
    }
}

pub(crate) fn from_wire_transfer_options(options: &wire::TransferOptions) -> TransferOptions {
    TransferOptions {
        chunk_size: options.chunk_size,
        concurrency: options.concurrency,
        compression: from_wire_compression(options.compression),
        bandwidth_limit: options.bandwidth_limit,
        retry_policy: from_wire_retry_policy(&options.retry_policy),
        verify_on_complete: options.verify_on_complete,
        hash_algorithm: from_wire_hash_algorithm(options.hash_algorithm),
        resumable: options.resumable,
        overwrite: options.overwrite,
        mode: from_wire_transfer_mode(options.mode),
        preserve_metadata: options.preserve_metadata,
    }
}

pub(crate) fn to_wire_manifest(manifest: &TransferManifest) -> wire::TransferManifest {
    wire::TransferManifest {
        version: manifest.version,
        session_id: manifest.session_id,
        source_path: manifest.source_path.clone(),
        dest_path: manifest.dest_path.clone(),
        file_size: manifest.file_size,
        file_hash: manifest.file_hash.clone(),
        hash_algorithm: to_wire_hash_algorithm(manifest.hash_algorithm),
        chunk_size: manifest.chunk_size,
        chunk_count: manifest.chunk_count,
        chunks: manifest.chunks.iter().map(to_wire_chunk_meta).collect(),
        metadata: manifest.metadata.as_ref().map(to_wire_file_metadata),
        options: to_wire_transfer_options(&manifest.options),
        created_at: manifest.created_at,
    }
}

pub(crate) fn from_wire_manifest(manifest: wire::TransferManifest) -> TransferManifest {
    TransferManifest {
        version: manifest.version,
        session_id: manifest.session_id,
        source_path: manifest.source_path,
        dest_path: manifest.dest_path,
        file_size: manifest.file_size,
        file_hash: manifest.file_hash,
        hash_algorithm: from_wire_hash_algorithm(manifest.hash_algorithm),
        chunk_size: manifest.chunk_size,
        chunk_count: manifest.chunk_count,
        chunks: manifest
            .chunks
            .into_iter()
            .map(from_wire_chunk_meta)
            .collect(),
        metadata: manifest.metadata.map(from_wire_file_metadata),
        options: from_wire_transfer_options(&manifest.options),
        created_at: manifest.created_at,
    }
}

fn to_wire_chunk_meta(meta: &ChunkMeta) -> wire::ChunkMeta {
    wire::ChunkMeta {
        index: meta.index,
        offset: meta.offset,
        size: meta.size,
        checksum: meta.checksum,
    }
}

fn from_wire_chunk_meta(meta: wire::ChunkMeta) -> ChunkMeta {
    ChunkMeta {
        index: meta.index,
        offset: meta.offset,
        size: meta.size,
        checksum: meta.checksum,
    }
}

pub(crate) fn to_wire_file_metadata(metadata: &FileMetadata) -> wire::FileMetadata {
    wire::FileMetadata {
        created_at: metadata.created_at,
        modified_at: metadata.modified_at,
        permissions: metadata.permissions,
        file_type: to_wire_file_type(metadata.file_type),
    }
}

pub(crate) fn from_wire_file_metadata(metadata: wire::FileMetadata) -> FileMetadata {
    FileMetadata {
        created_at: metadata.created_at,
        modified_at: metadata.modified_at,
        permissions: metadata.permissions,
        file_type: from_wire_file_type(metadata.file_type),
    }
}

fn to_wire_file_type(file_type: FileType) -> wire::FileType {
    match file_type {
        FileType::File => wire::FileType::File,
        FileType::Directory => wire::FileType::Directory,
        FileType::Symlink => wire::FileType::Symlink,
    }
}

fn from_wire_file_type(file_type: wire::FileType) -> FileType {
    match file_type {
        wire::FileType::File => FileType::File,
        wire::FileType::Directory => FileType::Directory,
        wire::FileType::Symlink => FileType::Symlink,
    }
}

fn to_wire_transfer_mode(mode: TransferMode) -> wire::TransferMode {
    match mode {
        TransferMode::Copy => wire::TransferMode::Copy,
        TransferMode::Move => wire::TransferMode::Move,
    }
}

fn from_wire_transfer_mode(mode: wire::TransferMode) -> TransferMode {
    match mode {
        wire::TransferMode::Copy => TransferMode::Copy,
        wire::TransferMode::Move => TransferMode::Move,
    }
}

fn to_wire_hash_algorithm(algorithm: HashAlgorithm) -> wire::HashAlgorithm {
    match algorithm {
        HashAlgorithm::Sha256 => wire::HashAlgorithm::Sha256,
        HashAlgorithm::Blake3 => wire::HashAlgorithm::Blake3,
        HashAlgorithm::XxHash64 => wire::HashAlgorithm::XxHash64,
    }
}

fn from_wire_hash_algorithm(algorithm: wire::HashAlgorithm) -> HashAlgorithm {
    match algorithm {
        wire::HashAlgorithm::Sha256 => HashAlgorithm::Sha256,
        wire::HashAlgorithm::Blake3 => HashAlgorithm::Blake3,
        wire::HashAlgorithm::XxHash64 => HashAlgorithm::XxHash64,
    }
}

fn to_wire_compression(compression: CompressionAlgorithm) -> wire::CompressionAlgorithm {
    match compression {
        CompressionAlgorithm::None => wire::CompressionAlgorithm::None,
        CompressionAlgorithm::Zstd => wire::CompressionAlgorithm::Zstd,
        CompressionAlgorithm::ZstdLevel(level) => wire::CompressionAlgorithm::ZstdLevel(level),
    }
}

fn from_wire_compression(compression: wire::CompressionAlgorithm) -> CompressionAlgorithm {
    match compression {
        wire::CompressionAlgorithm::None => CompressionAlgorithm::None,
        wire::CompressionAlgorithm::Zstd => CompressionAlgorithm::Zstd,
        wire::CompressionAlgorithm::ZstdLevel(level) => CompressionAlgorithm::ZstdLevel(level),
    }
}

fn to_wire_retry_policy(policy: &RetryPolicy) -> wire::RetryPolicy {
    wire::RetryPolicy {
        max_retries: policy.max_retries,
        initial_delay: policy.initial_delay,
        max_delay: policy.max_delay,
        backoff_multiplier: policy.backoff_multiplier,
        jitter: policy.jitter,
    }
}

fn from_wire_retry_policy(policy: &wire::RetryPolicy) -> RetryPolicy {
    RetryPolicy {
        max_retries: policy.max_retries,
        initial_delay: policy.initial_delay,
        max_delay: policy.max_delay,
        backoff_multiplier: policy.backoff_multiplier,
        jitter: policy.jitter,
    }
}
