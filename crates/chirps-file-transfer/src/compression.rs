use crate::error::FileTransferError;
use crate::options::CompressionAlgorithm;
use std::io::{Read, Write};

const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Compresses a byte slice using the requested algorithm.
///
/// # Errors
/// Returns `FileTransferError::Compression` when the compression backend fails.
///
/// # Panics
/// This function does not panic.
pub fn compress_bytes(
    data: &[u8],
    algorithm: CompressionAlgorithm,
) -> Result<Vec<u8>, FileTransferError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd => compress_with_level(data, DEFAULT_ZSTD_LEVEL),
        CompressionAlgorithm::ZstdLevel(level) => compress_with_level(data, level),
    }
}

/// Compresses an owned payload while reusing its allocation for `None`.
///
/// # Errors
/// Returns `FileTransferError::Compression` when the compression backend fails.
///
/// # Panics
/// This function does not panic.
pub fn compress_owned_bytes(
    data: Vec<u8>,
    algorithm: CompressionAlgorithm,
) -> Result<Vec<u8>, FileTransferError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data),
        CompressionAlgorithm::Zstd | CompressionAlgorithm::ZstdLevel(_) => {
            compress_bytes(&data, algorithm)
        }
    }
}

/// Decompresses a byte slice using the requested algorithm.
///
/// # Errors
/// Returns `FileTransferError::Compression` when zstd initialization or decoding fails.
///
/// # Panics
/// This function does not panic.
pub fn decompress_bytes(
    data: &[u8],
    algorithm: CompressionAlgorithm,
    uncompressed_size: Option<usize>,
) -> Result<Vec<u8>, FileTransferError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd | CompressionAlgorithm::ZstdLevel(_) => {
            let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(data))
                .map_err(|e| FileTransferError::Compression(e.to_string()))?;
            let mut output = Vec::with_capacity(uncompressed_size.unwrap_or(0));
            decoder
                .read_to_end(&mut output)
                .map_err(|e| FileTransferError::Compression(e.to_string()))?;
            Ok(output)
        }
    }
}

/// Decompresses an owned payload while reusing its allocation for `None`.
///
/// # Errors
/// Returns `FileTransferError::Compression` when the decompression backend fails.
///
/// # Panics
/// This function does not panic.
pub fn decompress_owned_bytes(
    data: Vec<u8>,
    algorithm: CompressionAlgorithm,
    uncompressed_size: Option<usize>,
) -> Result<Vec<u8>, FileTransferError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data),
        CompressionAlgorithm::Zstd | CompressionAlgorithm::ZstdLevel(_) => {
            decompress_bytes(&data, algorithm, uncompressed_size)
        }
    }
}

/// Streams compression from a reader into a writer.
///
/// # Errors
/// Returns `FileTransferError::Compression` when the compression backend fails or when
/// reading from or writing to the provided streams fails.
///
/// # Panics
/// This function does not panic.
pub fn compress_reader<R: Read, W: Write>(
    mut reader: R,
    writer: W,
    algorithm: CompressionAlgorithm,
) -> Result<(), FileTransferError> {
    match algorithm {
        CompressionAlgorithm::None => {
            let mut writer = writer;
            std::io::copy(&mut reader, &mut writer)
                .map_err(|e| FileTransferError::Compression(e.to_string()))?;
            Ok(())
        }
        CompressionAlgorithm::Zstd => {
            compress_stream_with_level(reader, writer, DEFAULT_ZSTD_LEVEL)
        }
        CompressionAlgorithm::ZstdLevel(level) => compress_stream_with_level(reader, writer, level),
    }
}

/// Streams decompression from a reader into a writer.
///
/// # Errors
/// Returns `FileTransferError::Compression` when zstd initialization or decoding fails
/// or when reading from or writing to the provided streams fails.
///
/// # Panics
/// This function does not panic.
pub fn decompress_reader<R: Read, W: Write>(
    mut reader: R,
    mut writer: W,
    algorithm: CompressionAlgorithm,
) -> Result<(), FileTransferError> {
    match algorithm {
        CompressionAlgorithm::None => {
            std::io::copy(&mut reader, &mut writer)
                .map_err(|e| FileTransferError::Compression(e.to_string()))?;
            Ok(())
        }
        CompressionAlgorithm::Zstd | CompressionAlgorithm::ZstdLevel(_) => {
            let mut decoder = zstd::stream::Decoder::new(reader)
                .map_err(|e| FileTransferError::Compression(e.to_string()))?;
            std::io::copy(&mut decoder, &mut writer)
                .map_err(|e| FileTransferError::Compression(e.to_string()))?;
            Ok(())
        }
    }
}

fn compress_with_level(data: &[u8], level: i32) -> Result<Vec<u8>, FileTransferError> {
    zstd::stream::encode_all(data, level).map_err(|e| FileTransferError::Compression(e.to_string()))
}

fn compress_stream_with_level<R: Read, W: Write>(
    mut reader: R,
    writer: W,
    level: i32,
) -> Result<(), FileTransferError> {
    let mut encoder = zstd::stream::Encoder::new(writer, level)
        .map_err(|e| FileTransferError::Compression(e.to_string()))?;
    std::io::copy(&mut reader, &mut encoder)
        .map_err(|e| FileTransferError::Compression(e.to_string()))?;
    encoder
        .finish()
        .map_err(|e| FileTransferError::Compression(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{compress_owned_bytes, decompress_owned_bytes};
    use crate::CompressionAlgorithm;

    #[test]
    fn none_reuses_owned_payload_allocation() {
        let payload = vec![7_u8; 1024 * 1024];
        let allocation = payload.as_ptr();
        let encoded = compress_owned_bytes(payload, CompressionAlgorithm::None).expect("encode");
        assert_eq!(encoded.as_ptr(), allocation);

        let allocation = encoded.as_ptr();
        let decoded =
            decompress_owned_bytes(encoded, CompressionAlgorithm::None, Some(1024 * 1024))
                .expect("decode");
        assert_eq!(decoded.as_ptr(), allocation);
    }
}
