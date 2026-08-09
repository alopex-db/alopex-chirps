use crate::TransferSessionId;
use crate::options::MAX_CHUNK_SIZE;
use quinn::{ReadExactError, RecvStream, SendStream};
use std::io::{self, ErrorKind};

/// Magic byte prefix for chunk streams.
pub const CHUNK_STREAM_MAGIC: u8 = 0x46;
/// Maximum encoded chunk size, including bounded Zstd expansion overhead.
pub const MAX_WIRE_CHUNK_SIZE: usize = MAX_CHUNK_SIZE + 128 * 1024;

/// Decoded chunk stream header. The payload and optional trailing checksum are
/// read separately so the receiver can select framing from the session manifest.
pub struct ChunkStreamHeader {
    pub session_id: TransferSessionId,
    pub chunk_index: u32,
    pub data_len: usize,
}

/// Codec for chunk stream frames.
pub struct ChunkStreamCodec;

impl ChunkStreamCodec {
    /// Builds the fixed wire header for a chunk frame.
    pub fn encode_header(
        session_id: &TransferSessionId,
        chunk_index: u32,
        data_len: usize,
    ) -> io::Result<[u8; 25]> {
        if data_len > MAX_WIRE_CHUNK_SIZE || data_len > u32::MAX as usize {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "encoded chunk data exceeds maximum size",
            ));
        }
        let mut header = [0u8; 25];
        header[0] = CHUNK_STREAM_MAGIC;
        header[1..17].copy_from_slice(session_id.as_bytes());
        header[17..21].copy_from_slice(&chunk_index.to_le_bytes());
        header[21..25].copy_from_slice(&(data_len as u32).to_le_bytes());
        Ok(header)
    }

    /// Parses the fixed wire header, including its magic byte.
    pub fn decode_header_bytes(header: &[u8; 25]) -> io::Result<ChunkStreamHeader> {
        if header[0] != CHUNK_STREAM_MAGIC {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid chunk stream magic",
            ));
        }
        let session_id = TransferSessionId::from_bytes(&header[1..17])
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid session id"))?;
        let chunk_index = u32::from_le_bytes(header[17..21].try_into().expect("fixed header"));
        let data_len =
            u32::from_le_bytes(header[21..25].try_into().expect("fixed header")) as usize;
        if data_len > MAX_WIRE_CHUNK_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "encoded chunk data exceeds maximum size",
            ));
        }
        Ok(ChunkStreamHeader {
            session_id,
            chunk_index,
            data_len,
        })
    }

    /// Encodes a chunk frame to the provided send stream.
    ///
    /// # Errors
    /// Returns an `io::Error` with `ErrorKind::InvalidInput` when the payload exceeds
    /// `MAX_CHUNK_SIZE`, or propagates any I/O error from writing to the stream.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn encode(
        stream: &mut SendStream,
        session_id: &TransferSessionId,
        chunk_index: u32,
        data: &[u8],
    ) -> io::Result<()> {
        let header = Self::encode_header(session_id, chunk_index, data.len())?;
        // Preserve the caller-owned payload allocation. A single concatenated
        // frame would add a 1 MiB copy for every chunk; Quinn coalesces writes
        // while the stream remains writable, so keep header and payload
        // zero-copy at the application boundary.
        stream.write_all(&header).await?;
        stream.write_all(data).await?;
        Ok(())
    }

    /// Encodes a manifest-v2 chunk with its uncompressed XXHash64 trailer.
    pub async fn encode_with_checksum(
        stream: &mut SendStream,
        session_id: &TransferSessionId,
        chunk_index: u32,
        data: &[u8],
        checksum: u64,
    ) -> io::Result<()> {
        Self::encode(stream, session_id, chunk_index, data).await?;
        stream.write_all(&checksum.to_le_bytes()).await?;
        Ok(())
    }

    /// Decodes a chunk frame from the provided receive stream.
    ///
    /// # Errors
    /// Returns an `io::Error` with `ErrorKind::InvalidData` when the session id is invalid
    /// or the payload exceeds `MAX_CHUNK_SIZE`, or propagates any I/O error from reading
    /// the stream.
    ///
    /// # Panics
    /// This method does not panic.
    pub async fn decode(stream: &mut RecvStream) -> io::Result<(TransferSessionId, u32, Vec<u8>)> {
        let header = Self::decode_header(stream).await?;
        let data = Self::decode_payload(stream, header.data_len).await?;
        Ok((header.session_id, header.chunk_index, data))
    }

    /// Decodes the header after the transport has consumed the magic byte.
    pub async fn decode_header(stream: &mut RecvStream) -> io::Result<ChunkStreamHeader> {
        let mut header = [0u8; 24];
        stream
            .read_exact(&mut header)
            .await
            .map_err(map_read_exact)?;
        let mut full = [0u8; 25];
        full[0] = CHUNK_STREAM_MAGIC;
        full[1..].copy_from_slice(&header);
        Self::decode_header_bytes(&full)
    }

    /// Reads a bounded payload described by [`ChunkStreamHeader`].
    pub async fn decode_payload(stream: &mut RecvStream, data_len: usize) -> io::Result<Vec<u8>> {
        if data_len > MAX_WIRE_CHUNK_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "encoded chunk data exceeds maximum size",
            ));
        }
        let mut data = vec![0u8; data_len];
        stream.read_exact(&mut data).await.map_err(map_read_exact)?;
        Ok(data)
    }

    /// Reads the manifest-v2 uncompressed XXHash64 trailer.
    pub async fn decode_checksum(stream: &mut RecvStream) -> io::Result<u64> {
        let mut checksum = [0u8; 8];
        stream
            .read_exact(&mut checksum)
            .await
            .map_err(map_read_exact)?;
        Ok(u64::from_le_bytes(checksum))
    }
}

fn map_read_exact(err: ReadExactError) -> io::Error {
    match err {
        ReadExactError::FinishedEarly(bytes_read) => io::Error::new(
            ErrorKind::UnexpectedEof,
            format!("stream finished early after {bytes_read} bytes"),
        ),
        ReadExactError::ReadError(read_err) => read_err.into(),
    }
}
