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
        if data.len() > MAX_WIRE_CHUNK_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "encoded chunk data exceeds maximum size",
            ));
        }
        stream.write_all(&[CHUNK_STREAM_MAGIC]).await?;
        stream.write_all(session_id.as_bytes()).await?;
        stream.write_all(&chunk_index.to_le_bytes()).await?;
        stream.write_all(&(data.len() as u32).to_le_bytes()).await?;
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
        let mut session_id_bytes = [0u8; 16];
        stream
            .read_exact(&mut session_id_bytes)
            .await
            .map_err(map_read_exact)?;
        let session_id = TransferSessionId::from_bytes(&session_id_bytes)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid session id"))?;

        let mut index_bytes = [0u8; 4];
        stream
            .read_exact(&mut index_bytes)
            .await
            .map_err(map_read_exact)?;
        let chunk_index = u32::from_le_bytes(index_bytes);

        let mut len_bytes = [0u8; 4];
        stream
            .read_exact(&mut len_bytes)
            .await
            .map_err(map_read_exact)?;
        let data_len = u32::from_le_bytes(len_bytes) as usize;
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
