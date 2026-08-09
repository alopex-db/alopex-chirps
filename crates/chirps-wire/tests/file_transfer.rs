use alopex_chirps_wire::file_transfer::{
    CancelRequest, ChunkAck, ChunkMeta, ChunkRequest, CompressionAlgorithm, ExistsRequest,
    ExistsResponse, FileInfo, FileMetadata, FileTransferFrame, FileTransferMessage, FileType,
    HashAlgorithm, ListRequest, ListResponse, ManifestAck, MetadataRequest, MetadataResponse,
    ProgressUpdate, RemoveRequest, RemoveResponse, RetryPolicy, SyncRequest, TransferComplete,
    TransferErrorMessage, TransferManifest, TransferMode, TransferOptions, TransferRequest,
    TransferResponse, TransferSessionId, TransferState,
};
use alopex_chirps_wire::frame::{
    Frame, GossipMessage, MemberStatus, MembershipUpdate, RaftFrame, UserMessage,
};
use alopex_chirps_wire::node_id::NodeId;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::net::SocketAddr;
use std::time::Duration;

fn assert_roundtrip<T>(value: &T)
where
    T: Serialize + DeserializeOwned,
{
    let bytes = bincode::serialize(value).expect("serialize");
    let decoded: T = bincode::deserialize(&bytes).expect("deserialize");
    let reencoded = bincode::serialize(&decoded).expect("re-serialize");
    assert_eq!(bytes, reencoded);
}

fn sample_session_id() -> TransferSessionId {
    TransferSessionId::from([7u8; 16])
}

fn sample_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_retries: 2,
        initial_delay: Duration::from_millis(20),
        max_delay: Duration::from_secs(2),
        backoff_multiplier: 1.5,
        jitter: true,
    }
}

fn sample_options() -> TransferOptions {
    TransferOptions {
        chunk_size: 4096,
        concurrency: 3,
        compression: CompressionAlgorithm::ZstdLevel(3),
        bandwidth_limit: Some(64 * 1024),
        retry_policy: sample_retry_policy(),
        verify_on_complete: false,
        hash_algorithm: HashAlgorithm::Blake3,
        resumable: true,
        overwrite: true,
        mode: TransferMode::Move,
        preserve_metadata: false,
        follow_symlinks: false,
    }
}

fn sample_metadata() -> FileMetadata {
    FileMetadata {
        created_at: Some(1),
        modified_at: Some(2),
        permissions: Some(0o644),
        file_type: FileType::File,
        size: Some(123),
    }
}

fn sample_manifest() -> TransferManifest {
    TransferManifest {
        version: 1,
        session_id: sample_session_id(),
        source_path: "src/path.bin".to_string(),
        dest_path: "dest/path.bin".to_string(),
        file_size: 123,
        file_hash: vec![1, 2, 3, 4],
        hash_algorithm: HashAlgorithm::Sha256,
        chunk_size: 64,
        chunk_count: 1,
        chunks: vec![ChunkMeta {
            index: 0,
            offset: 0,
            size: 123,
            checksum: 42,
        }],
        metadata: Some(sample_metadata()),
        options: sample_options(),
        created_at: 10,
    }
}

#[test]
fn file_transfer_message_roundtrip() {
    let messages = vec![
        FileTransferMessage::TransferRequest(TransferRequest {
            source_path: "src.bin".to_string(),
            dest_path: "dest.bin".to_string(),
            file_size: 12,
            chunk_count: 1,
            chunk_size: 12,
            mode: TransferMode::Copy,
            options: sample_options(),
            metadata: Some(sample_metadata()),
        }),
        FileTransferMessage::TransferResponse(TransferResponse {
            accepted: true,
            rejection_reason: None,
            existing_chunks: vec![0, 2],
        }),
        FileTransferMessage::Manifest(sample_manifest()),
        FileTransferMessage::ManifestAck(ManifestAck {
            accepted: true,
            skip_chunks: vec![1],
            error: None,
        }),
        FileTransferMessage::ChunkAck(ChunkAck {
            index: 1,
            verified: true,
            error: None,
        }),
        FileTransferMessage::ChunkRequest(ChunkRequest {
            indices: vec![0, 3],
        }),
        FileTransferMessage::Progress(ProgressUpdate {
            chunks_completed: 2,
            bytes_transferred: 256,
            state: TransferState::InProgress,
        }),
        FileTransferMessage::Cancel(CancelRequest {
            reason: "cancelled".to_string(),
        }),
        FileTransferMessage::Complete(TransferComplete {
            bytes_transferred: 256,
            duration_ms: 42,
            file_hash: vec![1, 2, 3],
            hash_algorithm: HashAlgorithm::Sha256,
        }),
        FileTransferMessage::Error(TransferErrorMessage {
            code: 404,
            message: "missing".to_string(),
            recoverable: false,
        }),
        FileTransferMessage::ExistsRequest(ExistsRequest {
            path: "exists.bin".to_string(),
        }),
        FileTransferMessage::ExistsResponse(ExistsResponse {
            exists: true,
            is_file: true,
            is_directory: false,
        }),
        FileTransferMessage::RemoveRequest(RemoveRequest {
            path: "remove.bin".to_string(),
            recursive: true,
            ignore_not_found: false,
        }),
        FileTransferMessage::RemoveResponse(RemoveResponse {
            success: true,
            error: None,
        }),
        FileTransferMessage::MetadataRequest(MetadataRequest {
            path: "meta.bin".to_string(),
        }),
        FileTransferMessage::MetadataResponse(MetadataResponse {
            found: true,
            metadata: Some(sample_metadata()),
            size: Some(123),
            error: None,
        }),
        FileTransferMessage::ListRequest(ListRequest {
            path: "/tmp".to_string(),
            recursive: true,
            include_hidden: false,
        }),
        FileTransferMessage::ListResponse(ListResponse {
            files: vec![FileInfo {
                path: "/tmp/file".to_string(),
                size: 10,
                modified_at: 5,
                file_type: FileType::File,
            }],
            error: None,
        }),
        FileTransferMessage::SyncRequest(SyncRequest {
            source_path: "remote/source.bin".to_string(),
            dest_path: "local/destination.bin".to_string(),
            options: sample_options(),
        }),
    ];

    for message in messages {
        assert_roundtrip(&message);
    }
}

#[test]
fn frame_roundtrip_with_file_transfer() {
    let file_transfer = Frame::FileTransfer(FileTransferFrame {
        session_id: sample_session_id(),
        message: FileTransferMessage::Manifest(sample_manifest()),
    });
    assert_roundtrip(&file_transfer);

    let ping = Frame::Ping {
        seq: 1,
        from: NodeId::new(),
    };
    assert_roundtrip(&ping);

    let ack = Frame::Ack {
        seq: 2,
        from: NodeId::new(),
    };
    assert_roundtrip(&ack);

    let ping_req = Frame::PingReq {
        seq: 3,
        from: NodeId::new(),
        target: NodeId::new(),
    };
    assert_roundtrip(&ping_req);

    let gossip = Frame::Gossip(GossipMessage {
        updates: vec![MembershipUpdate {
            node_id: NodeId::new(),
            incarnation: 1,
            addr: SocketAddr::from(([127, 0, 0, 1], 8080)),
            status: MemberStatus::Alive,
        }],
    });
    assert_roundtrip(&gossip);

    let user = Frame::User(UserMessage {
        payload: b"user".to_vec(),
    });
    assert_roundtrip(&user);

    let raft = Frame::Raft(RaftFrame {
        group_id: 7,
        payload: vec![1, 2, 3],
    });
    assert_roundtrip(&raft);

    let raft_snapshot = Frame::RaftSnapshot(RaftFrame {
        group_id: 8,
        payload: vec![4, 5, 6],
    });
    assert_roundtrip(&raft_snapshot);
}
