use alopex_chirps::profile::{
    BackendCapabilities, EnvelopeMetadata, MessageProfile, ProfileError, enforce_profile,
    resolve_profile,
};
use alopex_chirps_core::backend::{BackendProfile, MessageBackend};
use alopex_chirps_core::error::TransportError;
use alopex_chirps_transport_quic::HandshakeMessage;
use alopex_chirps_wire::frame::{Frame, RaftFrame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use alopex_chirps_wire::{
    envelope::FrameEnvelopeV2,
    file_transfer::{
        CancelRequest, ChunkAck, ChunkMeta, ChunkRequest, ExistsRequest, ExistsResponse, FileInfo,
        FileMetadata, FileTransferFrame, FileTransferMessage, FileType, HashAlgorithm, ListRequest,
        ListResponse, ManifestAck, MetadataRequest, MetadataResponse, ProgressUpdate,
        RemoveRequest, RemoveResponse, SyncRequest, TransferComplete, TransferErrorMessage,
        TransferManifest, TransferMode, TransferOptions, TransferRequest, TransferResponse,
        TransferSessionId, TransferState,
    },
    frame::{GossipMessage, MemberStatus, MembershipUpdate},
};
#[cfg(feature = "hlc")]
use alopex_chirps_wire::{
    frame::{HlcEventId, HlcGossipMessage, StampedMembershipUpdate},
    hlc::HybridTimestamp,
};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

#[derive(Default)]
struct CountingBackend {
    sends: AtomicUsize,
}

#[async_trait]
impl MessageBackend for CountingBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            durable: true,
            ..BackendCapabilities::default()
        }
    }

    async fn send(&self, _target: NodeId, _frame: Frame) -> Result<(), TransportError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn close(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        Vec::new()
    }
}

fn user_frame() -> Frame {
    Frame::User(UserMessage {
        payload: b"hello".to_vec(),
    })
}

#[test]
fn control_pass_through_for_user_frame() {
    let frame = user_frame();
    let eff = enforce_profile(&frame, MessageProfile::Control).unwrap();
    assert_eq!(eff, MessageProfile::Control);
}

#[test]
fn ephemeral_pass_through_when_not_raft() {
    let frame = user_frame();
    let eff = enforce_profile(&frame, MessageProfile::Ephemeral).unwrap();
    assert_eq!(eff, MessageProfile::Ephemeral);
}

#[test]
fn durable_is_not_implemented() {
    let frame = user_frame();
    let res = enforce_profile(&frame, MessageProfile::Durable);
    assert!(res.is_err(), "Durable should return NotImplemented error");
}

#[test]
fn durable_is_a_typed_capability_error_and_metadata_is_reserved() {
    let frame = user_frame();
    let error = resolve_profile(
        &frame,
        MessageProfile::Durable,
        BackendCapabilities::default(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ProfileError::Unsupported {
            profile: MessageProfile::Durable,
            ..
        }
    ));

    let metadata = EnvelopeMetadata {
        message_id: Some([7; 16]),
        sequence: Some(3),
        partition: Some(2),
        acknowledgement: Some(1),
        replay: true,
        checkpoint: Some(8),
        offset: Some(9),
    };
    let encoded = serde_json::to_vec(&metadata).unwrap();
    let decoded: EnvelopeMetadata = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, metadata);
}

#[tokio::test]
async fn default_backend_extension_never_falls_back_from_durable() {
    let backend = CountingBackend::default();
    let result = backend
        .send_with_profile(
            NodeId::new(),
            user_frame(),
            BackendProfile::Durable,
            EnvelopeMetadata::default(),
        )
        .await;

    assert!(matches!(result, Err(TransportError::NotImplemented(_))));
    assert_eq!(backend.sends.load(Ordering::SeqCst), 0);
}

#[test]
fn raft_frames_should_force_control_and_warn() {
    let frame = Frame::Raft(RaftFrame {
        group_id: 1,
        payload: Vec::new(),
    });
    let eff = enforce_profile(&frame, MessageProfile::Ephemeral).unwrap();
    assert_eq!(eff, MessageProfile::Control);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn v061_wire_fixtures() -> Vec<(&'static str, String)> {
    let node = NodeId::from([0x11; 16]);
    let peer = NodeId::from([0x22; 16]);
    let session_id = TransferSessionId::from([0x33; 16]);
    let metadata = FileMetadata {
        created_at: Some(11),
        modified_at: Some(12),
        permissions: Some(0o640),
        file_type: FileType::File,
        size: Some(13),
    };
    let options = TransferOptions {
        chunk_size: 4,
        concurrency: 2,
        compression: Default::default(),
        bandwidth_limit: Some(1024),
        retry_policy: Default::default(),
        verify_on_complete: true,
        hash_algorithm: HashAlgorithm::Sha256,
        resumable: true,
        overwrite: false,
        mode: TransferMode::Copy,
        preserve_metadata: true,
        follow_symlinks: false,
    };
    let transfer_frame = |message| {
        Frame::FileTransfer(FileTransferFrame {
            session_id,
            message,
        })
    };
    let frames = vec![
        (
            "ping",
            Frame::Ping {
                seq: 0x0102_0304_0506_0708,
                from: node,
            },
        ),
        (
            "ack",
            Frame::Ack {
                seq: 0x1112_1314_1516_1718,
                from: peer,
            },
        ),
        (
            "ping-req",
            Frame::PingReq {
                seq: 9,
                from: node,
                target: peer,
            },
        ),
        (
            "gossip",
            Frame::Gossip(GossipMessage {
                updates: vec![MembershipUpdate {
                    node_id: peer,
                    incarnation: 7,
                    addr: "127.0.0.1:9042".parse().unwrap(),
                    status: MemberStatus::Suspect,
                }],
            }),
        ),
        (
            "user",
            Frame::User(UserMessage {
                payload: vec![0x01, 0x02, 0xfe, 0xff],
            }),
        ),
        (
            "raft",
            Frame::Raft(RaftFrame {
                group_id: 42,
                payload: vec![3, 1, 4],
            }),
        ),
        (
            "raft-snapshot",
            Frame::RaftSnapshot(RaftFrame {
                group_id: 43,
                payload: vec![1, 5, 9],
            }),
        ),
        (
            "file-transfer-request",
            transfer_frame(FileTransferMessage::TransferRequest(TransferRequest {
                source_path: "src".to_owned(),
                dest_path: "dst".to_owned(),
                file_size: 13,
                chunk_count: 4,
                chunk_size: 4,
                mode: TransferMode::Copy,
                options: options.clone(),
                metadata: Some(metadata.clone()),
            })),
        ),
        (
            "file-transfer-response",
            transfer_frame(FileTransferMessage::TransferResponse(TransferResponse {
                accepted: true,
                rejection_reason: Some("ok".to_owned()),
                existing_chunks: vec![1, 3],
            })),
        ),
        (
            "file-transfer-manifest",
            transfer_frame(FileTransferMessage::Manifest(TransferManifest {
                version: 2,
                session_id,
                source_path: "src".to_owned(),
                dest_path: "dst".to_owned(),
                file_size: 13,
                file_hash: vec![0xaa, 0xbb],
                hash_algorithm: HashAlgorithm::Sha256,
                chunk_size: 4,
                chunk_count: 1,
                chunks: vec![ChunkMeta {
                    index: 0,
                    offset: 0,
                    size: 4,
                    checksum: 5,
                }],
                metadata: Some(metadata.clone()),
                options: options.clone(),
                created_at: 14,
            })),
        ),
        (
            "file-transfer-manifest-ack",
            transfer_frame(FileTransferMessage::ManifestAck(ManifestAck {
                accepted: true,
                skip_chunks: vec![2],
                error: Some("none".to_owned()),
            })),
        ),
        (
            "file-transfer-chunk-ack",
            transfer_frame(FileTransferMessage::ChunkAck(ChunkAck {
                index: 2,
                verified: true,
                error: Some("none".to_owned()),
            })),
        ),
        (
            "file-transfer-chunk-request",
            transfer_frame(FileTransferMessage::ChunkRequest(ChunkRequest {
                indices: vec![0, 2],
            })),
        ),
        (
            "file-transfer-progress",
            transfer_frame(FileTransferMessage::Progress(ProgressUpdate {
                chunks_completed: 2,
                bytes_transferred: 8,
                state: TransferState::InProgress,
            })),
        ),
        (
            "file-transfer-cancel",
            transfer_frame(FileTransferMessage::Cancel(CancelRequest {
                reason: "v061".to_owned(),
            })),
        ),
        (
            "file-transfer-complete",
            transfer_frame(FileTransferMessage::Complete(TransferComplete {
                bytes_transferred: 13,
                duration_ms: 15,
                file_hash: vec![0xaa, 0xbb],
                hash_algorithm: HashAlgorithm::Sha256,
            })),
        ),
        (
            "file-transfer-error",
            transfer_frame(FileTransferMessage::Error(TransferErrorMessage {
                code: 16,
                message: "error".to_owned(),
                recoverable: true,
            })),
        ),
        (
            "file-transfer-exists-request",
            transfer_frame(FileTransferMessage::ExistsRequest(ExistsRequest {
                path: "src".to_owned(),
            })),
        ),
        (
            "file-transfer-exists-response",
            transfer_frame(FileTransferMessage::ExistsResponse(ExistsResponse {
                exists: true,
                is_file: true,
                is_directory: false,
            })),
        ),
        (
            "file-transfer-remove-request",
            transfer_frame(FileTransferMessage::RemoveRequest(RemoveRequest {
                path: "dst".to_owned(),
                recursive: true,
                ignore_not_found: false,
            })),
        ),
        (
            "file-transfer-remove-response",
            transfer_frame(FileTransferMessage::RemoveResponse(RemoveResponse {
                success: true,
                error: Some("none".to_owned()),
            })),
        ),
        (
            "file-transfer-metadata-request",
            transfer_frame(FileTransferMessage::MetadataRequest(MetadataRequest {
                path: "src".to_owned(),
            })),
        ),
        (
            "file-transfer-metadata-response",
            transfer_frame(FileTransferMessage::MetadataResponse(MetadataResponse {
                found: true,
                metadata: Some(metadata),
                size: Some(13),
                error: Some("none".to_owned()),
            })),
        ),
        (
            "file-transfer-list-request",
            transfer_frame(FileTransferMessage::ListRequest(ListRequest {
                path: "src".to_owned(),
                recursive: true,
                include_hidden: false,
            })),
        ),
        (
            "file-transfer-list-response",
            transfer_frame(FileTransferMessage::ListResponse(ListResponse {
                files: vec![FileInfo {
                    path: "src/a".to_owned(),
                    size: 13,
                    modified_at: 12,
                    file_type: FileType::File,
                }],
                error: Some("none".to_owned()),
            })),
        ),
        (
            "file-transfer-sync-request",
            transfer_frame(FileTransferMessage::SyncRequest(SyncRequest {
                source_path: "src".to_owned(),
                dest_path: "dst".to_owned(),
                options,
            })),
        ),
        (
            "file-transfer-finalize-request",
            transfer_frame(FileTransferMessage::FinalizeRequest(TransferComplete {
                bytes_transferred: 13,
                duration_ms: 15,
                file_hash: vec![0xaa, 0xbb],
                hash_algorithm: HashAlgorithm::Sha256,
            })),
        ),
    ];

    #[cfg(feature = "hlc")]
    let frames = {
        let mut frames = frames;
        frames.push((
            "hlc-gossip",
            Frame::HlcGossip(HlcGossipMessage {
                event_id: HlcEventId {
                    source: node,
                    sequence: 17,
                },
                timestamp: HybridTimestamp::new(18, 19),
                updates: vec![StampedMembershipUpdate {
                    event_id: HlcEventId {
                        source: peer,
                        sequence: 20,
                    },
                    timestamp: HybridTimestamp::new(21, 22),
                    update: MembershipUpdate {
                        node_id: peer,
                        incarnation: 23,
                        addr: "127.0.0.1:9043".parse().unwrap(),
                        status: MemberStatus::Alive,
                    },
                }],
            }),
        ));
        frames
    };

    let mut fixtures = frames
        .into_iter()
        .map(|(name, frame)| (name, hex(&bincode::serialize(&frame).unwrap())))
        .collect::<Vec<_>>();
    fixtures.push((
        "handshake",
        hex(&bincode::serialize(&HandshakeMessage::new(node)).unwrap()),
    ));
    fixtures.push((
        "envelope",
        hex(&FrameEnvelopeV2 {
            kind: 3,
            seq: 7,
            ack_seq: 5,
            timestamp: 1_234_567,
            payload_len: 0,
            frame: Frame::User(UserMessage {
                payload: vec![0xaa, 0xbb],
            }),
        }
        .encode()),
    ));
    fixtures
}

#[test]
fn v061_quic_frame_handshake_and_envelope_bytes_match_golden() {
    let mut expected = vec![
        (
            "ping",
            "00000000080706050403020111111111111111111111111111111111".to_owned(),
        ),
        (
            "ack",
            "01000000181716151413121122222222222222222222222222222222".to_owned(),
        ),
        (
            "ping-req",
            "0200000009000000000000001111111111111111111111111111111122222222222222222222222222222222"
                .to_owned(),
        ),
        (
            "gossip",
            "030000000100000000000000222222222222222222222222222222220700000000000000000000007f000001522301000000"
                .to_owned(),
        ),
        (
            "user",
            "0400000004000000000000000102feff".to_owned(),
        ),
        (
            "raft",
            "050000002a000000000000000300000000000000030104".to_owned(),
        ),
        (
            "raft-snapshot",
            "060000002b000000000000000300000000000000010509".to_owned(),
        ),
        (
            "file-transfer-request",
            "070000003333333333333333333333333333333300000000030000000000000073726303000000000000006473740d00000000000000040000000400000000000000040000000000000002000000000000000000000001000400000000000003000000000000000000e1f5050a00000000000000000000000000000000000040010100000000010000000000010001010b00000000000000010c0000000000000001a001000000000000010d00000000000000"
                .to_owned(),
        ),
        (
            "file-transfer-response",
            "070000003333333333333333333333333333333301000000010102000000000000006f6b02000000000000000100000003000000".to_owned(),
        ),
        (
            "file-transfer-manifest",
            "070000003333333333333333333333333333333302000000020033333333333333333333333333333333030000000000000073726303000000000000006473740d000000000000000200000000000000aabb000000000400000001000000010000000000000000000000000000000000000004000000050000000000000001010b00000000000000010c0000000000000001a001000000000000010d00000000000000040000000000000002000000000000000000000001000400000000000003000000000000000000e1f5050a0000000000000000000000000000000000004001010000000001000000000001000e00000000000000"
                .to_owned(),
        ),
        (
            "file-transfer-manifest-ack",
            "070000003333333333333333333333333333333303000000010100000000000000020000000104000000000000006e6f6e65".to_owned(),
        ),
        (
            "file-transfer-chunk-ack",
            "07000000333333333333333333333333333333330400000002000000010104000000000000006e6f6e65".to_owned(),
        ),
        (
            "file-transfer-chunk-request",
            "07000000333333333333333333333333333333330500000002000000000000000000000002000000".to_owned(),
        ),
        (
            "file-transfer-progress",
            "07000000333333333333333333333333333333330600000002000000080000000000000001000000".to_owned(),
        ),
        (
            "file-transfer-cancel",
            "070000003333333333333333333333333333333307000000040000000000000076303631".to_owned(),
        ),
        (
            "file-transfer-complete",
            "0700000033333333333333333333333333333333080000000d000000000000000f000000000000000200000000000000aabb00000000".to_owned(),
        ),
        (
            "file-transfer-error",
            "0700000033333333333333333333333333333333090000001000000005000000000000006572726f7201".to_owned(),
        ),
        (
            "file-transfer-exists-request",
            "07000000333333333333333333333333333333330a0000000300000000000000737263".to_owned(),
        ),
        (
            "file-transfer-exists-response",
            "07000000333333333333333333333333333333330b000000010100".to_owned(),
        ),
        (
            "file-transfer-remove-request",
            "07000000333333333333333333333333333333330c00000003000000000000006473740100".to_owned(),
        ),
        (
            "file-transfer-remove-response",
            "07000000333333333333333333333333333333330d000000010104000000000000006e6f6e65".to_owned(),
        ),
        (
            "file-transfer-metadata-request",
            "07000000333333333333333333333333333333330e0000000300000000000000737263".to_owned(),
        ),
        (
            "file-transfer-metadata-response",
            "07000000333333333333333333333333333333330f0000000101010b00000000000000010c0000000000000001a001000000000000010d00000000000000010d000000000000000104000000000000006e6f6e65".to_owned(),
        ),
        (
            "file-transfer-list-request",
            "07000000333333333333333333333333333333331000000003000000000000007372630100".to_owned(),
        ),
        (
            "file-transfer-list-response",
            "070000003333333333333333333333333333333311000000010000000000000005000000000000007372632f610d000000000000000c00000000000000000000000104000000000000006e6f6e65".to_owned(),
        ),
        (
            "file-transfer-sync-request",
            "07000000333333333333333333333333333333331200000003000000000000007372630300000000000000647374040000000000000002000000000000000000000001000400000000000003000000000000000000e1f5050a000000000000000000000000000000000000400101000000000100000000000100".to_owned(),
        ),
        (
            "file-transfer-finalize-request",
            "0700000033333333333333333333333333333333130000000d000000000000000f000000000000000200000000000000aabb00000000".to_owned(),
        ),
    ];

    #[cfg(feature = "hlc")]
    expected.push((
        "hlc-gossip",
        "080000001111111111111111111111111111111111000000000000001200000000000000130000000100000000000000222222222222222222222222222222221400000000000000150000000000000016000000222222222222222222222222222222221700000000000000000000007f000001532300000000".to_owned(),
    ));
    expected.extend([
        (
            "handshake",
            "06001111111111111111111111111111111101010101".to_owned(),
        ),
        (
            "envelope",
            "0300000000000000070000000000000005000000000012d6870000000e040000000200000000000000aabb".to_owned(),
        ),
    ]);

    assert_eq!(v061_wire_fixtures(), expected);
}
