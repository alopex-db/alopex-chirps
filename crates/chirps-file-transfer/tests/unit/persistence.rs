use alopex_chirps_file_transfer::{
    ChunkMeta, ChunkTracker, FileTransferConfig, HashAlgorithm, SessionPersistence,
    TransferKind, TransferManifest, TransferMode, TransferOptions, TransferSession,
    TransferSessionId, TransferState,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

fn build_manifest(session_id: TransferSessionId, data: &[u8]) -> TransferManifest {
    let checksum = xxhash_rust::xxh64::xxh64(data, 0);
    TransferManifest {
        version: TransferManifest::CURRENT_VERSION,
        session_id,
        source_path: "src.txt".into(),
        dest_path: "dst.txt".into(),
        file_size: data.len() as u64,
        file_hash: data.to_vec(),
        hash_algorithm: HashAlgorithm::XxHash64,
        chunk_size: data.len() as u32,
        chunk_count: 1,
        chunks: vec![ChunkMeta {
            index: 0,
            offset: 0,
            size: data.len() as u32,
            checksum,
        }],
        metadata: None,
        options: TransferOptions::default(),
        created_at: 0,
    }
}

fn build_session(session_id: TransferSessionId) -> TransferSession {
    let data = b"hello";
    let manifest = build_manifest(session_id, data);
    let options = TransferOptions::default();
    let chunk_tracker = ChunkTracker::new(manifest.chunk_count, options.retry_policy.max_retries);
    let mut session = TransferSession::new(
        session_id,
        TransferKind::Send,
        TransferMode::Copy,
        Default::default(),
        Vec::new(),
        "src.txt".into(),
        "dst.txt".into(),
        manifest,
        chunk_tracker,
        options,
    );
    session.state = TransferState::Paused;
    session
}

#[tokio::test]
async fn session_persistence_round_trip() {
    let dir = tempdir().expect("tempdir");
    let mut config = FileTransferConfig::default();
    config.base_path = dir.path().to_path_buf();
    config.session_dir = Some(dir.path().join("sessions"));
    let persistence = SessionPersistence::new(&config);

    let session_id = TransferSessionId::new();
    let mut session = build_session(session_id);
    session.chunk_tracker.mark_completed(0);

    persistence.save(&session).await.expect("save session");
    let loaded = persistence.load(session_id).await.expect("load session");

    assert_eq!(loaded.id, session.id);
    assert_eq!(loaded.state, session.state);
    assert_eq!(loaded.manifest.file_size, session.manifest.file_size);
    assert_eq!(loaded.chunk_tracker.completed, session.chunk_tracker.completed);
}

#[tokio::test]
async fn session_persistence_gc_removes_expired() {
    let dir = tempdir().expect("tempdir");
    let mut config = FileTransferConfig::default();
    config.base_path = dir.path().to_path_buf();
    config.session_dir = Some(dir.path().join("sessions"));
    config.session_retention = Duration::from_millis(1);
    config.max_sessions = 10;
    let persistence = SessionPersistence::new(&config);

    let session_id = TransferSessionId::new();
    let mut session = build_session(session_id);
    let old_time = SystemTime::now()
        .checked_sub(Duration::from_secs(60))
        .unwrap_or(UNIX_EPOCH);
    session.created_at = old_time;
    session.updated_at = old_time;

    persistence.save(&session).await.expect("save session");
    persistence.gc().await.expect("gc");

    let result = persistence.load(session_id).await;
    assert!(result.is_err());
}
