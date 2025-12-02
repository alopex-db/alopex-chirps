use serde::Serialize;
use tracing::{info, warn};

use crate::handshake::NegotiatedCapabilities;

/// トランスポートで観測する構造化イベント。
#[derive(Serialize, Debug, Clone)]
#[serde(tag = "event")]
pub enum TransportEvent {
    #[serde(rename = "peer_connected")]
    PeerConnected {
        node_id: String,
        protocol_version: u16,
        capabilities: NegotiatedCapabilities,
    },
    #[serde(rename = "peer_disconnected")]
    PeerDisconnected {
        node_id: String,
        reason: String,
        buffered_messages: usize,
    },
    #[serde(rename = "retransmission_started")]
    RetransmissionStarted {
        node_id: String,
        message_count: usize,
    },
    #[serde(rename = "retransmission_completed")]
    RetransmissionCompleted {
        node_id: String,
        duration_ms: u64,
        success_count: usize,
        failed_count: usize,
    },
    #[serde(rename = "buffer_overflow")]
    BufferOverflow {
        node_id: String,
        dropped_count: usize,
        buffer_size: usize,
    },
    #[serde(rename = "backpressure_triggered")]
    BackpressureTriggered {
        stream_kind: String,
        queue_size: usize,
        queue_limit: usize,
    },
    #[serde(rename = "version_mismatch")]
    VersionMismatch {
        remote_version: u16,
        local_version: u16,
    },
}

/// イベントをtracingの構造化フィールドとして出力する。
pub fn emit_event(event: TransportEvent) {
    match event {
        TransportEvent::PeerConnected {
            node_id,
            protocol_version,
            capabilities,
        } => info!(
            event = "peer_connected",
            %node_id,
            protocol_version,
            capabilities = ?capabilities,
            "peer_connected"
        ),
        TransportEvent::PeerDisconnected {
            node_id,
            reason,
            buffered_messages,
        } => info!(
            event = "peer_disconnected",
            %node_id,
            %reason,
            buffered_messages,
            "peer_disconnected"
        ),
        TransportEvent::RetransmissionStarted {
            node_id,
            message_count,
        } => info!(
            event = "retransmission_started",
            %node_id,
            message_count,
            "retransmission_started"
        ),
        TransportEvent::RetransmissionCompleted {
            node_id,
            duration_ms,
            success_count,
            failed_count,
        } => info!(
            event = "retransmission_completed",
            %node_id,
            duration_ms,
            success_count,
            failed_count,
            "retransmission_completed"
        ),
        TransportEvent::BufferOverflow {
            node_id,
            dropped_count,
            buffer_size,
        } => warn!(
            event = "buffer_overflow",
            %node_id,
            dropped_count,
            buffer_size,
            "buffer_overflow"
        ),
        TransportEvent::BackpressureTriggered {
            stream_kind,
            queue_size,
            queue_limit,
        } => warn!(
            event = "backpressure_triggered",
            %stream_kind,
            queue_size,
            queue_limit,
            "backpressure_triggered"
        ),
        TransportEvent::VersionMismatch {
            remote_version,
            local_version,
        } => warn!(
            event = "version_mismatch",
            remote_version, local_version, "version_mismatch"
        ),
    }
}
