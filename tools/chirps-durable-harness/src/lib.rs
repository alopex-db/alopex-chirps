//! Verification-only independent oracle for Chirps Durable fault evidence.

pub mod creation;
pub mod oracle;
pub mod proxy;

pub use creation::{
    CreationCorpusError, CreationCorpusInputs, CreationCorpusOracle, CreationExpectation,
    CreationManifestReadback, CreationMaterializationBinding, CreationReadbackError,
    CreationSnapshot, CreationTransitionObservation, CreationVerdict, MaterializedCreationCase,
    OwnerReadback, materialize_creation_case, read_creation_snapshot, verify_creation_history,
};

pub use oracle::{
    APPEND_ONE_SYNCED_CODE, AckObservation, CheckpointObservation, CreationManifestObservation,
    CreationObservation, CreationState, DeliveryObservation, EffectObservation, ExactLocation,
    ObservationKind, OracleAttemptId, OracleError, OracleIntent, OracleMessageId,
    OracleObservation, OracleRecord, OracleStore, OracleVerdict, OracleViolation,
    ReceiptObservation, RecoveryObservation, RecoveryState, ResponseObservation, StoredReadback,
    WireObservation, WireStage, verify_attempt, verify_log,
};
pub use proxy::{FaultProxy, ProxyError};

/// Identifies the staged verification harness without exposing it as a package.
pub const HARNESS_NAME: &str = "chirps-fault-oracle";

#[cfg(test)]
mod tests {
    use super::HARNESS_NAME;

    #[test]
    fn identifies_the_fault_oracle() {
        assert_eq!(HARNESS_NAME, "chirps-fault-oracle");
    }
}
