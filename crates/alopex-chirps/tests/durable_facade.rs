#![cfg(feature = "durable-iggy")]

use alopex_chirps::{
    DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES, DurableBuildError, DurableBuilder,
    DurableCheckpointConfig, DurableConfig, DurableCredential, DurableCredentialProvider,
    DurableCredentialProviderError, DurableExtensionConfig, DurableLeaseConfig,
    DurablePartitionProjection, DurableProfile, DurableResourceConfig, DurableRoutingConfig,
    DurableSendError, DurableSubscriptionError, DurableTlsConfig, NodeId,
};
use alopex_chirps_core::durable::{
    ConfirmationBoundary, InitialPosition, LifecyclePhase, Readiness, SubscriptionId,
    UnavailableReason,
};

#[tokio::test]
async fn v07_task_4_6_builder_is_additive_and_unconfigured_never_falls_back() {
    let source = NodeId::new();
    let target = NodeId::new();
    let mut handle = DurableBuilder::new(source)
        .inbox_generation(7)
        .explicit_partitions(3)
        .build()
        .expect("valid explicit durable routing");

    let prepared = handle
        .prepare(target, b"order-7".to_vec(), b"payload")
        .expect("preparation is local");

    assert_eq!(prepared.source(), source);
    assert_eq!(prepared.target(), target);
    assert_eq!(prepared.generation(), 7);
    assert!(prepared.partition() < 3);
    assert!(matches!(
        handle
            .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .await,
        Err(DurableSendError::Unconfigured)
    ));
    assert_eq!(handle.health().lifecycle(), LifecyclePhase::Starting);
    assert_eq!(
        handle.health().readiness(),
        Readiness::Unavailable(UnavailableReason::Unconfigured)
    );
    assert!(matches!(
        handle
            .create_subscription(
                SubscriptionId::from_bytes([0x33; 16]),
                target,
                prepared.partition(),
                [0x44; 32],
                InitialPosition::EarliestRetained,
            )
            .await,
        Err(DurableSubscriptionError::Unconfigured)
    ));
    let shutdown = handle
        .shutdown(tokio::time::Instant::now())
        .await
        .expect("unconfigured shutdown still returns a core report");
    assert!(shutdown.transport_closed());
    assert!(shutdown.workers_joined());
    assert_eq!(
        handle
            .shutdown(tokio::time::Instant::now())
            .await
            .expect("repeated shutdown returns the same core report"),
        shutdown
    );
}

#[test]
fn v07_task_4_6_builder_rejects_implicit_or_balanced_routing() {
    let source = NodeId::new();

    assert!(matches!(
        DurableBuilder::new(source).explicit_partitions(3).build(),
        Err(DurableBuildError::MissingGeneration)
    ));
    assert!(matches!(
        DurableBuilder::new(source)
            .inbox_generation(7)
            .explicit_partitions(0)
            .build(),
        Err(DurableBuildError::InvalidPartitions)
    ));
}

#[tokio::test]
async fn v07_task_4_6_production_composition_requires_connection_config_per_partition() {
    struct Credentials;

    #[async_trait::async_trait]
    impl DurableCredentialProvider for Credentials {
        async fn resolve(
            &self,
            _reference: &str,
        ) -> Result<DurableCredential, DurableCredentialProviderError> {
            panic!("invalid configuration must fail before credential resolution")
        }
    }

    let config = DurableConfig::new(
        "127.0.0.1:8090".parse().unwrap(),
        DurableTlsConfig::new("broker.example".into(), vec![vec![1]]),
        "secret/durable".into(),
        DurableProfile::OsSyncedAccepted,
        DurableRoutingConfig::new(7, 2),
        DurableResourceConfig::new(1, 1, Vec::new()),
        DurableCheckpointConfig::new(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            1,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        DurableLeaseConfig::new(30_000, tokio::time::Duration::from_secs(10)),
        DurableExtensionConfig::required(1024 * 1024),
    );
    let error = DurableBuilder::new(NodeId::new())
        .connect(config, &Credentials, tokio::time::Instant::now())
        .await
        .err()
        .expect("missing connection configs must reject production composition");

    assert_eq!(error, DurableBuildError::BackendConfiguration);

    let certificate = rcgen::generate_simple_self_signed(vec!["broker.example".into()]).unwrap();
    let invalid_projection = DurableConfig::new(
        "127.0.0.1:8090".parse().unwrap(),
        DurableTlsConfig::new(
            "broker.example".into(),
            vec![certificate.serialize_der().unwrap()],
        ),
        "secret/durable".into(),
        DurableProfile::OsSyncedAccepted,
        DurableRoutingConfig::new(7, 1),
        DurableResourceConfig::new(
            1,
            1,
            vec![DurablePartitionProjection::new(
                0, [0; 16], 4, [0; 20], 6, 7, true, [0; 32], [0; 32], [0; 32],
            )],
        ),
        DurableCheckpointConfig::new(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            1,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        DurableLeaseConfig::new(30_000, tokio::time::Duration::from_secs(10)),
        DurableExtensionConfig::required(1024 * 1024),
    );
    let error = DurableBuilder::new(NodeId::new())
        .connect(
            invalid_projection,
            &Credentials,
            tokio::time::Instant::now(),
        )
        .await
        .err()
        .expect("invalid capability projection must fail before credential resolution");

    assert_eq!(error, DurableBuildError::BackendConfiguration);
}

#[tokio::test]
async fn v07_task_4_6_official_development_profile_fails_closed_before_secret_resolution() {
    struct Credentials;

    #[async_trait::async_trait]
    impl DurableCredentialProvider for Credentials {
        async fn resolve(
            &self,
            _reference: &str,
        ) -> Result<DurableCredential, DurableCredentialProviderError> {
            panic!("invalid development readback must fail before credential resolution")
        }
    }

    let profile = DurableProfile::broker_accepted(
        b"[system.message_deduplication]\nenabled = true\n".to_vec(),
    );
    assert!(!format!("{profile:?}").contains("message_deduplication"));
    let config = DurableConfig::new(
        "127.0.0.1:8090".parse().unwrap(),
        DurableTlsConfig::new("broker.example".into(), vec![vec![1]]),
        "secret/durable".into(),
        profile,
        DurableRoutingConfig::new(7, 1),
        DurableResourceConfig::new(
            1,
            2,
            vec![DurablePartitionProjection::new(
                0, [3; 16], 4, [5; 20], 6, 7, true, [8; 32], [9; 32], [10; 32],
            )],
        ),
        DurableCheckpointConfig::new(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            1,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        DurableLeaseConfig::new(30_000, tokio::time::Duration::from_secs(10)),
        DurableExtensionConfig::required(1024 * 1024),
    );

    let error = DurableBuilder::new(NodeId::new())
        .connect(config, &Credentials, tokio::time::Instant::now())
        .await
        .err()
        .expect("development profile must reject enabled broker deduplication");

    assert_eq!(error, DurableBuildError::BackendConfiguration);
}
