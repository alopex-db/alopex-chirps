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

#[tokio::test]
async fn official_numeric_resource_needs_no_compatible_projection_or_lease() {
    struct Credentials(std::sync::atomic::AtomicBool);
    #[async_trait::async_trait]
    impl DurableCredentialProvider for Credentials {
        async fn resolve(
            &self,
            reference: &str,
        ) -> Result<DurableCredential, DurableCredentialProviderError> {
            assert_eq!(reference, "official-test");
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            Err(DurableCredentialProviderError::Rejected)
        }
    }
    let certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    let credentials = Credentials(std::sync::atomic::AtomicBool::new(false));
    let config = DurableConfig::broker_accepted(
        "127.0.0.1:1".parse().unwrap(),
        DurableTlsConfig::new(
            "localhost".to_owned(),
            vec![certificate.serialize_der().unwrap()],
        ),
        "official-test".to_owned(),
        b"[system.message_deduplication]\nenabled = false\n".to_vec(),
        DurableRoutingConfig::new(1, 1),
        alopex_chirps::DurableDevelopmentResourceConfig::new(1, 2, vec![0]),
        DurableCheckpointConfig::new(
            checkpoint.path().to_path_buf(),
            1,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        1024 * 1024,
    );
    let error = DurableBuilder::new(NodeId::new())
        .connect(config, &credentials, tokio::time::Instant::now())
        .await
        .err()
        .unwrap();
    assert_eq!(error, DurableBuildError::CredentialRejected);
    assert!(credentials.0.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn official_numeric_resource_rejects_invalid_geometry_before_credentials() {
    struct NoCredentials;
    #[async_trait::async_trait]
    impl DurableCredentialProvider for NoCredentials {
        async fn resolve(
            &self,
            _: &str,
        ) -> Result<DurableCredential, DurableCredentialProviderError> {
            panic!("invalid official configuration reached credentials");
        }
    }
    let certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
    let checkpoint = tempfile::tempdir().unwrap();
    for (stream, topic, count, partitions) in [
        (0, 1, 1, vec![0]),
        (1, 0, 1, vec![0]),
        (1, 1, 0, vec![]),
        (1, 1, 1, vec![]),
        (1, 1, 1, vec![1]),
        (1, 1, 2, vec![0, 0]),
    ] {
        let config = DurableConfig::broker_accepted(
            "127.0.0.1:1".parse().unwrap(),
            DurableTlsConfig::new(
                "localhost".to_owned(),
                vec![certificate.serialize_der().unwrap()],
            ),
            "official-test".to_owned(),
            b"[system.message_deduplication]\nenabled = false\n".to_vec(),
            DurableRoutingConfig::new(1, count),
            alopex_chirps::DurableDevelopmentResourceConfig::new(stream, topic, partitions),
            DurableCheckpointConfig::new(
                checkpoint.path().to_path_buf(),
                1,
                DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
            ),
            1024 * 1024,
        );
        assert_eq!(
            DurableBuilder::new(NodeId::new())
                .connect(config, &NoCredentials, tokio::time::Instant::now())
                .await
                .err()
                .unwrap(),
            DurableBuildError::BackendConfiguration
        );
    }
}

#[test]
fn strong_session_still_requires_exact_capability_and_strict_renewal_bounds() {
    use alopex_chirps_backend_iggy::runtime::{
        SessionConnectConfig, SessionConnectionInput, SessionProfileInput, SessionProjectionInput,
    };
    let certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
    for (renew_millis, lease_millis, accepted) in [
        (0, 30_000, false),
        (1, 30_000, true),
        (29_999, 30_000, true),
        (30_000, 30_000, false),
        (30_001, 30_000, false),
        (1, 0, false),
    ] {
        let input = SessionConnectionInput {
            address: "127.0.0.1:1".parse().unwrap(),
            tls_server_name: "localhost".to_owned(),
            trusted_roots_der: vec![certificate.serialize_der().unwrap()],
            max_frame_len: 1024 * 1024,
            credential: None,
            profile: SessionProfileInput::OsSyncedAccepted,
            projection: SessionProjectionInput {
                build_sha: [1; 20],
                resource_id: *NodeId::new().as_bytes(),
                resource_epoch: 1,
                stream_id: 1,
                topic_id: 1,
                partition_id: 0,
                retention_bytes: 1_000_000,
                retention_messages: 1000,
                checksum_enabled: true,
                configuration_digest: [2; 32],
                security_digest: [3; 32],
                capability_digest: [4; 32],
                lease_millis,
            },
            renew_interval: std::time::Duration::from_millis(renew_millis),
        };
        assert_eq!(
            SessionConnectConfig::from_neutral(input).is_ok(),
            accepted,
            "renew={renew_millis}, lease={lease_millis}"
        );
    }
}

#[tokio::test]
async fn configured_profiles_validate_each_local_boundary_before_credentials() {
    struct RejectCredentials;
    #[async_trait::async_trait]
    impl DurableCredentialProvider for RejectCredentials {
        async fn resolve(
            &self,
            _: &str,
        ) -> Result<DurableCredential, DurableCredentialProviderError> {
            Err(DurableCredentialProviderError::Rejected)
        }
    }
    let certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
    let root = tempfile::tempdir().unwrap();
    for strong in [false, true] {
        for case in [
            "valid",
            "frame_minimum",
            "frame_below",
            "checkpoint_generation",
            "journal_small",
            "journal_large",
            "empty_root",
            "empty_credential",
            "empty_server",
            "empty_roots",
            "generation_mismatch",
            "partition_mismatch",
            "matching_builder",
        ] {
            let frame = match case {
                "frame_minimum" => 8,
                "frame_below" => 7,
                _ => 1024 * 1024,
            };
            let tls = DurableTlsConfig::new(
                if case == "empty_server" {
                    String::new()
                } else {
                    "localhost".to_owned()
                },
                if case == "empty_roots" {
                    Vec::new()
                } else {
                    vec![certificate.serialize_der().unwrap()]
                },
            );
            let checkpoint = DurableCheckpointConfig::new(
                if case == "empty_root" {
                    std::path::PathBuf::new()
                } else {
                    root.path().to_path_buf()
                },
                u64::from(case != "checkpoint_generation"),
                match case {
                    "journal_small" => DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES - 1,
                    "journal_large" => DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES + 1,
                    _ => DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
                },
            );
            let credential = if case == "empty_credential" {
                String::new()
            } else {
                "test".to_owned()
            };
            let config = if strong {
                DurableConfig::new(
                    "127.0.0.1:1".parse().unwrap(),
                    tls,
                    credential,
                    DurableProfile::OsSyncedAccepted,
                    DurableRoutingConfig::new(1, 1),
                    DurableResourceConfig::new(
                        1,
                        2,
                        vec![DurablePartitionProjection::new(
                            0,
                            *NodeId::new().as_bytes(),
                            4,
                            [5; 20],
                            6,
                            7,
                            true,
                            [8; 32],
                            [9; 32],
                            [10; 32],
                        )],
                    ),
                    checkpoint,
                    DurableLeaseConfig::new(30_000, tokio::time::Duration::from_secs(10)),
                    DurableExtensionConfig::required(frame),
                )
            } else {
                DurableConfig::broker_accepted(
                    "127.0.0.1:1".parse().unwrap(),
                    tls,
                    credential,
                    b"[system.message_deduplication]\nenabled = false\n".to_vec(),
                    DurableRoutingConfig::new(1, 1),
                    alopex_chirps::DurableDevelopmentResourceConfig::new(1, 2, vec![0]),
                    checkpoint,
                    frame,
                )
            };
            let mut builder = DurableBuilder::new(NodeId::new());
            match case {
                "generation_mismatch" => builder = builder.inbox_generation(2),
                "partition_mismatch" => builder = builder.explicit_partitions(2),
                "matching_builder" => builder = builder.inbox_generation(1).explicit_partitions(1),
                _ => {}
            }
            let error = builder
                .connect(config, &RejectCredentials, tokio::time::Instant::now())
                .await
                .err()
                .unwrap();
            let expected = if matches!(case, "valid" | "frame_minimum" | "matching_builder") {
                DurableBuildError::CredentialRejected
            } else {
                DurableBuildError::BackendConfiguration
            };
            assert_eq!(error, expected, "strong={strong}, case={case}");
        }
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn official_partition_validation_matches_exact_set_model(
        stream in 0_u32..3, topic in 0_u32..3, count in 0_u32..5,
        partitions in proptest::collection::vec(0_u32..6, 0..7),
    ) {
        struct Credentials;
        #[async_trait::async_trait]
        impl DurableCredentialProvider for Credentials {
            async fn resolve(&self, _: &str) -> Result<DurableCredential, DurableCredentialProviderError> {
                Err(DurableCredentialProviderError::Rejected)
            }
        }
        let observed = partitions.iter().copied().collect::<std::collections::BTreeSet<_>>();
        let expected = (0..count).collect::<std::collections::BTreeSet<_>>();
        let valid = stream != 0 && topic != 0 && count != 0 && partitions.len() == count as usize && observed == expected;
        let certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
        let root = tempfile::tempdir().unwrap();
        let config = DurableConfig::broker_accepted(
            "127.0.0.1:1".parse().unwrap(),
            DurableTlsConfig::new("localhost".to_owned(), vec![certificate.serialize_der().unwrap()]),
            "test".to_owned(), b"[system.message_deduplication]\nenabled = false\n".to_vec(),
            DurableRoutingConfig::new(1, count), alopex_chirps::DurableDevelopmentResourceConfig::new(stream, topic, partitions),
            DurableCheckpointConfig::new(root.path().to_path_buf(), 1, DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES), 8,
        );
        let error = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            DurableBuilder::new(NodeId::new()).connect(config, &Credentials, tokio::time::Instant::now()).await.err().unwrap()
        });
        proptest::prop_assert_eq!(error, if valid { DurableBuildError::CredentialRejected } else { DurableBuildError::BackendConfiguration });
    }
}
