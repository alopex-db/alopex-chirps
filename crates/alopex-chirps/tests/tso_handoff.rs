#![cfg(feature = "tso")]

use alopex_chirps::tso::{HybridTimestamp, TsoCommand, TsoResponse, TsoStateMachine};
use alopex_chirps_raft_storage::traits::StateMachine;
use alopex_chirps_raft_storage::types::{CommittedLeaderId, LogId};

async fn apply(
    state_machine: &mut TsoStateMachine,
    index: u64,
    command: TsoCommand,
) -> TsoResponse {
    let response = state_machine
        .apply(
            LogId::new(CommittedLeaderId::new(1, 1), index),
            bincode::serialize(&command).unwrap(),
        )
        .await
        .unwrap();
    bincode::deserialize(&response).unwrap()
}

#[tokio::test]
async fn new_leader_waits_for_previous_lease_before_issuing() {
    let mut state_machine = TsoStateMachine::default();
    assert_eq!(
        apply(
            &mut state_machine,
            1,
            TsoCommand::AcquireLease {
                leader_id: 1,
                now_ms: 100,
                lease_duration_ms: 50,
            },
        )
        .await,
        TsoResponse::LeaseAcquired { expires_at_ms: 150 }
    );
    let first = apply(
        &mut state_machine,
        2,
        TsoCommand::Allocate {
            leader_id: 1,
            lease_now_ms: 100,
            physical_ms: 100,
            count: 1,
        },
    )
    .await;
    assert!(matches!(first, TsoResponse::Allocated(_)));

    assert_eq!(
        apply(
            &mut state_machine,
            3,
            TsoCommand::AcquireLease {
                leader_id: 2,
                now_ms: 149,
                lease_duration_ms: 50,
            },
        )
        .await,
        TsoResponse::LeasePending { not_before_ms: 150 }
    );
    assert_eq!(
        apply(
            &mut state_machine,
            4,
            TsoCommand::Allocate {
                leader_id: 2,
                lease_now_ms: 149,
                physical_ms: 149,
                count: 1,
            },
        )
        .await,
        TsoResponse::LeasePending { not_before_ms: 150 }
    );

    assert_eq!(
        apply(
            &mut state_machine,
            5,
            TsoCommand::AcquireLease {
                leader_id: 2,
                now_ms: 150,
                lease_duration_ms: 50,
            },
        )
        .await,
        TsoResponse::LeaseAcquired { expires_at_ms: 200 }
    );
    let second = apply(
        &mut state_machine,
        6,
        TsoCommand::Allocate {
            leader_id: 2,
            lease_now_ms: 150,
            physical_ms: 150,
            count: 1,
        },
    )
    .await;
    let TsoResponse::Allocated(range) = second else {
        panic!("new leader must allocate after the old lease expires")
    };
    assert!(range.start > HybridTimestamp::new(100, 0));
}
