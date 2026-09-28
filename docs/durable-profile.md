# Durable messaging profile (v0.7)

This document defines the public contract of the opt-in Chirps Durable plane. It covers point-to-point messaging only. The existing QUIC mesh remains a separate control plane and keeps its existing API and behavior.

> Release status: this page describes the implemented v0.7 contract in the release branch. The contract is not a published release until the v0.7.0 tag and its attested artifacts are available.

## Contract at a glance

- Enable the umbrella crate's `durable-iggy` feature and construct a separate `DurableHandle` with `DurableBuilder`.
- Choose one confirmation boundary for every send. Only `OsSyncedAccepted` carries an exact receipt.
- Create each subscription with an explicit retained start position and keep its checkpoint directory under one local owner.
- Treat application effects and checkpoint acknowledgement as two operations. A crash between them can redeliver the message.
- Replay is bounded by broker retention, resource and inbox generations, local recovery state, and capacity. A retention loss is an explicit gap, never a normal tail.
- Chirps does not retry sends after process failure and does not own the application's transactional outbox.

Bounded metrics, health, and event correlation are diagnostic only; see the [Durable observability guide](durable-observability.md).

## Guarantee boundaries

The word “Durable” does not collapse send, creation, delivery, and checkpoint into one success condition. Callers must handle each boundary separately.

### Send attempt

`DurableHandle::prepare` creates an immutable envelope and a Chirps-generated UUIDv4 message ID without contacting the backend. One call to `send` starts one attempt and invokes at most one append command.

| `DurableSendOutcome` | What Chirps knows | Caller action |
| --- | --- | --- |
| `NotSubmitted(reason)` | Append invocation did not begin. This attempt cannot later append. | The caller may explicitly retry the same prepared request. |
| `BrokerAccepted` | The pinned official broker returned its ordinary success response. No exact offset/index or restart-presence claim exists. | Use for development interoperability only. |
| `OsSyncedAccepted` | The compatible server verified that the assigned message and index reached its OS-sync boundary. The result contains an exact `DurableReceipt`. | Persist or correlate the receipt as required by the application. |
| `Indeterminate(reason)` | Append invocation began, but the terminal response was not established. Zero or one append may have happened. | Reconcile or explicitly retry the same prepared request; a duplicate is possible. |

An explicit retry reuses the prepared message ID, route, payload, and canonical envelope digest. It creates a new attempt ID. Chirps never performs a hidden second append or automatic sender recovery.

`OsSyncedAccepted` is not a device power-loss, replication, delivery, or application-transaction guarantee. `BrokerAccepted` and `Indeterminate` do not guarantee restart presence or delivery liveness.

### Subscription creation

Every new subscription selects exactly one `InitialPosition`:

- `EarliestRetained` resolves once to the oldest retained offset;
- `LatestAfterCapturedEnd` resolves once to the captured end-exclusive offset;
- `Exact(offset)` accepts an offset inside the same atomic oldest/end observation.

The resolved start (`E0`) is persisted and is not recalculated after restart.

| `SubscriptionCreationOutcome` | Meaning | Allowed continuation |
| --- | --- | --- |
| `CreationNotCommitted(reason)` | No creation candidate could have become durable. | A fresh creation may be attempted. |
| `CreationUnknown(binding)` | Candidate reachability is unknown. | Only recovery of the same subscription and checkpoint directory with the exact binding is allowed. |
| `Created(binding)` | The immutable manifest and current local owner epoch are durably installed. | Polling may begin. |

The checkpoint directory uses a cross-process exclusive lock. Reopen/recovery installs a new durable owner epoch; stale owners and their delivery handles cannot advance the checkpoint. Chirps does not provide cross-host ownership transfer or rebalance.

### Delivery

One checked poll returns at most one record for one explicit partition. Chirps validates the resource epoch, expected offset, retained range, route, message ID, payload digest, and canonical envelope digest before delivery. It persists the processed identity before exposing the application handle.

Each partition has at most one in-flight delivery. Ordering is broker offset order only within the exact `(target, inbox generation, partition)` binding. `release`, timeout, or shutdown fencing does not advance the checkpoint. A later delivery uses a new attempt and permanently fences the old handle.

Crash/restart at-least-once delivery is conditional. It applies only to selected `OsSyncedAccepted` records at or after `E0` while all of the following remain true:

- the message has not expired from broker retention;
- the subscription, broker, and checkpoint state can be recovered;
- the resource and inbox generations still match;
- checked polling/redelivery is available and fair;
- no corruption or retention gap is present;
- local capacity remains available.

### Checkpoint acknowledgement

`ack` applies only to the exact current delivery handle.

| `CheckpointOutcome` | Meaning | Caller action |
| --- | --- | --- |
| `CheckpointCommitted` | The canonical checkpoint record reached the required local sync boundary. The next offset may be polled. | The handle is terminal. |
| `CheckpointNotCommitted` | A known-old failure happened before candidate reachability became ambiguous. | The same handle may retry `ack`. |
| `CheckpointUnknown` | Checkpoint reachability is unknown. The partition enters recovery-required state. | Reopen/recover; do not infer success from the original result. |

Recovery selects a complete old or new checkpoint. Old means redelivery; new means the next offset. Recovery never rewrites the original `CheckpointUnknown` result into a success.

The application owns its side effect. If the application commits an effect and crashes before `CheckpointCommitted`, Chirps may redeliver the same logical message. Applications that need an atomic business effect must make their effect idempotent or coordinate it with an application-owned inbox/outbox; Chirps does not provide that distributed transaction.

## Replay and retention

With committed checkpoint `S`, the next expected offset is checked `S + 1`. Without a checkpoint, it is the persisted `E0`.

- `expected == end` with an empty observation is normal tail.
- `expected < oldest_available` is an explicit retention gap.
- `oldest_available <= expected < end` requires exactly the record at `expected`.
- A missing, duplicated, mismatched, corrupt, or out-of-order record fails closed; Chirps does not skip it.

Therefore “replay from any point” is not a v0.7 guarantee. Replay starts only at a position still covered by retention. Purge, reset, delete/recreate, and partition replacement change the resource identity and invalidate the old namespace; ordinary retention eviction advances `oldest_available` without changing the resource identity.

## Identity horizon

The local identity index detects duplicate logical messages only while their identity records remain inside the horizon. An identity is eligible for crash-safe collection only when all three facts are true:

1. its delivery is checkpointed;
2. its original offset is older than the broker's current oldest retained offset;
3. the caller-supplied `retry_not_before_unix_ms` has passed under a trusted durable-clock reading.

`DurableHandle::next_delivery` receives both the retry-not-before timestamp and `DurableDeliveryClock` provenance. A rollback-detected or unknown clock prevents collection; compaction retains the identity and closes polling rather than silently weakening duplicate detection. Capacity exhaustion likewise stops new admission/polling instead of evicting live recovery state.

After an identity has legitimately crossed all three horizons and has been collected, a delayed retry with the same message ID may be treated as first-seen. Post-horizon deduplication is not guaranteed. Within the horizon, the same message ID and envelope digest is recorded as another attempt; the same ID with a different envelope digest is corruption and fails closed.

## Supported matrix

The matrix is deliberately narrow. Both profiles require TCP/TLS server authentication, explicit credentials, a pre-provisioned stream/topic, numeric partitions, deterministic routing, and message deduplication explicitly disabled. Neither profile falls back to the QUIC plane.

| Dimension | Official development profile | Compatible production/strong profile |
| --- | --- | --- |
| Public selector | `DurableProfile::broker_accepted(actual_startup_config)` | `DurableProfile::OsSyncedAccepted` |
| Server source | Apache Iggy `server-0.8.0`, baseline commit `f5350d999d883fd3ca9dd33b3dc2754ddb0df049` | Chirps-compatible source commit `336d20c53b4bba663c257bdc0271373cfc2f1864`, tree `b2099c2dc404534429e210069990a10496d4fefd`, based on the same Iggy commit |
| Published/attested artifact | Chirps v0.7 does not declare an official development image digest; the exact startup configuration is supplied and hashed at connect time | Candidate server binary SHA-256 `3840ead85e35a20c0c86b519c15b87edf2fe08a9dc866b4746915ed8cb312f88`; runtime base image `debian@sha256:38a76d01668772e381ad2826d876627c89e7133e2f8a0f5d567306798b0f2a16` |
| Transport/protocol | Adapter-owned TCP/TLS; official Iggy protocol/model crates `0.10.0`; standard login/resource readback/send | Adapter-owned TCP/TLS; official outer framing `0.10.0` plus private extension v1 |
| Private extension | Not used | Schema SHA-256 `71e462395675017a7fd5be74d2f71b0a2488da882aca52d27bd1916d93c3d6a2`; `CapabilityBind`, `LeaseRenew`, `AppendOneSynced`, `CheckedPoll` |
| Topology | One broker resource, replication factor `1`, one configured connection per explicit partition | One compatible broker resource, replication factor `1`, one bound/leased connection per explicit partition |
| Required configuration | Actual config readback with `[system.message_deduplication] enabled = false`; matching topic and explicit partition | Production profile; root/admin/runtime credentials provisioned before startup; dedup disabled; partition and state fsync enabled; checksum validation enabled; `messages_required_to_save=1`; exact build/resource/config/security/capability projections; lease renewal shorter than lease duration |
| Guarantee | Point-to-point `BrokerAccepted` development interoperability | Full strong send receipt, checked delivery, local checkpoint/recovery, bounded local state, and conditional retention-scoped redelivery described above |
| Not guaranteed | Exact location, OS sync, restart presence, delivery, subscription recovery, replication | Device power-loss durability, replication/quorum/HA/failover, exactly-once effects, unlimited replay, cross-host rebalance |

The failpoint-enabled server is a distinct `publish-disabled-test` artifact. It is evidence tooling, not a deployable profile.

## Point-to-point flow

The following code uses only implemented public methods. Construction is omitted because deployments must supply their own attested projection, TLS roots, credential provider, lease, capacity, and checkpoint path. The umbrella crate requires the `durable-iggy` feature; provider-neutral result types come from `alopex-chirps-core`.

```rust,no_run
use alopex_chirps::{DurableDeliveryClock, DurableHandle, DurablePoll, NodeId};
use alopex_chirps_core::durable::{
    CheckpointOutcome, ConfirmationBoundary, DurableSendOutcome, InitialPosition,
    SubscriptionCreationOutcome, SubscriptionId,
};

async fn send_one(sender: &mut DurableHandle, target: NodeId) {
    let prepared = sender
        .prepare(target, b"account-42".to_vec(), b"created")
        .expect("prepare immutable point-to-point envelope");
    let result = sender
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await
        .expect("send attempt produced a typed result");

    match result.outcome() {
        DurableSendOutcome::OsSyncedAccepted => {
            let receipt = result.receipt().expect("strong outcome has a receipt");
            println!("OS-synced offset={}", receipt.assigned_offset());
        }
        DurableSendOutcome::NotSubmitted(_) => {
            // An explicit retry may reuse `prepared`.
        }
        DurableSendOutcome::Indeterminate(_) => {
            // Reconcile or retry `prepared`; a duplicate is possible.
        }
        DurableSendOutcome::BrokerAccepted => unreachable!("strong boundary requested"),
    }
}

async fn receive_one(
    receiver: &mut DurableHandle,
    subscription_id: SubscriptionId,
    local_target: NodeId,
    partition: u32,
) {
    let created = receiver
        .create_subscription(
            subscription_id,
            local_target,
            partition,
            [0x42; 32],
            InitialPosition::EarliestRetained,
        )
        .await
        .expect("typed creation operation");
    assert!(matches!(created, SubscriptionCreationOutcome::Created(_)));

    match receiver
        .next_delivery(
            subscription_id,
            // Absolute end of the application's producer-retry window.
            1_900_000_000_000,
            DurableDeliveryClock::Trusted,
        )
        .await
        .expect("checked poll")
    {
        DurablePoll::Delivery(mut delivery) => {
            apply_idempotently(delivery.canonical_bytes()).await;
            match receiver
                .ack(subscription_id, delivery.handle_mut())
                .expect("typed checkpoint operation")
            {
                CheckpointOutcome::CheckpointCommitted => {}
                CheckpointOutcome::CheckpointNotCommitted => {
                    // Retry ack on this same handle.
                }
                CheckpointOutcome::CheckpointUnknown => {
                    // Stop and recover this subscription.
                }
            }
        }
        DurablePoll::Tail | DurablePoll::IdentityNotCommitted => {}
    }
}

async fn apply_idempotently(_canonical_envelope: &[u8]) {}
```

Production code must also handle `CreationNotCommitted` and `CreationUnknown`; the latter permits only `recover_subscription` with the exact returned binding and the same checkpoint directory.

## Explicit non-goals

Chirps v0.7 Durable does not promise:

- exactly-once broker append, delivery, or application effect;
- an atomic transaction between an application database and a Chirps checkpoint;
- unlimited replay or deduplication after the identity horizon;
- order between targets, partitions, subscriptions, or a global total order;
- sender outbox ownership, process-crash resend, or automatic retry/reconciliation;
- replication, quorum, high availability, broker failover, or cross-host subscription rebalance;
- mutual TLS, device-cache flush, or power-loss durability;
- an atomic message/data snapshot, backup, or restore API;
- Durable broadcast.

Applications own business-effect idempotency, outbox/inbox policy, explicit retry decisions, stable checkpoint storage, capacity sizing, credential provisioning, and terminal `DurableHandle::shutdown`.
