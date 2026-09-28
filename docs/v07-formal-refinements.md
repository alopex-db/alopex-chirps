# v0.7 formal refinement references

The release candidate uses colocated Rust tests instead of the originally
planned integration-test filenames. The subscription catalog now points at
the implemented files:

| Original planned location | Implemented location and regression evidence |
| --- | --- |
| backend `tests/checked_poll.rs` | `src/poll.rs`: exact record/tail, epoch mismatch, and replay truth-table tests |
| backend `tests/routing.rs` | `src/routing.rs`: explicit partition mapping and typed misroute tests |
| backend `tests/subscriber.rs` | `src/subscriber.rs`: identity-before-delivery and known-old/unknown acknowledgement tests |
| core `tests/durable_contract.rs` | `src/durable/subscription.rs`: different-handle checkpoint rejection and terminal outcome tests |
| backend `src/state/checkpoint.rs`, `tests/checkpoint.rs` | `src/state/journal.rs`: checkpoint/identity share one synced frame; stale owner, digest, and attempt cannot advance a checkpoint |
| backend `src/retention.rs` | `src/poll.rs`: epoch mismatch and replay-gap classification; the implementation task is 3.8 |
| backend `tests/codec.rs` | `src/codec.rs`: corruption, substitution, truncation, and identity-conflict tests |
| Iggy `core/server/src/chirps_extension/checked_poll.rs` | `core/server/src/chirps_extension/poll.rs`: checked-poll dispatch, shard request and response handling |

These changes repair coordinates only. They do not change TLA+ operators,
constants, bounds, invariants, RED profiles, or witnesses, and do not establish
that the listed runtime tests have passed for a new candidate.

The development Compose catalog gate still permits planned locations.
Before accepting final release evidence, run the additional strict gate:

```sh
python3 scripts/release/v07_formal_refinements.py \
  --chirps-root "$CHIRPS_SOURCE_ROOT" --chirps-commit "$CHIRPS_COMMIT" \
  --iggy-root "$IGGY_SOURCE_ROOT" --iggy-commit "$IGGY_COMMIT" \
  --output "$EVIDENCE_DIR/formal-refinements.json"
```

Both revisions must be full immutable commit IDs. The gate reads Git objects,
rejects missing production or test paths, symlinks and submodules, and records
catalog hashes and the complete reference list. It never trusts working-tree
files or silently drops a planned reference. This verifies reference integrity;
the model logs and runtime test evidence remain separate requirements.

The historical August model evidence applies only to its recorded input hashes.
The updated catalog and Compose hashes require fresh candidate-bound evidence.
