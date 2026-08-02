# v0.5.2 File Transfer reliability design

## Scope

This document fixes the implementation boundary for Issue #1.  A successful
`send_file`, `broadcast_file`, or `sync_file` result means that the receiver
has verified the complete file, applied requested metadata, and atomically
placed the destination.  A transmitted final chunk acknowledgement alone is
not success.

## Evidence used for the design

- `magic-wormhole.rs` commit `41d78aed662d29729c98ac5715d7e413c139e18e`,
  `src/transfer/v1.rs`: the sender waits for a receiver acknowledgement that
  contains the final SHA-256 and rejects a mismatch.
- `iroh` commit `0438766522229f8b330ac95f45cd61c7159f352a`,
  `iroh/examples/transfer.rs`: the side that completely drains a QUIC receive
  stream calls `SendStream::finish`; the sender waits for that confirmation.
- Tokio `Semaphore` documentation: an `Arc<Semaphore>` with
  `acquire_owned` keeps a fair permit across spawned task boundaries.
- `rust-prometheus` commit `81514180fc47ed387d20140119b97c33495f85fe`:
  collectors are registered in an explicit `Registry`; duplicate registration
  returns `AlreadyReg` rather than disabling metrics.

## Protocol and state invariants

1. The receiver sends `Complete` only after final hash verification, metadata
   application to the temporary file, and atomic rename have succeeded.  The
   sender validates the echoed file hash and waits for this message before it
   marks the session complete or removes a Move source.
2. A failed stream write or a negative `ChunkAck` creates a scheduled retry.
   The sending loop stays alive while any scheduled retry exists; it cannot
   conclude from an empty `pending` and `in_flight` set alone.
3. Pull is an explicit request carrying the transfer session id.  The remote
   service starts the reverse send with that id, so the caller can wait for one
   receiving session without separately calling `send_file`.
4. Global transfer slots are acquired per actual send session.  They are held
   until the final receiver completion acknowledgement, then released.
5. Path symlink policy is propagated with a transfer/sync request.  Resolving
   a symlink is allowed only when its resolved existing ancestor remains under
   the configured base path.
6. Remote remove carries `ignore_not_found`.  Metadata preservation applies
   Unix mode and modified time to the temporary destination before rename.
7. Each `FileTransferServiceImpl` owns a Prometheus `Registry`; construction
   fails on registration error instead of silently returning a service with no
   metrics.
8. Independent QUIC receive streams may write distinct chunk offsets in
   parallel. Session metadata is locked only to snapshot/commit a chunk; the
   task that changes the session to `Verifying` is the only task allowed to
   hash, apply metadata, rename, and emit `Complete`.

## Verification

The integration suite covers final placement before return, stream-error and
checksum-NACK retries, standalone Pull and remote-newer Bidirectional sync,
configuration defaults and transfer limits, remove/symlink option propagation,
metadata preservation, and multiple service metric exposition.  The dedicated
1 Gbps performance runner records the 100 MB/s gate separately from normal CI.
