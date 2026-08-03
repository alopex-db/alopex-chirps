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
9. A fresh receiver incrementally hashes only chunks whose checksum and
   resumable checkpoint have succeeded, buffering out-of-order chunks until
   their predecessors arrive. Resume or incomplete incremental state falls
   back to hashing the completed temporary file before atomic placement.

## Verification

The integration suite covers final placement before return, stream-error and
checksum-NACK retries, standalone Pull and remote-newer Bidirectional sync,
configuration defaults and transfer limits, remove/symlink option propagation,
metadata preservation, and multiple service metric exposition. The 100 MB/s
goal is verified only by the controlled two-container `ft-1g-v1` profile
defined in `v0_5_2_two_node_performance.md`; normal CI and physical-LAN
diagnostics do not define that product-performance result.

## Controlled product-performance transport profile

Quinn 0.10.6 documents its default transport configuration as
being tuned for a 100 Mbps, 100 ms path and defines a 1.25 MB per-stream
receive window.  That default is not the transport contract of this release
profile. For `ft-1g-v1`, both client and server use
16 MiB per-stream and 64 MiB connection flow-control/send windows, with 256
incoming uni-streams.  This bounds the benchmark's advertised receive memory
while providing at least 1 Gbps bandwidth-delay product capacity.

The direct two-process opener coalesces concurrent first chunk requests into
one QUIC connection per peer. This removes duplicate handshakes from a transfer
that starts multiple chunk tasks while keeping every chunk on a separate
unidirectional stream. The profile keeps the public default of four concurrent
chunks; raising it to the 16-stream ceiling reduced local QUIC throughput during
prior diagnosis. The performance assertion remains
end-to-end: source metadata/hash creation, chunk transfer, receiver
verification, metadata application, atomic placement, and `Complete`
acknowledgement are all within the measured interval.

The current ignored fixture places both QUIC endpoints in the same test
process. It remains useful for local component diagnosis, but is not
`ft-1g-v1` evidence and cannot satisfy the 100 MB/s product SLO. The release
contract remains blocked until the controlled two-container harness records
the profile/image/source/integrity evidence. Deterministic and multi-host fault
coverage is tracked separately from performance.

On the receive path the temporary destination is preallocated once when the
manifest is accepted.  On Linux, each independent chunk then uses a completed
positional `write_at` operation instead of repeatedly opening, inspecting,
resizing, seeking, writing, and flushing the same temporary file.  Tokio's
`File` documentation requires `flush` before a dropped async file can be read
immediately; the Unix positional write returns only after the kernel has
accepted the bytes, so final verification still occurs after every chunk write
has completed.  Non-Unix builds retain the async seek/write/flush path after
the shared preallocation.
