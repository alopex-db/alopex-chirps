# Raft stream batching measurement (2026-08-08)

## Change under test

Ordinary `Frame::Raft` envelopes are appended to a bounded temporary QUIC
unidirectional stream (default 32 envelopes), with a length prefix and explicit
batch marker. The receiver parses each envelope incrementally, so a partial
final batch is applied before drain. Snapshots, control, user, gossip, and file
transfer streams retain the legacy path.

## Controlled run

- 3 logical nodes in 3 containers on one host
- 100 Raft groups, 300 clients, 1 KiB payload
- 1 ms shaped RTT, 60 s measured interval, 5 samples per mode
- resource audit enabled
- output: `/tmp/chirps-raftstreambatch32`
- summaries SHA256:
  `da29c6e785e2d158695dd3fff1215306eaeb169152e6bcebeb1f21512860a58a`

The workload completed all ten summaries with zero errors and zero timeouts.
The script's final verifier rejected the run only because shaped RTT p95 was
outside the strict `1.0 ± 0.2 ms` environment gate; this is an environment
condition, not a Raft/transport correctness failure. No release artifact is
claimed from this run.

## Results

| Mode | Throughput samples (committed/s) | Exact median | CPU median (s) | RSS median | Errors/timeouts |
|---|---|---:|---:|---:|---:|
| Multi-Raft | 734.7000, 711.8000, 728.4667, 722.9167, 722.2000 | **722.9167** | 342.19 | 145,633,280 | 0 / 0 |
| Single-Group | 214.9500, 210.9000, 212.9833, 215.0000, 214.0000 | **214.0000** | 94.35 | 13,107,200 | 0 / 0 |

The prior audited dispatch-batch baseline was 715.5500 Multi-Raft and
217.0167 Single-Group, with CPU medians 342.88 s / 94.50 s and RSS medians
145,911,808 / 13,328,384 bytes. The stream-batch run is +1.03% Multi-Raft and
-1.39% Single-Group, with negligible resource changes; this is an improvement
candidate but not a proven dominant bottleneck removal.

## Causal evidence and next measurement

Before this run, the audit showed about 175k Raft transport sends per node.
The implementation now exposes `transport_streams_opened` separately from
`transport_sent` so the next identical run can directly verify stream
amortization rather than infer it from envelope counts. That counter is
serde-defaulted for old evidence and does not alter workload semantics.

The current result supports correctness and no regression. It does not support
the obsolete 100,000 committed/s target, nor does it justify release by itself.
