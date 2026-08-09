# Pre-serialized Raft envelope measurement (2026-08-08)

## Change under test

The transport now serializes each outgoing Raft frame once. The same byte
length is passed to the retransmission buffer, and the already serialized body
is reused by `FrameEnvelopeV2::encode_with_payload` for the QUIC write. This
removes redundant bincode traversals without changing the wire format.

## Results

- Output: `/tmp/chirps-preserial-batch32`
- Summary SHA256:
  `71ea8740b29f1806321ed462f2078fc6835c92dde8f8f0f30736e91129203ea9`
- 10 workload summaries, 0 errors, 0 timeouts
- Multi-Raft exact median: **722.8833 committed/s**
- Single-Group exact median: **212.9833 committed/s**
- Multi-Raft CPU median: **341.37 s**
- Single-Group CPU median: **94.46 s**
- Multi-Raft RSS median: **146,165,760 bytes**
- Single-Group RSS median: **13,152,256 bytes**

The preceding stream-batch run measured 722.9167 / 214.0000 committed/s and
342.19 / 94.35 CPU seconds. The serialization reuse therefore did not produce
a measurable throughput gain in this environment; the Multi-Raft CPU change is
about -0.24%, within run-to-run variation. Redundant serialization is not the
dominant bottleneck at this workload.

## Direct stream-count evidence

In Multi-Raft sample0, the new detachable counter recorded:

| Node | `transport_sent` envelopes | `transport_streams_opened` |
|---|---:|---:|
| 1 | 181,508 | 5,673 |
| 2 | 183,948 | 5,749 |
| 3 | 183,886 | 5,747 |

Stream batching reduced stream opens by approximately **96.9%** while keeping
the workload error-free. This confirms that the stream-setup candidate was real,
but the small throughput change shows that another component now dominates.

## Gate limitation

The script's final verifier rejected the artifact only because shaped RTT p95
exceeded the strict `1.0 ± 0.2 ms` environment gate. The workload summaries,
integrity checks, and error/timeout checks completed; no release artifact is
claimed from this run.
