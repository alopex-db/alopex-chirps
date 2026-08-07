# WAL range-read fix measurement (2026-08-07)

Revision `64f62b9` applies the node-side storage fixes identified by the RSS
audit:

- WAL fallback reads retain only entries inside the requested Raft range;
- replacing an existing log index no longer appends a duplicate cache-order
  entry;
- snapshot installation no longer clones the entire state-machine payload.

This follows TiKV Raft Engine's relevant design boundary: an index resolves a
log location and only the requested record is read, instead of materializing a
whole log. TiKV also exposes explicit limits for in-flight messages and log
cache/GC, rather than relying on an unbounded read path. See the implementation
notes in the [Raft Engine design](https://pingcap.co.jp/blog/raft-engine-a-log-structured-embedded-storage-engine-for-multi-raft-logs-in-tikv/)
and TiKV's [raftstore limits](https://tikv.org/docs/4.0/tasks/configure/raftstore/).

## Result

| metric | before (`b13fba2`) | after (`64f62b9`) |
|---|---:|---:|
| Multi-Raft median | 296.72/s | 408.85/s |
| maximum node RSS | 2.03 GiB | 1.76 GiB |
| phase records | 130/130 | 130/130 |
| node metric coverage | 239–240/window | 239–240/window |

The after run still had timeouts (including a zero-commit sample), so this is
an improvement signal, not a release pass. Dispatch/retransmit/overflow/
backpressure counters remained zero in the retained node summary. All storage
UTs (20), resilience tests (4), and workspace clippy passed before the run.

The compact evidence consists of `sample-summary.tsv`,
`node-measurement-summary.tsv`, `loadgen-phase-summary.tsv`, and the complete
`phase-metrics.ndjson` stream.
