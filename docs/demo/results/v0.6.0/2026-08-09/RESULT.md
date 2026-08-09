# Chirps v0.6.0 デモ実行結果

- 実行時刻: `2026-08-09T13:17:54.525082+00:00`
- commit: `b6f52ccd4d7776cc155b47604f51b0ca1d99582c`
- host: `Linux x86_64 6.18.35.2-microsoft-standard-WSL2`
- overall: **PASS**

## シーン結果

| シーン | 結果 | 秒 | 証跡 |
| --- | --- | ---: | --- |
| `workspace-all-features` | PASS | 310.094 | `workspace-all-features.stdout.log` / `workspace-all-features.stderr.log` |
| `wire` | PASS | 8.721 | `wire.stdout.log` / `wire.stderr.log` |
| `core` | PASS | 4.166 | `core.stdout.log` / `core.stderr.log` |
| `raft-storage` | PASS | 26.405 | `raft-storage.stdout.log` / `raft-storage.stderr.log` |
| `gossip-swim` | PASS | 6.893 | `gossip-swim.stdout.log` / `gossip-swim.stderr.log` |
| `transport-quic` | PASS | 44.427 | `transport-quic.stdout.log` / `transport-quic.stderr.log` |
| `quic-ignored` | PASS | 36.021 | `quic-ignored.stdout.log` / `quic-ignored.stderr.log` |
| `raft-cluster` | PASS | 15.841 | `raft-cluster.stdout.log` / `raft-cluster.stderr.log` |
| `deterministic-harness` | PASS | 61.105 | `deterministic-harness.stdout.log` / `deterministic-harness.stderr.log` |
| `perf-harness` | PASS | 38.715 | `perf-harness.stdout.log` / `perf-harness.stderr.log` |
| `three-node-mesh` | PASS | 27.797 | `three-node-mesh.stdout.log` / `three-node-mesh.stderr.log` |
| `multinode-read-write` | PASS | 0.836 | `multinode-read-write.stdout.log` / `multinode-read-write.stderr.log` |
| `multi-raft` | PASS | 0.879 | `multi-raft.stdout.log` / `multi-raft.stderr.log` |
| `tso` | PASS | 0.250 | `tso.stdout.log` / `tso.stderr.log` |
| `snapshot` | PASS | 0.483 | `snapshot.stdout.log` / `snapshot.stderr.log` |
| `hlc-metrics` | PASS | 0.233 | `hlc-metrics.stdout.log` / `hlc-metrics.stderr.log` |
| `file-transfer` | PASS | 1.451 | `file-transfer.stdout.log` / `file-transfer.stderr.log` |

## マルチノード read/write

- group: `600` / leader: `node-1`
- write commit: `6/6`
- replica read consistency: **True**
- key counts: `{"node-1": 6, "node-2": 6, "node-3": 6}`
- scope: three logical nodes in one process using MockNetwork and WAL-backed storage; not physical-node evidence

## 解釈上の注意

このデモは機能・整合性の実行証跡であり、性能SLOの測定ではない。
論理3ノードを1プロセス内のMockNetworkで実行しているため、物理3ノード、
実ネットワーク障害、TiKVのRawKV/YCSB性能を主張しない。
