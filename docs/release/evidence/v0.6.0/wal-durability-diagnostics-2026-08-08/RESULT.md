# WAL durability diagnostics

## 実施条件

- 実行: `scripts/perf/run-controlled-container-multi-raft.sh --output /tmp/chirps-wal-diag-2026-08-08 --resource-audit`
- 3ノード、Multi-Raft 100 groups / single-group 1 group、各5サンプル、各60秒
- network shaping: 500us netem（実測 RTT p50 約0.76--0.87ms）
- 実装: commit `8f9df63`

## 結果

| lane | throughput median | bootstrap CI lower | durability barriers | participant syncs |
|---|---:|---:|---:|---:|
| Multi-Raft | 711.75/s | 675.27/s | 34,810--36,054 | 89,855--95,272 |
| single-group | 211.93/s | — | 13,341--13,937 | 13,341--13,937 |

全10サンプルで `errors=0`、`timeouts=0`。ただし既存の性能ゲートは、Multi-Raft の絶対値/RTT条件を満たさず `Fail` である。これはデータ整合性失敗ではない。

## 診断

Multi-Raft は、durability barrier 1回あたり約2.64--2.68個の participant sync まで削減されており、coordinator の dirty-only batching は機能している。一方、3ノード・複数グループで約9万回の個別WAL `sync_all` が発生している。single-group は barrier と participant sync が1対1である。

したがって、次の改善候補は barrier 集約そのものではなく、ノード内でWALを共有して同一ファイルの同期回数を集約すること。これはTiKVのRaft Engine（ノード内の複数Raftグループを共有append-only logへ書く設計）に対応する。共有WALは互換性維持のためデフォルト無効とし、別レーンでUTと同条件測定を行う。

## 適用範囲

この結果は本リポジトリのDocker/loopback環境における診断値であり、TiKV公式の40 vCPU/64 GiB/500 GiB NVMe構成の性能値や、物理3ノードの代替ではない。
