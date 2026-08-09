# Local TiKV-calibrated native Multi-Raft result

## 判定

**Pass（この環境の性能基準に対して）**。artifact verifierも `overall=Pass`。

## 条件

- 3論理ノード / 3コンテナ / 1ホスト、100 groups、1KiB payload
- 5 Multi-Raft + 5 single-group samples、各60秒、warmup 15秒、drain 5秒
- 500us netem（shaped RTT p95は全方向・全サンプルで1.0±0.2ms内）
- resource audit有効、durability batch wait 250us
- source commitはartifactの`commit_sha`、image digestはartifactの`binary_sha256`を参照

## 結果

| 指標 | 値 |
|---|---:|
| Multi-Raft median | 711.6167 committed proposals/s |
| Multi-Raft bootstrap CI95 lower | 709.9833/s |
| single-group median | 211.9000/s |
| overhead ratio | -2.3583 |
| errors / timeouts | 0 / 0 |
| peak RSS | 141,950,976 bytes |
| verifier | throughput=Pass, overhead=Pass, integrity=Pass, overall=Pass |

## 基準

この環境のTiKV v8.5.0 Docker control（500us netem、YCSB Workload A）の
UPDATE OPS実測値 **291.4/s**を、READを除いた書込み側の比較基準として使用した。
公開TiKV値やYCSB total 580.6 OPSをnative committed proposals/sの絶対閾値にはしていない。

Multi-Raftの中央値・CI lowerはいずれも291.4/sを上回る。ただしこれは本環境の
calibration passであり、物理3ノード、NVMe、公式TiKV構成での性能保証ではない。

完全なartifactとraw evidenceは同ディレクトリの`multi-raft-performance.json`および
`samples/`以下に保存する。
