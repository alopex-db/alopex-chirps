# TiKV control-baseline status (2026-08-08)

## Verdict

**NOT MEASURED / NOT COMPARABLE.** No TiKV process was run on this host, so
there is no same-workload TiKV control result for the v0.6 release decision.

## Reference prerequisites

The TiKV performance overview describes a 3-node RawKV cluster using Go YCSB,
40 vCPUs, 64 GiB RAM, and a 500 GiB NVMe SSD per TiKV node. Its reported
212,000 point-get OPS and 43,200 update OPS are YCSB operations, not committed
Raft proposals. The benchmark instructions also call for a separate YCSB
worker and PD node and recommend local SSDs.

Sources:

- https://tikv.org/docs/dev/deploy/performance/overview/
- https://tikv.org/docs/6.5/deploy/performance/instructions/
- https://docs.pingcap.com/tidb/stable/hardware-and-software-requirements/

## Observed local host

- WSL2/Microsoft hypervisor; one host and one filesystem
- Intel Core i7-1260P, 6 physical cores / 12 logical CPUs
- 23 GiB RAM and 32 GiB swap configured
- Chirps profile: three logical containers, 8 GiB tmpfs per node, synthetic
  netem delay, no independent SSD per logical node
- Docker API access was unavailable to this audit process, so a TiKV image
  inventory or container control run could not be performed

These attributes do not satisfy the published TiKV performance configuration.
Until three independent SSD-backed TiKV nodes (plus PD and a YCSB worker) are
available, the only honest comparison is a contract-level comparison with no
numeric TiKV claim.
