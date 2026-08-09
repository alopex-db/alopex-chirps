# TiKV control-baseline status (2026-08-08)

## Verdict

**MEASURED AS A LOCAL DOCKER CONTROL; NOT A PUBLISHED-TiKV REPRODUCTION.**
The official TiKV service was run as PD + three TiKV stores on one Docker host.
The result is usable as a same-host control for diagnosing workload shape, but
it is not comparable to the published 40-vCPU/NVMe benchmark and does not
change the Chirps release gate.

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
- Docker Engine 29.4.1, Ubuntu 22.04.5 LTS, x86_64, 12 CPUs, 25,200,021,504
  bytes memory exposed to Docker
- TiKV stores were Docker named volumes on the same Docker root filesystem;
  TiKV logged `not on SSD device` for `/data` and `/data/raft`
- TiKV logged `vm.swappiness=60` and could not set raftstore thread priority
  (`PermissionDenied`)

## Images and cluster

- PD v8.5.0: `sha256:5cf66a73894ca652dd640e2ae72bc0bff6211ec55efbe95fbf45f98807e26af0`
- TiKV v8.5.0: `sha256:0524a2070bbfe3fef1331589113344f10050e6b821c2c0a4b79108b4b535f824`
- Go YCSB image: `sha256:1df37763a419449d0185f49ebc283fde1d3f4c75b8e529ba58c3c27c872b8aa7`
- PD store API reported three stores, all `Up`, version `8.5.0`, with no OOM or
  restart on any container.

## Workload A results

The official Go YCSB TiKV binding was used with RawKV (`tikv.type=raw` default),
10,000 records, 10 fields × 100 bytes (approximately 1 KiB values), uniform
keys, 16 threads, and 50% read / 50% update. The initial 10,000-record load
completed at 138.8 INSERT OPS in 72.13 s.

| Client path | Total OPS | READ OPS | UPDATE OPS | READ avg/p99 | UPDATE avg/p99 |
|---|---:|---:|---:|---:|---:|
| Unshaped Docker bridge | 641.8 | 320.4 | 321.4 | 1.56 ms / 4 ms | 46.7 ms / 65 ms |
| Client egress netem 500 µs | 580.6 | 289.2 | 291.4 | 1.69 ms / 4 ms | 51.9 ms / 235 ms |

The shaped client path measured 20 ICMP samples to `tikv-control-1` at
0.589–1.221 ms (average 0.737 ms); qdisc reported zero drops. The measured
throughput fell 9.5% under this path, while update tail latency increased.

The run used 10k/50k rather than the official overview's 10M/30M workload so
it would fit the constrained host and complete as a diagnostic control. It is
therefore not a replacement for the official TiKV result.

These attributes do not satisfy the published TiKV performance configuration.
The local Docker control is now available for relative diagnosis, but any
cross-project numerical claim must still use a separately provisioned
three-node SSD-backed TiKV environment or remain explicitly environment-bound.

Reproduction commands are recorded in [COMMANDS.md](COMMANDS.md).
