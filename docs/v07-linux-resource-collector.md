# Linux resource observations for durable performance

`scripts/perf/chirps-durable-audit.py` implements the v2 probe protocol for a
native Linux client and production server on one host. It collects resource
measurements only. The Rust driver verifies actual broker readback, logical
identities, payload digests, admission queue, confirmed-message lag, errors
and timeouts. Neither component substitutes constants for unavailable data.

The driver invokes:

```sh
scripts/perf/chirps-durable-audit.py \
  --config /absolute/collector.json --config-sha256 SHA256 \
  --chirps-audit-request-json REQUEST_JSON
```

The request schema is `chirps.durable-perf-audit-request/v2`. Besides the
observation identity, phase, arm, sample, control and payload digest, it
contains the actual client PID and canonical workload checkpoint root. The
response schema is `chirps.durable-perf-audit-response/v2`. Begin returns
observed axes with no metrics. Finish returns `peak_rss_bytes`,
`disk_growth_bytes` and `hard_resource_limit_exceeded`.

## Configuration binding

Configuration uses schema `chirps.durable-perf-audit-config/v1` and rejects
unknown or omitted fields. Paths must be absolute and may not traverse
symlinks. File references below contain `path` and `sha256`.

| Field | Required observation or bound |
|---|---|
| `state_root` | Existing operator-owned directory with mode 0700, separate from measured roots |
| `server` | `pid`, `start_time_ticks`, `executable_sha256`, `manifest` and `config` file references, `command_sha256`, `environment_sha256`, and `data_root` |
| `client_executable_sha256` | Actual `/proc/PID/exe` digest for the driver |
| `checkpoint_root` | Exact root provided by the running driver |
| `payload`, `partition_set` | Pinned file references |
| `image` | Stored OCI archive file reference plus `manifest_digest` |
| `inspector_sha256` | SHA-256 of the shared `scripts/release/oci_artifact.py` source |
| `planned_axes` | Confirmation profile, offered load, warmup/measurement/drain milliseconds, placement, execution class and topology counts |
| `sample_interval_millis` | Sampling period between 1 and 1000 ms |
| `max_observation_millis` | Finite deadline, at most measurement + drain + 120 seconds and at most 200,000 scheduled samples |
| `max_rss_bytes`, `max_disk_growth_bytes` | Positive normal resource bounds fixed before comparison |
| `hard_control_rss_bytes` | Tighter positive RSS bound used only by the hard-resource safety control |

Placement is `same-host`, execution class is `loopback`, and host, broker and
replication counts are each one. The collector rejects other deployment
claims. Planned timing, offered load and confirmation behavior are checked
by the Rust workload and evidence verifier.

Begin and Finish independently inspect process start times and executable
bytes. Server manifest bytes must identify the running production executable.
Its effective bytes inside the stored OCI image must match too, including
layer replacement and whiteout handling. Noncanonical archive paths are
rejected. OCI inspection limits archives to 1 GiB, individual entries to
512 MiB, and entries to 100,000; each layer has the same expanded-byte budget.

The measured server root must equal the running process's explicit
`IGGY_SYSTEM_PATH`, and the pinned configuration file must equal its explicit
absolute `IGGY_CONFIG_PATH`. The server command digest hashes raw `/proc/PID/cmdline`.
The environment digest hashes canonical JSON of its `IGGY_*` environment,
excluding password, token and secret values. It never stores those values.
The configuration axis hashes canonical JSON with `file_sha256`,
`command_sha256` and `environment_sha256` keys. The source axis hashes the
manifest's `source` table as canonical JSON. Canonical JSON sorts keys, uses
compact separators and UTF-8 encoding.

The host fingerprint hashes uname, machine ID and boot ID. Payload and
partition fingerprints come from their actual file bytes. Image identity
comes from the verified OCI manifest descriptor. These axes are observed
from pinned inputs and running processes rather than copied from the
candidate's expected fingerprints.

## Resource measurement and cleanup

A detached sampler records client and server RSS from `/proc` and their
combined observed high-water value. This is sampled RSS, not an exact
continuous maximum or a sum of unique physical pages. Every sample records
its monotonic time and both process values. A gap above five declared sample
periods fails the observation. PID reuse, process exit, or changed collector
configuration also fails it.

Disk observations count allocated file blocks under the dedicated server and
checkpoint roots, deduplicating hard links. The comparison metric is positive
net growth. The resource-limit check also uses the sampled peak growth, so
later deletion or compaction cannot erase an observed limit violation.
Events shorter than a sample interval remain outside this sampled guarantee.

Finish requests cooperative sampler termination and waits for a complete
result. A malformed matching Finish or failed Begin readiness also requests
termination. The sampler's finite deadline bounds an abandoned observation;
it does not signal or stop the measured server. Completed JSON is published
atomically without replacing an existing file. Raw samples and failure
records remain under the observation's unique state directory.

## Verification

`python3 scripts/perf/test-chirps-durable-audit.py` tests configuration/file
binding, PID-stat parsing, allocated disk accounting, hard links, archive
aliases and budgets, atomic result publication and excessive sampling gaps.
On Linux it also starts a dedicated synthetic process, measures actual RSS
and allocated files, detects a transient disk-limit violation after deletion,
and exercises the hard-RSS control and failed-Finish cleanup. The synthetic
process is not Iggy and does not establish production performance.

The first Linux execution reproduced an empty-result-file read race. The
atomic publication regression and the subsequent real Linux run cover its
repair. Mac runs skip the real `/proc` case explicitly. The existing local
HTTP publication fixture checks the shared OCI inspector through the
publisher and read-only publication wrapper.
