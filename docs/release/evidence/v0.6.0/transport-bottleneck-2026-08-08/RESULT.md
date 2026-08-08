# v0.6.0 transport bottleneck controlled measurement

Date: 2026-08-08 (JST)  
Revision under test: `7c8e6d0` (the measurement image was built from this
revision).  Environment: one WSL2 host, three logical Docker nodes, fixed
CPU sets, loopback networks with 250 us `tc netem`, 1 KiB payload, 100
Multi-Raft groups, 300 load-generator clients, five samples per mode.

The run completed all ten phases, but the runner correctly rejected the
aggregate because one single-group sample had timeouts.

| mode | sample | committed | committed/s | errors | timeouts |
|---|---:|---:|---:|---:|---:|
| Multi-Raft | 0 | 41,389 | 689.8167 | 0 | 0 |
| Multi-Raft | 1 | 40,799 | 679.9833 | 0 | 0 |
| Multi-Raft | 2 | 41,251 | 687.5167 | 0 | 0 |
| Multi-Raft | 3 | 45,212 | 753.5333 | 0 | 0 |
| Multi-Raft | 4 | 43,972 | 732.8667 | 0 | 0 |
| single-group | 0 | 5,593 | 93.2167 | 0 | 0 |
| single-group | 1 | 5,489 | 91.4833 | 0 | 0 |
| single-group | 2 | 5,785 | 96.4167 | 0 | 0 |
| single-group | 3 | 5,529 | 92.1500 | 0 | 0 |
| single-group | 4 | 5,582 | 93.0333 | 0 | 52 |

Multi-Raft median is **687.5167 committed proposals/s**.  All Multi-Raft
samples converged with zero errors and zero timeouts after (a) resolving the
leader before the first proposal and (b) disabling QUIC peer-stop waiting only
for the explicit Multi-Raft fanout lane.  The single-group default continues
to await peer-stop notifications; its final sample demonstrates a tail-latency
saturation failure under this 300-client profile.

This is a valid diagnostic result, not a release pass: it is far below the
historical 100,000/s proposal gate, and the no-timeout predicate is false.
The host has no physical three-node network; Docker/WSL2 and the missing
cgroup swap-limit capability are environmental limitations recorded in the
raw host facts.  No bootstrap lower bound is reported because the sample
validity predicate is false.

Raw evidence (kept outside the repository to avoid generated-artifact growth):

- `/tmp/chirps-final-controlled/summaries.ndjson`
  (`b6eec3abcf772677a375ac9e811ec5e91db335f14a99b16d74ebfdbd0fc7bbce`)
- `/tmp/chirps-final-controlled/samples.json`
  (`d8e094985d1cf334035daeb74d3efd2e6ebb1452f3ca93aec5bd6c21c9759ad1`)
- `/tmp/chirps-final-controlled/host-facts.txt`
  (`e65d8edabcd5995e1b85a3b962bbdd605592c4208a374b769e06fe6e4e006c3b`)

## Decision

`RELEASE BLOCKED`.  The transport bottleneck fix is covered by unit tests and
the Multi-Raft lane is stable in this run, but the performance and no-timeout
release gates are not met.  No tag, push, or GitHub Release was created.
