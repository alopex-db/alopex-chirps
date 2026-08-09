# v0.6 Multi-Raft response/snapshot isolation evidence

Date: 2026-08-08  
Source revision: `7b926eeb3a1fb5e8f498a3da0d65786ac505b6bd`  
Execution class: one WSL/Linux host, three Docker logical nodes; no physical
three-node deployment was available.

## Result

The release gate is **not passed**. The fixed-order run completed all five
Multi-Raft and five single-group measurement samples, but the verifier rejected
the artifact because host swap grew during the run. Independently of that host
condition, Multi-Raft samples 2 and 4 had 1,188 timeouts and post-drain
replica divergence, so the zero-timeout/integrity gate is also not met.

Multi-Raft throughput values were 866.88, 860.40, 656.42, 795.48, and 627.12
committed proposals/s (median 795.48/s). The corresponding single-group values
were 92.68, 93.73, 89.87, 98.85, and 93.70/s (median 93.70/s). These are
engineering measurements only and are far below the 100,000/s release target.

## Root-cause evidence and follow-up

With `snapshot_threshold=512`, the earlier classified run produced only server
errors of `Read Snapshot(None): snapshot not found`. The benchmark lane now uses
`snapshot_threshold=10,000` to keep automatic snapshot work outside the 60 s
proposal window; storage snapshot lifecycle remains covered by its own tests.
In the 10-sample run after this isolation, `server_errors=0` and
`server_error_reasons={}` for every sample.

The remaining timeout samples had non-zero response-send drops/failures. Code
inspection found a double acquisition of the response-send semaphore (the pump
acquired a permit and `dispatch_raft_frame` tried to acquire it again). Commit
`7b926ee` passes the already acquired permit through the dispatch path and waits
for a permit for request-generated responses instead of dropping them.

A post-fix targeted Multi-Raft sample is preserved under
`post-fix-single-sample/`: 46,683 commits, 778.05/s, zero errors/timeouts,
zero server/transport errors, zero response-send drops/failures, and peak node
RSS 151,498,752 bytes. This validates the fix locally but is not a release
gate; the full post-fix five-sample run is still required in a host with swap
growth disabled and must be re-verified for integrity.

