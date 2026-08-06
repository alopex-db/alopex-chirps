# Snapshot bounded-check evidence

Date: 2026-08-06  
Tool: Apalache 0.58.3  
Image: `ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795`  
Source base: `30af2ac` plus the `formal/snapshot/` files committed with this evidence

Both commands ran from `formal/` using the canonical root `compose.yml`.

| Command/profile | Bound | Verdict |
| --- | ---: | --- |
| `typecheck snapshot/model.tla` | n/a | PASS (`Your types are purrfect!`) |
| `SnapshotTransfer.cfg` / `Next` | 10 | PASS (`NoError`) |

The verdict covers two chunks, one retry per chunk, and at most two concurrent
in-flight chunks. It establishes only the safety invariants listed in
`catalog.yaml` within that finite domain. It is not evidence of unbounded
liveness or fairness, filesystem durability, OpenRaft behavior, real QUIC
behavior, or the 100 MB/s controlled-profile performance target.
