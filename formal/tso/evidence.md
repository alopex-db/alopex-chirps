# TSO bounded-check evidence

Date: 2026-08-06  
Tool: Apalache 0.58.3  
Image: `ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795`

All commands ran from `formal/` using the canonical root `compose.yml`.

| Command/profile | Bound | Verdict |
| --- | ---: | --- |
| `typecheck tso/model.tla` | n/a | PASS (`Your types are purrfect!`) |
| `Allocation.cfg` / `AllocationNext` | 6 | PASS (`NoError`) |
| `Follower.cfg` / `FollowerNext` | 3 | PASS (`NoError`) |
| `Handoff.cfg` / `HandoffNext` | 9 | PASS (`NoError`) |
| `Client.cfg` / `ClientNext` | 5 | PASS (`NoError`) |
| `Tso.cfg` / `Next` | 5 | PASS (`NoError`) |

The verdict is limited to the finite domain and exclusions in `catalog.yaml`.
In particular, the handoff and client profiles witness bounded progress along
their prescribed paths; they do not establish temporal liveness or fairness.
