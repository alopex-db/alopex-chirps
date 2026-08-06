# HLC bounded-check evidence

Date: 2026-08-06  
Tool: Apalache 0.58.3  
Image: `ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795`

All commands ran from `formal/` using the canonical root `compose.yml`.

| Command/profile | Bound | Verdict |
| --- | ---: | --- |
| `typecheck hlc/model.tla` | n/a | PASS (`Your types are purrfect!`) |
| `Tick.cfg` / `TickNext` | 5 | PASS (`NoError`) |
| `Reorder.cfg` / `ReorderNext` | 6 | PASS (`NoError`) |
| `Skew.cfg` / `SkewNext` | 2 | PASS (`NoError`) |
| `Hlc.cfg` / `Next` | 4 | PASS (`NoError`) |

The verdict is limited to the finite domain and exclusions in `catalog.yaml`.
It is not evidence of unbounded convergence, liveness, timing, or production
performance.
