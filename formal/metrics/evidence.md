# Metrics bounded-check evidence

Date: 2026-08-06  
Tool: Apalache 0.58.3  
Image: `ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795`  
Source base: `cbf50c6` plus the `formal/metrics/` files committed with this evidence

All commands ran from `formal/` with the canonical root `compose.yml`.

| Command/profile | Bound | Verdict |
| --- | ---: | --- |
| `typecheck metrics/model.tla` | n/a | PASS (`Your types are purrfect!`) |
| `AuthRequired.cfg` / `Next` | 6 | PASS (`NoError`) |
| `Public.cfg` / `Next` | 6 | PASS (`NoError`) |

The first authorization-profile check produced a modeling counterexample: a
completed response was compared with a later cache refresh. The model was
refined so source/refresh actions close the abstract response event, then both
profiles passed. The verdict is limited to two group labels and three source
revisions; it is not proof of HTTP/TLS security, exporter availability,
unbounded freshness, algorithm correctness, or production performance.
