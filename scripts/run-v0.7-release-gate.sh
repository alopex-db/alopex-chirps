#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: run-v0.7-release-gate.sh --structure-only
       run-v0.7-release-gate.sh --candidate FILE --evidence FILE --bundle FILE
                                  [--schema FILE]

Structure-only mode validates the pre-publication lane graph and self-contained
schema fixtures. Full mode additionally requires one external candidate,
evidence index, and bundle. This command never publishes or creates evidence.
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
structure_only=false
candidate=""
evidence=""
bundle=""
schema="$repo_root/docs/release/v0.7.0-evidence-schema.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --structure-only) structure_only=true; shift ;;
    --candidate) candidate="${2:?missing value for --candidate}"; shift 2 ;;
    --evidence) evidence="${2:?missing value for --evidence}"; shift 2 ;;
    --bundle) bundle="${2:?missing value for --bundle}"; shift 2 ;;
    --schema) schema="${2:?missing value for --schema}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage; exit 2 ;;
  esac
done

python3 - <<'PY'
from __future__ import annotations

lanes = {
    "schema": (),
    "source": ("schema",),
    "specification": ("source",),
    "model": ("specification",),
    "configuration": ("source",),
    "tool": ("source",),
    "environment": ("source",),
    "process": ("configuration", "environment", "tool"),
    "fault": ("model", "process"),
    "performance": ("configuration", "environment", "process"),
    "package": ("source", "tool"),
    "compatibility": ("package", "process"),
    "server": ("configuration", "source", "tool"),
    "security": ("package", "server"),
    "candidate": (
        "configuration",
        "environment",
        "model",
        "package",
        "server",
        "source",
        "specification",
        "tool",
    ),
    "evidence": (
        "candidate",
        "compatibility",
        "fault",
        "performance",
        "process",
        "security",
    ),
    "bundle": ("evidence",),
    "contract": ("bundle",),
    "release-gate": ("contract",),
}
required = {
    "schema",
    "source",
    "specification",
    "model",
    "configuration",
    "tool",
    "environment",
    "process",
    "fault",
    "performance",
    "package",
    "compatibility",
    "server",
    "security",
    "candidate",
    "evidence",
    "bundle",
    "contract",
    "release-gate",
}


def validate(graph: dict[str, tuple[str, ...]]) -> None:
    missing = required - set(graph)
    if missing:
        raise ValueError(f"missing required lane: {sorted(missing)}")
    unknown = {
        dependency
        for dependencies in graph.values()
        for dependency in dependencies
        if dependency not in graph
    }
    if unknown:
        raise ValueError(f"unknown lane dependency: {sorted(unknown)}")

    active: set[str] = set()
    complete: set[str] = set()

    def visit(lane: str) -> None:
        if lane in active:
            raise ValueError(f"lane dependency cycle reaches {lane}")
        if lane in complete:
            return
        active.add(lane)
        for dependency in graph[lane]:
            visit(dependency)
        active.remove(lane)
        complete.add(lane)

    visit("release-gate")
    unreachable = set(graph) - complete
    if unreachable:
        raise ValueError(f"lanes cannot reach release-gate: {sorted(unreachable)}")


validate(lanes)

missing_lane = dict(lanes)
del missing_lane["fault"]
missing_lane["evidence"] = tuple(
    dependency for dependency in missing_lane["evidence"] if dependency != "fault"
)
try:
    validate(missing_lane)
except ValueError:
    pass
else:
    raise SystemExit("missing-lane negative fixture unexpectedly passed")

unreachable_lane = dict(lanes)
unreachable_lane["orphan"] = ()
try:
    validate(unreachable_lane)
except ValueError:
    pass
else:
    raise SystemExit("unreachable-lane negative fixture unexpectedly passed")

future_cycle = dict(lanes)
future_cycle["published-verification"] = ("release-gate",)
future_cycle["release-gate"] = ("contract", "published-verification")
try:
    validate(future_cycle)
except ValueError:
    pass
else:
    raise SystemExit("future-cycle negative fixture unexpectedly passed")

print(
    "v0.7 lane graph validated: "
    "missing-lane, unreachable-lane, and future-cycle rejected"
)
PY

if [[ "$structure_only" == true ]]; then
  [[ -z "$candidate" && -z "$evidence" && -z "$bundle" ]] || {
    printf '%s\n' '--structure-only does not accept future evidence paths' >&2
    exit 2
  }
  "$repo_root/scripts/verify-release-contract.sh" \
    --version 0.7.0 --structure-only --schema "$schema"
  "$repo_root/scripts/release/verify-published-v0.7.sh" --self-test
  for check in consumer-evidence perf-verifier api-evidence semantic-hooks publication-workflow; do
    python3 "$repo_root/scripts/release/test-v07-${check}.py"
  done
  printf '%s\n' 'v0.7 release gate structure validated'
  exit 0
fi

[[ -n "$candidate" && -n "$evidence" && -n "$bundle" ]] || {
  printf '%s\n' 'full v0.7 gate requires --candidate, --evidence, and --bundle' >&2
  exit 2
}

"$repo_root/scripts/verify-release-contract.sh" \
  --version 0.7.0 \
  --candidate "$candidate" \
  --evidence "$evidence" \
  --bundle "$bundle" \
  --schema "$schema"
printf '%s\n' 'v0.7 release gate evidence validated'
