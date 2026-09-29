#!/usr/bin/env bash
# Run the target-version gate declared by a release evidence catalog.
set -euo pipefail

usage() {
  echo "Usage: $0 --repo-root DIR --version X.Y.Z --output-dir DIR --source-commit SHA" >&2
}

repo_root=""
version=""
output_dir=""
source_commit=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) repo_root="${2:?missing repo root}"; shift 2 ;;
    --version) version="${2:?missing version}"; shift 2 ;;
    --output-dir) output_dir="${2:?missing output dir}"; shift 2 ;;
    --source-commit) source_commit="${2:?missing source commit}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done
[[ -n "$repo_root" && -n "$version" && -n "$output_dir" && -n "$source_commit" ]] || { usage; exit 2; }

catalog="$repo_root/docs/release/evidence/v${version}/required-evidence.json"
[[ -f "$catalog" ]] || {
  printf 'release evidence catalog is missing: %s\n' "$catalog" >&2
  exit 1
}

runner="$(python3 - "$catalog" <<'PY'
import json
import sys
from pathlib import Path

catalog = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
runner = catalog.get("target_gate_runner")
if runner is not None:
    if not isinstance(runner, str) or not runner or Path(runner).is_absolute() or ".." in Path(runner).parts:
        raise SystemExit("target_gate_runner must be a relative repository path")
    print(runner)
PY
)"
runner_from_catalog="$runner"

if [[ -z "$runner" ]]; then
  mapfile -t legacy_runners < <(find "$repo_root/scripts" -maxdepth 1 -type f -name 'run-*-release-gate.sh' -print | sort)
  if [[ ${#legacy_runners[@]} -ne 1 ]]; then
    printf 'evidence catalog must declare target_gate_runner; found %s legacy runners\n' "${#legacy_runners[@]}" >&2
    exit 1
  fi
  runner="${legacy_runners[0]#"$repo_root/"}"
fi

runner_path="$repo_root/$runner"
[[ -f "$runner_path" ]] || { printf 'target gate runner is missing: %s\n' "$runner" >&2; exit 1; }
[[ "$runner_path" == *.sh ]] || { printf 'target gate runner must be a shell script: %s\n' "$runner" >&2; exit 1; }

echo "Running release evidence gate declared by ${catalog#$repo_root/}: ${runner}"
if [[ -n "${runner_from_catalog:-}" ]]; then
  bash "$runner_path" --version "$version" --output-dir "$output_dir" --source-commit "$source_commit"
else
  # Compatibility path for an older target commit whose catalog predates the
  # runner field. It is still fail-closed because exactly one legacy runner is
  # required above.
  bash "$runner_path" --output-dir "$output_dir" --source-commit "$source_commit"
fi
