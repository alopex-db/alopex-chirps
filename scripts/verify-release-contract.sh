#!/usr/bin/env bash
# Validates that a versioned release contract exists before publish.  The
# contract is reviewed evidence, not a substitute for the tests it references.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify-release-contract.sh --version X.Y.Z [--require-ready]
       [--manifest FILE] [--source-commit SHA] [--repo-root DIR]
       [--structure-only] [--candidate FILE --evidence FILE --bundle FILE]
       [--schema FILE]
       verify-release-contract.sh --publication-workflow

Checks docs/release/vX.Y.Z.md for traceability, exclusions, and approval
sections. --require-ready additionally rejects a non-READY release status and
unproven/TODO markers. Versions with a required-evidence catalog require a version-bound evidence
manifest, target-version gate, exact required evidence set, and artifact SHA-256
verification before --require-ready can succeed.

For v0.7.0, --structure-only checks the contract and self-contained schema
fixtures without future evidence. Full mode requires explicit external
candidate, evidence index, and bundle files and validates their byte bindings.
--publication-workflow parses the release workflow and validates the v0.7
protected-environment exact-byte publication dataflow without publishing.
USAGE
}

tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_root="$tool_root"
version=""
require_ready=false
manifest=""
source_commit=""
structure_only=false
candidate=""
evidence=""
bundle=""
schema=""
publication_workflow=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version="${2:?missing value for --version}"; shift 2 ;;
    --require-ready) require_ready=true; shift ;;
    --manifest) manifest="${2:?missing value for --manifest}"; shift 2 ;;
    --source-commit) source_commit="${2:?missing value for --source-commit}"; shift 2 ;;
    --structure-only) structure_only=true; shift ;;
    --candidate) candidate="${2:?missing value for --candidate}"; shift 2 ;;
    --evidence) evidence="${2:?missing value for --evidence}"; shift 2 ;;
    --bundle) bundle="${2:?missing value for --bundle}"; shift 2 ;;
    --schema) schema="${2:?missing value for --schema}"; shift 2 ;;
    --publication-workflow) publication_workflow=true; shift ;;
    --repo-root) repo_root="${2:?missing value for --repo-root}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$publication_workflow" == true ]]; then
  [[ -z "$version" \
    && "$require_ready" == false \
    && -z "$manifest" \
    && -z "$source_commit" \
    && "$structure_only" == false \
    && -z "$candidate" \
    && -z "$evidence" \
    && -z "$bundle" \
    && -z "$schema" ]] || {
    printf '%s\n' '--publication-workflow accepts no release or evidence arguments' >&2
    exit 2
  }
  python3 - "$repo_root/.github/workflows/release.yml" <<'PY'
from __future__ import annotations

import re
import shlex
import subprocess
import sys
from pathlib import Path


class WorkflowError(ValueError):
    pass


def fail(message: str) -> None:
    raise WorkflowError(message)


def scalar(value: str) -> object:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    if value == "true":
        return True
    if value == "false":
        return False
    if value.startswith("[") and value.endswith("]"):
        body = value[1:-1].strip()
        return [] if not body else [scalar(item) for item in body.split(",")]
    return value


def parse_workflow(path: Path) -> dict:
    raw_lines = path.read_text(encoding="utf-8").splitlines()
    tokens: list[tuple[int, str, int]] = []
    for line_number, raw in enumerate(raw_lines, 1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            fail(f"line {line_number}: tab indentation is forbidden")
        indent = len(raw) - len(raw.lstrip(" "))
        tokens.append((indent, raw[indent:], line_number))

    def key_value(content: str, line_number: int) -> tuple[str, str]:
        match = re.fullmatch(r"([^:]+):(.*)", content)
        if match is None:
            fail(f"line {line_number}: expected YAML mapping entry")
        key = match.group(1).strip()
        if not key:
            fail(f"line {line_number}: empty YAML key")
        return key, match.group(2).strip()

    def block(index: int, indent: int) -> tuple[object, int]:
        if index >= len(tokens) or tokens[index][0] != indent:
            fail("invalid YAML indentation")
        if tokens[index][1].startswith("- "):
            values: list[object] = []
            while index < len(tokens):
                current_indent, content, line_number = tokens[index]
                if current_indent < indent:
                    break
                if current_indent != indent or not content.startswith("- "):
                    break
                remainder = content[2:].strip()
                index += 1
                if not remainder:
                    if index >= len(tokens) or tokens[index][0] <= indent:
                        fail(f"line {line_number}: empty sequence entry")
                    value, index = block(index, tokens[index][0])
                    values.append(value)
                    continue
                if re.fullmatch(r"[^:]+:.*", remainder):
                    key, raw_value = key_value(remainder, line_number)
                    item: dict[str, object] = {}
                    if raw_value:
                        item[key] = scalar(raw_value)
                    elif index < len(tokens) and tokens[index][0] > indent:
                        item[key], index = block(index, tokens[index][0])
                    else:
                        item[key] = {}
                    if index < len(tokens) and tokens[index][0] > indent:
                        extra, index = block(index, tokens[index][0])
                        if not isinstance(extra, dict):
                            fail(f"line {line_number}: sequence mapping continuation is not a mapping")
                        duplicate = set(item).intersection(extra)
                        if duplicate:
                            fail(f"line {line_number}: duplicate YAML key {sorted(duplicate)}")
                        item.update(extra)
                    values.append(item)
                else:
                    values.append(scalar(remainder))
            return values, index

        values: dict[str, object] = {}
        while index < len(tokens):
            current_indent, content, line_number = tokens[index]
            if current_indent < indent:
                break
            if current_indent != indent or content.startswith("- "):
                break
            key, raw_value = key_value(content, line_number)
            if key in values:
                fail(f"line {line_number}: duplicate YAML key {key!r}")
            index += 1
            if raw_value in {"|", ">"}:
                block_lines: list[str] = []
                while index < len(tokens) and tokens[index][0] > indent:
                    child_indent, child, _ = tokens[index]
                    block_lines.append(" " * max(0, child_indent - indent - 2) + child)
                    index += 1
                values[key] = "\n".join(block_lines) + "\n"
            elif raw_value:
                values[key] = scalar(raw_value)
            elif index < len(tokens) and tokens[index][0] > indent:
                values[key], index = block(index, tokens[index][0])
            else:
                values[key] = {}
        return values, index

    if not tokens:
        fail("release workflow is empty")
    document, final_index = block(0, tokens[0][0])
    if final_index != len(tokens) or not isinstance(document, dict):
        fail("release workflow did not parse as one YAML mapping")
    return document


def mapping(value: object, label: str) -> dict:
    if not isinstance(value, dict):
        fail(f"{label} must be a mapping")
    return value


def sequence(value: object, label: str) -> list:
    if not isinstance(value, list):
        fail(f"{label} must be a sequence")
    return value


def command_argv(script: object, label: str) -> list[list[str]]:
    if not isinstance(script, str):
        fail(f"{label} must be a shell block")
    syntax = subprocess.run(
        ["bash", "-n"],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    if syntax.returncode != 0:
        fail(f"{label} is not valid Bash: {syntax.stderr.strip()}")
    logical: list[str] = []
    pending = ""
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pending += stripped[:-1] + " " if stripped.endswith("\\") else stripped
        if stripped.endswith("\\"):
            continue
        logical.append(pending)
        pending = ""
    result = []
    for line in logical:
        try:
            result.append(shlex.split(line, posix=True))
        except ValueError as exc:
            fail(f"{label} cannot be tokenized: {exc}")
    return result


def steps(job: dict, label: str) -> list[dict]:
    raw_steps = sequence(job.get("steps"), f"{label}.steps")
    return [
        mapping(item, f"{label}.steps[{index}]")
        for index, item in enumerate(raw_steps)
    ]


def one_step_with_use(items: list[dict], action: str, label: str) -> dict:
    matches = [item for item in items if item.get("uses") == action]
    if len(matches) != 1:
        fail(f"{label} must use {action} exactly once")
    return matches[0]


def run_commands(items: list[dict], label: str) -> list[list[str]]:
    result: list[list[str]] = []
    for index, item in enumerate(items):
        if "run" in item:
            result.extend(command_argv(item["run"], f"{label}.steps[{index}].run"))
    return result


workflow_path = Path(sys.argv[1]).resolve(strict=True)
workflow = parse_workflow(workflow_path)
dispatch = mapping(mapping(workflow.get("on"), "on").get("workflow_dispatch"), "workflow_dispatch")
inputs = mapping(dispatch.get("inputs"), "workflow_dispatch.inputs")
for input_name in ("commit", "v07_artifact_run_id", "v07_artifact_name"):
    if input_name not in inputs:
        fail(f"workflow_dispatch input {input_name} is missing")

jobs = mapping(workflow.get("jobs"), "jobs")
for job_name in ("ci-gate", "publish-tag", "publish-crate", "publish-v07-bundle", "create-release"):
    if job_name not in jobs:
        fail(f"required release job {job_name} is missing")

publication = mapping(jobs["publish-v07-bundle"], "publish-v07-bundle")
if publication.get("needs") != ["ci-gate", "publish-tag"]:
    fail("publish-v07-bundle must depend on ci-gate and publish-tag")
if publication.get("if") != "needs.ci-gate.outputs.version == '0.7.0'":
    fail("publish-v07-bundle version selection drifted")
if publication.get("environment") != "release":
    fail("publish-v07-bundle must use the protected release environment")
permissions = mapping(publication.get("permissions"), "publish-v07-bundle.permissions")
expected_permissions = {"actions": "read", "contents": "write", "packages": "write"}
if permissions != expected_permissions:
    fail("publish-v07-bundle permissions drifted")
publication_env = mapping(publication.get("env"), "publish-v07-bundle.env")
if publication_env.get("CHIRPS_RELEASE_ENVIRONMENT_APPROVAL") != "release:${{ inputs.commit }}":
    fail("publication approval is not bound to the explicit commit input")

publication_steps = steps(publication, "publish-v07-bundle")
if not any('echo "CHIRPS_POSTPUBLISH_EVIDENCE_DIR=${RUNNER_TEMP}/chirps-v07-registry-consumer" >> "$GITHUB_ENV"' in item.get("run", "") for item in publication_steps):
    fail("post-upload consumer evidence must be retained outside publisher scratch")
consumer_upload = [item for item in publication_steps if item.get("uses") == "actions/upload-artifact@v4" and item.get("with", {}).get("name") == "chirps-v07-registry-consumer-${{ github.run_id }}"]
if len(consumer_upload) != 1 or consumer_upload[0].get("if") != "always()" or consumer_upload[0].get("with", {}).get("path") != "${{ runner.temp }}/chirps-v07-registry-consumer":
    fail("post-upload consumer raw evidence must be preserved even on failure")
publication_checkouts = [item for item in publication_steps if item.get("uses") == "actions/checkout@v4"]
if len(publication_checkouts) != 2:
    fail("publication must isolate candidate source and trusted release tools")
checkout = next((item for item in publication_checkouts if not item.get("with", {}).get("path")), None)
trusted_checkout = next((item for item in publication_checkouts if item.get("with", {}).get("path") == "release-tools"), None)
if checkout is None or trusted_checkout is None or trusted_checkout.get("with", {}).get("ref") != "${{ github.sha }}":
    fail("publication tooling must use exact workflow source")
checkout_with = mapping(checkout.get("with"), "publish-v07-bundle checkout.with")
if checkout_with.get("ref") != "${{ inputs.commit }}":
    fail("publish-v07-bundle checkout does not use the explicit commit input")
downloads = [item for item in publication_steps if item.get("uses") == "actions/download-artifact@v4"]
if len(downloads) != 2:
    fail("publication must download frozen bytes and the same-run trusted verifier")
download = next((item for item in downloads if item.get("with", {}).get("name") == "${{ inputs.v07_artifact_name }}"), None)
if download is None:
    fail("publication has no frozen bundle download")
tool_download = next(item for item in downloads if item is not download)
if tool_download.get("with") != {"name": "chirps-v07-perf-verifier-${{ github.run_id }}", "path": "${{ runner.temp }}/chirps-perf-verifier"}:
    fail("trusted PERF tool must come from this run, never the candidate artifact run")
download_with = mapping(download.get("with"), "publish-v07-bundle download.with")
expected_download = {
    "name": "${{ inputs.v07_artifact_name }}",
    "path": "${{ runner.temp }}/chirps-v0.7-release-bundle",
    "github-token": "${{ github.token }}",
    "run-id": "${{ inputs.v07_artifact_run_id }}",
}
if download_with != expected_download:
    fail("publish-v07-bundle artifact identity dataflow drifted")

publication_commands = run_commands(publication_steps, "publish-v07-bundle")
publisher_calls = [
    argv
    for argv in publication_commands
    if argv and argv[0] == "$GITHUB_WORKSPACE/release-tools/scripts/release/publish-v0.7-bundle.sh"
]
if len(publisher_calls) != 1:
    fail("publish-v07-bundle must invoke the exact-byte publisher once")
publisher_call = publisher_calls[0]
expected_publisher_call = [
    "$GITHUB_WORKSPACE/release-tools/scripts/release/publish-v0.7-bundle.sh",
    "--bundle",
    "${root}/release-bundle.json",
    "--candidate",
    "${root}/candidate.json",
    "--evidence",
    "${root}/evidence.json",
    "--require-environment-approval",
    "--resume-only-on-checksum-match",
]
if publisher_call != expected_publisher_call:
    fail("exact-byte publisher arguments or artifact dataflow drifted")
for argv in publication_commands:
    for index, token in enumerate(argv):
        if token == "cargo" and index + 1 < len(argv) and argv[index + 1] in {"package", "publish"}:
            fail("v0.7 publication job may not package or publish through Cargo")
        if token == "scripts/publish-crate.sh" or token.endswith("/publish-crate.sh"):
            fail("v0.7 publication job may not invoke the legacy package publisher")

publish_tag = mapping(jobs["publish-tag"], "publish-tag")
if publish_tag.get("needs") != ["ci-gate"]:
    fail("publish-tag must depend on ci-gate")
if publish_tag.get("environment") != "release":
    fail("publish-tag must use the protected release environment")
publish_tag_checkout = one_step_with_use(
    steps(publish_tag, "publish-tag"), "actions/checkout@v4", "publish-tag"
)
if mapping(publish_tag_checkout.get("with"), "publish-tag checkout.with").get("ref") != "${{ inputs.commit }}":
    fail("publish-tag checkout does not use the explicit commit input")

ci_gate = mapping(jobs["ci-gate"], "ci-gate")
ci_steps = steps(ci_gate, "ci-gate")
ci_checkouts = [item for item in ci_steps if item.get("uses") == "actions/checkout@v4"]
if len(ci_checkouts) != 2:
    fail("ci-gate must contain separate source and release-tools checkouts")
checkout_by_path = {
    mapping(item.get("with"), "ci-gate checkout.with").get("path"): item
    for item in ci_checkouts
}
if set(checkout_by_path) != {"source", "release-tools"}:
    fail("ci-gate checkout paths must isolate source and release-tools")
ci_checkout = checkout_by_path["source"]
if mapping(ci_checkout.get("with"), "ci-gate checkout.with").get("ref") != "${{ inputs.commit }}":
    fail("ci-gate checkout does not use the explicit commit input")
if mapping(checkout_by_path["release-tools"].get("with"), "release-tools checkout.with").get("ref") != "${{ github.sha }}":
    fail("release-tools checkout does not use the workflow revision")
ci_defaults = mapping(ci_gate.get("defaults"), "ci-gate.defaults")
if mapping(ci_defaults.get("run"), "ci-gate.defaults.run").get("working-directory") != "source":
    fail("ci-gate commands must run in the candidate source checkout")
ci_download = one_step_with_use(ci_steps, "actions/download-artifact@v4", "ci-gate")
if mapping(ci_download.get("with"), "ci-gate download.with") != expected_download:
    fail("ci-gate and publication job do not consume the same stored artifact")
ci_commands = run_commands(ci_steps, "ci-gate")
for job_name, job in (("ci-gate", ci_gate), ("publish-v07-bundle", publication)):
    environment = mapping(job.get("env"), f"{job_name}.env")
    if environment.get("CHIRPS_RELEASE_TOOLS_COMMIT") != "${{ github.sha }}" or not any('echo "CHIRPS_PERF_VERIFIER=${RUNNER_TEMP}/chirps-perf-verifier/chirps-durable-perf" >> "$GITHUB_ENV"' in item.get("run", "") for item in steps(job, job_name)):
        fail("trusted PERF verifier path/source binding drifted")
expected_tool_build = ["python3", "$GITHUB_WORKSPACE/release-tools/scripts/release/v07_perf_verifier.py", "build", "--source-root", "$GITHUB_WORKSPACE/release-tools", "--source-commit", "${{ github.sha }}", "--output", "${RUNNER_TEMP}/chirps-perf-verifier"]
if expected_tool_build not in ci_commands:
    fail("CI must build the read-only verifier from exact workflow source")
tool_uploads = [item for item in ci_steps if item.get("uses") == "actions/upload-artifact@v4" and item.get("with", {}).get("name") == "chirps-v07-perf-verifier-${{ github.run_id }}"]
if len(tool_uploads) != 1 or tool_uploads[0].get("with") != {"name": "chirps-v07-perf-verifier-${{ github.run_id }}", "path": "${{ runner.temp }}/chirps-perf-verifier", "if-no-files-found": "error"}:
    fail("CI must preserve the exact same-run verifier artifact")
if "sparse-checkout" in checkout_by_path["release-tools"].get("with", {}):
    fail("trusted verifier needs the complete source tree")
required_ci_calls = {
    ("scripts/run-v0.7-release-gate.sh", "--structure-only"),
    ("scripts/release/test-publish-v0.7-bundle.sh",),
    ("scripts/verify-release-contract.sh", "--publication-workflow"),
}
ci_call_set = {tuple(argv) for argv in ci_commands}
if not required_ci_calls.issubset(ci_call_set):
    fail("ci-gate does not execute release and publication structure checks")
download_index = ci_steps.index(ci_download)
for required_call in required_ci_calls:
    matching_steps = [
        index
        for index, item in enumerate(ci_steps)
        if "run" in item
        and required_call in {
            tuple(argv)
            for argv in command_argv(item["run"], f"ci-gate.steps[{index}].run")
        }
    ]
    if not matching_steps or min(matching_steps) >= download_index:
        fail("ci-gate must run structure checks before downloading frozen bytes")
full_gate = [argv for argv in ci_commands if argv[:1] == ["scripts/run-v0.7-release-gate.sh"] and "--candidate" in argv]
if len(full_gate) != 1:
    fail("ci-gate must verify the downloaded v0.7 evidence exactly once")
expected_full_gate = [
    "scripts/run-v0.7-release-gate.sh",
    "--candidate",
    "${root}/candidate.json",
    "--evidence",
    "${root}/evidence.json",
    "--bundle",
    "${root}/bundle.json",
]
if full_gate[0] != expected_full_gate:
    fail("ci-gate evidence artifact dataflow drifted")

legacy = mapping(jobs["publish-crate"], "publish-crate")
release = mapping(jobs["create-release"], "create-release")
if legacy.get("if") != "needs.ci-gate.outputs.version != '0.7.0'":
    fail("legacy Cargo publication is not excluded from v0.7")
if release.get("if") != "needs.ci-gate.outputs.version != '0.7.0'":
    fail("legacy release creation is not excluded from v0.7")

print("v0.7 publication workflow structure validated")
PY
  exit 0
fi

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  printf '%s\n' '--version must be semantic version X.Y.Z' >&2
  exit 2
}
contract_rel="docs/release/v${version}.md"
contract="$repo_root/$contract_rel"
[[ -f "$contract" ]] || { printf 'missing release contract: %s\n' "$contract_rel" >&2; exit 1; }

if [[ "$version" == "0.7.0" ]]; then
  [[ -z "$manifest" ]] || {
    printf '%s\n' 'v0.7.0 uses --evidence rather than the v0.6 manifest format' >&2
    exit 2
  }
  schema="${schema:-$repo_root/docs/release/v0.7.0-evidence-schema.json}"
  [[ -f "$schema" ]] || {
    printf 'missing v0.7 evidence schema: %s\n' "$schema" >&2
    exit 1
  }
  evidence_verifier="$tool_root/scripts/release/verify-v0.7-evidence.py"
  [[ -f "$evidence_verifier" && -x "$evidence_verifier" ]] || {
    printf 'missing executable v0.7 evidence verifier: %s\n' "$evidence_verifier" >&2
    exit 1
  }

  if [[ "$structure_only" == true ]]; then
    [[ -z "$candidate" && -z "$evidence" && -z "$bundle" ]] || {
      printf '%s\n' '--structure-only does not accept future evidence paths' >&2
      exit 2
    }
    [[ "$require_ready" == false ]] || {
      printf '%s\n' '--structure-only does not make a release-readiness decision' >&2
      exit 2
    }
    python3 "$evidence_verifier" \
      --self-test "$schema"
    printf 'release contract structure validated: %s\n' "$contract_rel"
    exit 0
  fi

  [[ "$require_ready" == false ]] || {
    printf '%s\n' 'v0.7 evidence verification does not encode the final release verdict' >&2
    exit 2
  }
  [[ -n "$candidate" && -n "$evidence" && -n "$bundle" ]] || {
    printf '%s\n' 'v0.7 full verification requires --candidate, --evidence, and --bundle' >&2
    exit 2
  }
  python3 - "$evidence" "$candidate" "$bundle" "$source_commit" <<'PY'
import json
import re
import sys
from pathlib import Path

evidence = Path(sys.argv[1]).resolve(strict=True)
candidate = Path(sys.argv[2]).resolve(strict=True)
bundle = Path(sys.argv[3]).resolve(strict=True)
source_commit = sys.argv[4]
index = json.loads(evidence.read_bytes())
for name, supplied in (("candidate", candidate), ("bundle", bundle)):
    reference = index.get(name)
    if not isinstance(reference, dict) or not isinstance(reference.get("path"), str):
        raise SystemExit(f"evidence index has no {name} path reference")
    expected = (evidence.parent / reference["path"]).resolve(strict=True)
    if expected != supplied:
        raise SystemExit(f"supplied {name} is not the file referenced by evidence")
if source_commit:
    if re.fullmatch(r"[0-9a-f]{40}", source_commit) is None:
        raise SystemExit("--source-commit must be a lowercase 40-character SHA")
    candidate_object = json.loads(candidate.read_bytes())
    if candidate_object.get("source_commit") != source_commit:
        raise SystemExit("candidate source_commit differs from --source-commit")
PY
  python3 "$evidence_verifier" \
    --schema "$schema" "$evidence"
  printf 'release contract evidence validated: %s\n' "$contract_rel"
  exit 0
fi

[[ "$structure_only" == false \
  && -z "$candidate" \
  && -z "$evidence" \
  && -z "$bundle" \
  && -z "$schema" ]] || {
  printf '%s\n' 'v0.7 evidence arguments are only valid with --version 0.7.0' >&2
  exit 2
}

for heading in '## 要件・検証対応表' '## 未証明・除外事項' '## 変更影響レビュー' '## 承認'; do
  grep -Fqx "$heading" "$contract" || { printf 'missing heading %s in %s\n' "$heading" "$contract" >&2; exit 1; }
done
awk '
  /^## 要件・検証対応表$/ { in_matrix = 1; next }
  /^## / { in_matrix = 0 }
  in_matrix && /^\|/ && $0 !~ /^\|[[:space:]-]+\|/ && $0 !~ /^\| 要件 \/ 失敗モード \|/ {
    found = 1
  }
  END { exit(found ? 0 : 1) }
' "$contract" || {
  printf 'missing acceptance matrix data row in %s\n' "$contract" >&2
  exit 1
}

if [[ "$require_ready" == true ]]; then
  grep -Fqx 'Release readiness: READY' "$contract" || {
    printf 'release contract is not READY: %s\n' "$contract" >&2
    exit 1
  }
  awk -F '|' '
    function trim(value) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      return value
    }
    function unresolved(value) {
      return value ~ /(BLOCKED|未証明|未検証|TODO|未記入|未取得|未確定)/
    }
    /^## 要件・検証対応表$/ { section = "matrix"; next }
    /^## 未証明・除外事項$/ { section = "exclusions"; next }
    /^## 承認$/ { section = "approvals"; next }
    /^## / { section = ""; next }
    section == "matrix" && /^\|/ && $0 !~ /^\|[[:space:]-]+\|/ && $0 !~ /^\| 要件 \/ 失敗モード \|/ {
      if (unresolved(trim($(NF - 1)))) {
        printf "unresolved acceptance status: %s\n", $0 > "/dev/stderr"
        invalid = 1
      }
    }
    section == "exclusions" && /^\|/ && $0 !~ /^\|[[:space:]-]+\|/ && $0 !~ /^\| 項目 \|/ {
      if (unresolved(trim($(NF - 1)))) {
        printf "release-blocking exclusion: %s\n", $0 > "/dev/stderr"
        invalid = 1
      }
    }
    section == "approvals" && /^- / && unresolved($0) {
      printf "unresolved approval: %s\n", $0 > "/dev/stderr"
      invalid = 1
    }
    END { exit(invalid ? 1 : 0) }
  ' "$contract" || {
    printf 'release contract retains an unresolved release-blocking marker: %s\n' "$contract" >&2
    exit 1
  }
fi

requirements="$repo_root/docs/release/evidence/v${version}/required-evidence.json"
if [[ -n "$manifest" ]]; then
  [[ -f "$requirements" ]] || {
    printf 'manifest supplied but release evidence catalog is missing: %s\n' "$requirements" >&2
    exit 1
  }
  source_commit="${source_commit:-$(git -C "$repo_root" rev-parse HEAD)}"
  python3 "$tool_root/scripts/release/verify-evidence-manifest.py" \
    --manifest "$manifest" \
    --requirements "$repo_root/docs/release/evidence/v${version}/required-evidence.json" \
    --schema "$repo_root/docs/release/evidence/v${version}/manifest.schema.json" \
    --version "$version" \
    --source-commit "$source_commit"
elif [[ "$require_ready" == true && -f "$requirements" ]]; then
  printf 'READY verification requires --manifest and the target-version gate for %s\n' "$version" >&2
  exit 1
fi

printf 'release contract validated: %s\n' "$contract_rel"
