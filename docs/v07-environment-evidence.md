# Frozen runtime-environment bindings

`v07_environment_evidence.verify_environment` adds an identity check after the
existing E2E and Rust PERF semantic verifiers. It does not determine whether tests
or performance passed and does not replace either verifier. API and wire evidence
may continue to cover distinct operating systems; this hook only binds the two
E2E lanes and the existing comparable PERF observations.

The candidate's `environment_sha256` identifies the exact bytes of a
`chirps.v0.7.environment/v1` JSON object with these fields:

- `schema`: `chirps.v0.7.environment/v1`.
- `source_commit`, `iggy_commit`: the candidate's exact commit identities.
- `e2e_environment`: a relative `{path, sha256}` reference to the frozen E2E
  environment JSON. Its fields are the existing collector's `system`, `release`,
  `machine`, `node`, `rustc`, and `cargo` observations.
- `performance_axes`: the unchanged `ComparableAxes` object from the candidate's
  PERF plan. This includes the independently observed host fingerprint and the
  existing server/image/configuration/payload/partition and workload dimensions.

Record the actual E2E environment and the planned PERF axes before candidate
freeze. The environment manifest contains no candidate hash or references to PERF
results, so the candidate can include its digest without a reference cycle. A new
source or environment requires a new manifest and candidate. The frozen E2E JSON
and each run's observed E2E JSON must have exactly the same bytes and digest.

The hook checks every target in both mandatory E2E lanes against the frozen
source and environment. It checks A/A, bounds, safety, freeze, and paired PERF
candidate hashes and axes, then checks every raw observation's declared, beginning,
and finishing axes. It does not infer a missing measurement or convert a
serialized pass label into evidence. Opaque host/source fingerprints retain the
existing collector's provenance responsibilities; this module does not invent new
hardware measurements or reinterpret the fingerprints.

Call after successful semantic replay:

```python
from v07_environment_evidence import verify_environment

verify_environment(
    candidate_path,
    environment_manifest_path,
    production_lane_path,
    fault_lane_path,
    paired_path,
)
```

The central verifier selects exactly one `environment` entry, one production
`process` lane (excluding the separate `release-bundle` inventory), one `fault`
lane, and one `performance` entry. It verifies their outer digest bindings and calls this hook after complete E2E
and PERF semantic replay to check the inner observation bindings.

JSON reads are bounded at 256 MiB per artifact before decoding. Digest references
reject missing files, changed bytes, absolute/parent-traversing paths, and symlinks.
Focused synthetic fixtures test rehashed source/environment substitutions and all
PERF observation phases. They contain only identity fields and deliberately cannot
stand in for complete release evidence.
