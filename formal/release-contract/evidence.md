# Release contract model evidence

Status: PASS (2026-08-06 Asia/Tokyo)

## Checked scope

- checker: Apalache 0.58.3, image pinned by digest in `compose.yml`
- type mode: `--features=no-rows`
- bound: 7 transitions
- finite domain: versions `0.5.2` and `0.6.0`, their canonical contracts,
  target/stale commits, pass/fail/unknown digest and gate states, and
  complete/missing required evidence
- invariants: target contract, target evidence version, target commit, valid
  digests, required evidence completeness, target gate success, and
  publish/rejection safety

## Results

```text
typecheck model.tla: Type checker [OK]
ReleaseContract.cfg --length=7: NoError
UnsafeDigest.cfg --length=6: expected counterexample (EXITCODE: ERROR (12))
```

The unsafe profile bypasses only the digest check and violates
`VerifiedUsesValidDigests`. This demonstrates that the digest invariant is
reachable and sensitive to the modeled validation guard.

## Exclusions

This bounded result does not prove SHA-256 collision resistance, filesystem or
GitHub availability, correctness of the tests summarized by a gate result, or
human approval authenticity. Those are enforced or recorded by the executable
verifier, target-version gate, and release evidence manifest.
