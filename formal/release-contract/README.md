# Release contract model

This finite model covers the release-integrity decision from a selected version
to its canonical contract, required target-version evidence, SHA-256 and commit
matching, target-version gate result, and publication. It explores stale commits,
other-version manifests, missing evidence, failed digests, and failed gates.

The bounded check is not a proof of SHA-256 collision resistance, external
service availability, test correctness, or human approval authenticity.

```bash
docker compose run --rm apalache --features=no-rows typecheck model.tla
docker compose run --rm apalache --features=no-rows check \
  --config=ReleaseContract.cfg --length=7 model.tla
```

`UnsafeDigest.cfg` deliberately permits verification after a failed digest and
must produce a counterexample to `VerifiedUsesValidDigests`.
