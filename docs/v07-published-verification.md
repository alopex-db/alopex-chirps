# v0.7 read-only publication verification

`scripts/release/verify-published-v0.7.sh` checks the already-approved stored
candidate and evidence first, then compares actual public artifacts with the
publication manifest bound as the evidence bundle's `release-bundle` artifact.
It does not package source, upload, repair, delete, or retry publication.

```bash
bash scripts/release/verify-published-v0.7.sh \
  --candidate candidate.json --candidate-sha256 "$candidate_sha256" \
  --evidence evidence.json --evidence-sha256 "$evidence_sha256" \
  --bundle bundle.json --bundle-sha256 "$bundle_sha256" \
  --tag-object "$approved_annotated_tag_object"
```

Here `bundle.json` is the evidence bundle referenced by the index, not the
publication manifest. The three SHA-256 values come from the stored release
inputs. The tag object is the exact annotated object recorded by the approved
tag job; it cannot be inferred from the expected source commit alone. Preserve
that value independently before running verification. The verifier requires
both this object and its peeled source commit to match remote refs, and rejects
lightweight tags.

The local publication validator is shared through the publisher's
`--validate-only` mode. This mode exits before any service request or environment
approval check; publication mode still requires the protected release approval.
Existing candidate, evidence binding, nine-package order, archive hashes,
production OCI contents, test-server exclusion, and asset allowlist checks apply
in both modes.

Remote checks use only `git ls-remote`, registry HTTP GET, `skopeo inspect --raw`,
and `gh api --method GET`. Install Git, Python 3.11+, skopeo, and gh; provide
read access to GitHub/OCI only as required by those tools. The verifier does not
use a Cargo publishing token. Normal TLS and host verification remain enabled.

- Every stored `.crate` is downloaded from crates.io and checked for exact size
  and SHA-256; missing or changed archives fail.
- The raw production OCI manifest must hash to the frozen digest. Skopeo reads
  the tagged reference, so a substituted tag cannot pass merely by retaining an
  expected digest string in local metadata.
- The GitHub Release must be published, non-draft, non-prerelease and use the
  exact tag. The dedicated paginated assets API must contain exactly the stored
  asset names, without duplicate IDs/names. Each asset's actual bytes are hashed;
  remote checksum metadata is not treated as proof. GitHub's automatically
  generated source zip/tar links are not uploaded assets and are not in this set.
- The tag, image manifest, Release identity and asset identities are rechecked
  after downloads to detect movement during verification. Download counters may
  change without invalidating artifact identity.

A missing service, authorization error, malformed response, missing asset,
extra asset, checksum mismatch, moved tag, or changed image fails closed. Local
input hashes are checked again after remote verification. Successful output
records the exact tag object, source commit, manifest digest and checked counts.
This is an observation of the remote state, not a guarantee against future
administrative changes.

## Validation

`bash scripts/release/verify-published-v0.7.sh --self-test` exercises stored-byte
drift plus 13 read-only verifier tests (including 18 missing/changed crate
subcases, annotated/lightweight tag substitution, image bytes, Release state,
asset omissions/additions/duplicates/substitution, pagination, request failure,
and mid-verification movement). The adapter tests assert GET-only requests and
bounded registry streaming.

`bash scripts/release/test-publish-v0.7-bundle.sh` preserves the existing approval,
order, resume, mismatch and test-artifact negative fixtures. It additionally
verifies that local validation performs zero requests to the fixture services.
These local fixtures do not constitute evidence that v0.7 is publicly released;
the full command must run against the final approved stored bundle after
publication.

Protocol references: [Git annotated tag output](https://www.kernel.org/pub/software/scm/git/docs/git-ls-remote.html),
[GitHub release assets](https://docs.github.com/en/rest/releases/assets), and
[skopeo raw inspection](https://github.com/podman-container-tools/skopeo/blob/main/docs/skopeo-inspect.1.md).
