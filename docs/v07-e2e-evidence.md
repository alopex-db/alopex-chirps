# Complete E2E execution evidence

Add `--evidence-dir /external/new-production-directory` to the production
`run-v07-lane.sh --lane production --strict-all` invocation. Use a separate
new directory for the fault lane. All existing artifact trust anchors,
source-reconstruction checks, corpus requirements, and owned-server cleanup
remain prerequisites.

The collector requires a committed clean checkout, builds the named target
with Cargo's JSON artifact output, and obtains the ignored-test inventory
from that exact executable. It executes the same binary with one test thread
and checks every listed test name and the complete libtest result counts.
Successful test output is captured by libtest so it cannot interleave with
the result lines; failure output remains in the raw log. This collection mode
does not use `--nocapture`.

Each invocation retains build, discovery and execution logs, exit statuses,
the source commit/tree/lock, server manifest/binary identities, test binary
hash, corpus file inventory, and observed host/toolchain information. It
rechecks source, server, corpus and executable identities after execution.
The default per-command deadline is 900 seconds and each raw log has a
32 MiB output budget. A timeout or oversized log terminates only that
invocation's process group and records failure. Existing output is never
replaced.

Strict mode writes `lane.json` only after all ten production targets or all
four fault targets pass. The read-only verifier reparses the raw logs,
compares exact test inventories, and rejects empty suites, skipped tests,
mixed candidates, mixed server/environment/corpus identities, changed
commands, missing targets, path traversal and changed bytes. A `pass` label
alone is insufficient.

The release evidence verifier requires exactly one complete production lane
under `process` and one complete fault lane under `fault`. An additional
`release-bundle` process entry only identifies stored publication bytes; it
does not satisfy the production E2E requirement. The verifier applies these
checks on both release-gate and publisher entry points. A negative fixture
recomputes all outer hashes around an empty passing-looking fault report and
confirms that publication evidence is still rejected.

Run collector/verifier regression tests with:

```sh
python3 scripts/release/test-v07-e2e-evidence.py
```

These tests use explicitly synthetic fixtures. They establish collector
rejection behavior and never qualify a real server or a release candidate.
The report preserves local host/build paths in raw diagnostic logs; review
the intended release asset inventory before publishing those logs.
