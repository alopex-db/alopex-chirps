# Public source structure checks

`bash scripts/run-v0.7-release-gate.sh --public-structure-only` checks the
existing lane graph, actual E2E sources and Cargo manifest, the real public
candidate/evidence schema, and verifier self-tests. The transitional
`--structure-only` alias performs this same public subset. Neither is a complete
RELEASE-STRUCTURE verdict or release-readiness decision.

The read-only `scripts/release/v07_public_structure.py` helper is shared with
`tests/e2e/scripts/run-v07-lane.sh`. The runner invokes it before requesting
server artifact or corpus inputs. Production/fault strict selection uses the
same 10/4 target inventory as the E2E collector. It requires `autotests = false`,
explicit primary target/path pairs, all lane companions, their expected primary
imports, and reachability of every `.rs` source under `tests/e2e/tests` from a
Cargo test target. Unknown durable sources, orphan support modules, missing
sources, redirected imports, duplicate targets, and undeclared primaries fail.
The helper lexically separates strings (including raw strings), character
literals, line comments and nested block comments before reading top-level
file-module declarations. It supports the existing plain or `#[path]` imports;
other file-module attributes, nested external declarations and unterminated
literals/comments fail closed. It does not replace Cargo's type checking or
execute runtime tests.

The public schema is `docs/release/v0.7.0-evidence-schema.json`. The check reads
that file and reuses the production verifier's schema contract validation,
including source and configuration digest bindings. Actual production/test
manifests, patch series, toolchain/source identities and embedded bundle bindings
also pass through the same pure validator used by `prepare-v07-iggy-source.py`;
the public mode reads working-source bytes without fetching Git objects. These
are the existing source/configuration contracts, not newly invented schemas.
Runtime configuration and deployment evidence still require their existing
semantic validators after candidate freeze.

The public gate also executes the shared owned-target helper's negative and
idempotency tests. Complete RELEASE-STRUCTURE still requires the authoritative
task-ID/reference and eight-field Prompt validation. Its read-only checker is
documented in [task structure validation](v07-task-structure.md), but the private
task input's trusted CI supply route is not yet established. No fixed
private workspace path, copied task text, optional-input success, or self-declared
report substitutes for that input. Public source checks need no server binary,
corpus, post-freeze evidence, Cargo build, or network.

Development verification: `python3 scripts/release/test-v07-public-structure.py`
uses source-only temporary fixtures for missing/orphan/unknown inputs, both
allowlists, schema drift, and rejection before artifact environment checks.
These fixtures are not E2E or release qualification evidence.
