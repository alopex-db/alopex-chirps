# Read-only task structure verification

`scripts/release/v07_task_structure.py` checks an explicitly supplied task document
against its independently supplied SHA-256. It does not fetch documents, execute
instructions from them, change task status, infer approval, or certify a release.
It is not connected to the release gate or CI: the private-input supply route
must be chosen separately.

```sh
python3 -B scripts/release/v07_task_structure.py \
  --tasks-file "$TASK_DOCUMENT" --expected-sha256 "$EXPECTED_TASK_SHA256"
```

Both arguments are required. The expected digest must be a lowercase 64-digit
hex SHA-256 obtained from a trusted source. Computing an expected digest from an
untrusted supplied file is not an independent identity check. The parser reads
only a regular file, at most 4 MiB, and hashes the same bytes it parses.

The document grammar uses top-level Markdown task entries such as
`- [ ] 1.2 Synthetic task`, with canonical numeric dotted IDs. Checked and
in-progress entries remain part of the ID set. Every entry must contain exactly
one indented `_Prompt: ..._` attribute on one line. Its eight nonempty fields are,
in order: `Role`, `Task`, `Restrictions`, `Leverage`, `Requirements`, `Validation`,
`Success`, `Instructions`, separated by `|`. An optional prose preamble may
precede `Role:`; it cannot introduce additional field labels. Field bodies are
not interpreted and are never included in output.

All concrete textual `Task`/`Tasks` references are checked throughout the
input, including Prompt bodies. Numeric references may be single IDs, slash,
comma or `and` lists, or inclusive sibling ranges using `-` or `–`. Backticks and
bold Markdown do not exempt references. Each expanded ID must exist; malformed,
descending, cross-parent, or excessive ranges fail. Generic prose such as
`Task X` is not a concrete numeric reference. The parser limits the document to
10,000 task IDs and 100,000 expanded reference occurrences.

Success emits JSON containing only the schema version, input digest, task,
Prompt and reference counts, and `result`. Failure exits 2 and emits a short
fixed code, optionally a validated numeric Task ID and line number. Neither
path names nor input text enter diagnostics, including invalid CLI arguments and
file errors. Callers should likewise avoid echoing private argument values or
uploading the input document. Keep any retained execution report separate from
synthetic test results.

Run the synthetic negative tests with:

```sh
python3 -B scripts/release/test-v07-task-structure.py
```

Those tests exercise missing/duplicate IDs and Prompts, unresolved references,
range expansion, field order/count, digest/size limits, and diagnostic canaries.
They do not contain the private document and cannot substitute for running this
parser against the exact authoritative bytes. A successful parser result says
only that the supplied document is structurally consistent; public projected
data, task completion, requirement correctness, and approval are distinct.
