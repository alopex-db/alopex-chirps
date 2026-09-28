# Official development-profile interoperability probe

The probe targets only the unmodified Apache Iggy baseline
`f5350d999d883fd3ca9dd33b3dc2754ddb0df049`. It accepts the separately built,
non-publishable `chirps-official-devbaseline-v1` manifest and verifies the actual
binary size and digest before and after execution.

Verification plan before implementation builds:

1. Start one owned TLS server with the fixture's actual explicit deduplication-off
   configuration; provision a fresh resource using the official SDK.
2. Connect through `DevelopmentAppendConnection`. Numeric resource/partition and
   replication-factor readback comes from the standard protocol. The local UUID
   required by the attempt binding is application correlation only; it is never
   reported as a broker-issued incarnation or capability projection.
3. Reject `OsSyncedAccepted` before sending. Send two independently prepared
   canonical envelopes at `BrokerAccepted` and require no strong receipt.
4. Poll the actual bytes with a separate official SDK connection. Verify count,
   offset, broker message ID, complete canonical bytes, source/target, generation,
   partition, ordering key, and application payload against the pre-send ledger.
5. Close/join the adapter and SDK connections and gracefully stop the owned server.
   Write a report only after all checks and cleanup succeed.

The smallest development checks are example unit tests for manifest/readback
rejection, scoped Clippy, and mutation checks on the pure readback verifier.
Runtime qualification requires the actual Linux baseline artifact; synthetic
unit tests do not qualify this compatibility cell.

## Invocation and evidence scope

```sh
cargo run --locked -p chirps-e2e --example durable_official_interop -- \
  OFFICIAL_MANIFEST_JSON CHIRPS_SOURCE_COMMIT NEW_OUTPUT_DIRECTORY
```

The caller must bind the declared Chirps commit to the actual build and retain
its build/run logs. The report includes the running client executable hash,
official manifest hash, startup configuration, independent expected and observed
message bytes, and cleanup result. Large executable hashes are streamed.

The probe deliberately uses `DevelopmentAppendConnection`, not the public
`DurableConfig` facade. `public_durable_config_validated` is false in its report:
the public facade currently requires a compatible partition projection that the
official baseline does not provide. Passing this lower-level probe does not
resolve that public API compatibility requirement.

Development verification: focused example tests cover source/artifact rejection,
all single-byte readback corruptions, and executable hashing across buffer
boundaries. The installed coupling and ordinary mutation discovery tools exclude
example targets (zero analyzed files/mutants). The two generated pure-verifier
return mutations and the streaming hash return mutation are therefore applied
manually with the same focused Cargo command, then restored. Runtime baseline
qualification remains separate and requires the actual Linux artifact.
