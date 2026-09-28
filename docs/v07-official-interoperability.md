# Official development-profile interoperability probe

The probe targets only the unmodified Apache Iggy baseline
`f5350d999d883fd3ca9dd33b3dc2754ddb0df049`, tree
`0d6dcaf544588d3c0a54fe131a6de78b025eef14`, and unchanged Cargo.lock SHA-256
`0e4ac6717cfb6ba04894f734b8f56afc56e265925fdd805242b6e39d4d676b41`.
It accepts the separately built, non-publishable
`chirps-official-devbaseline-v1` manifest and streams the actual binary size and
digest before and after execution.

## Public configuration

`DurableConfig::broker_accepted` accepts the actual startup configuration,
`DurableDevelopmentResourceConfig` with numeric stream/topic/partition IDs, TLS,
credential reference, routing, local checkpoint configuration, and a frame limit.
It does not ask callers to invent a compatible server UUID, epoch, capability
digest, or lease. The backend verifies the numeric resource through the standard
protocol. Its fresh internal UUID is local attempt correlation only and is never
reported as a broker-issued incarnation or a strong receipt.

The existing `DurableConfig::new` constructor and compatible resource projection
remain available. Strong sessions still require the exact capability projection
and strictly bounded lease renewal. The new development constructor selects
`DurableProfile::BrokerAccepted` internally and does not require a private
extension. A strong send request through this development handle is rejected
through the public `DurableSendError::Unavailable` category without an append.

## Runtime verification

1. Start one owned TLS server with the fixture's actual explicit deduplication-off
   configuration; provision a fresh resource using the official SDK.
2. Connect through `DurableBuilder::connect` using the public development config.
3. Reject an `OsSyncedAccepted` send. Prepare and send two independent canonical
   envelopes through the public handle at `BrokerAccepted`, requiring no receipt.
4. Poll the actual bytes with a separate official SDK connection. Verify count,
   offset, broker message ID, complete canonical bytes, source/target, generation,
   partition, ordering key, and application payload against the pre-send ledger.
   Exactly two messages also excludes an append from the rejected strong request.
5. Close/join the public handle and SDK connections and gracefully stop the owned
   server. Write a report only after all checks and cleanup succeed.

```sh
cargo run --locked -p chirps-e2e --example durable_official_interop -- \
  OFFICIAL_MANIFEST_JSON CHIRPS_SOURCE_COMMIT NEW_OUTPUT_DIRECTORY
```

The caller binds the declared Chirps commit to the actual build and retains its
build/run logs. The report includes the client executable hash, official manifest
hash, startup configuration, independent expected and observed message bytes,
and cleanup result.

The public probe emits `chirps.v0.7.official-interoperability/v2`, identifies
`api_surface` as `DurableConfig`, and sets `public_durable_config_validated` only
when the public execution completed. Earlier v1 reports exercised only
`DevelopmentAppendConnection`; they cannot be reinterpreted as public facade
qualification. v2 omits the v1 local correlation fields and reports the actual
public strong-request error category.

## Development verification scope

Focused example tests cover source/artifact rejection, every single-byte readback
corruption, and executable hashing across buffer boundaries. The installed
coupling and ordinary mutation discovery tools exclude example targets (zero
analyzed files/mutants), so three generated return mutations were applied manually,
caught by the focused tests, and restored. The public facade/backend change is
separately tested and subjected to scoped mutation analysis.

Runtime qualification requires the actual Linux baseline artifact. Synthetic
unit tests and a compiled probe do not qualify this compatibility cell.

The facade tests independently vary TLS identity, credential reference, checkpoint
root/generation/journal bound, frame minimum, builder/config routing agreement,
and valid strong capability inputs. A generated partition-map test compares the
numeric resource configuration against an independent exact-set model, including
empty, duplicated, missing, and out-of-range partition IDs. Invalid configuration
must fail before credentials are resolved; valid configuration reaches the test
credential provider without starting network I/O.

The scoped coupling review covered the facade and backend separately: neither
reported a critical hotspot. Their existing composition roots intentionally bind
routing, transport, persistence, and lifecycle. Static coupling does not establish
runtime ordering, value/identity correctness, or behavior hidden by macros and
inactive feature conditions; the direct regressions and actual Linux probe cover
those separate concerns.

The final targeted mutation run covered the two configuration constructors,
partition extraction, and public `connect` validation: 27 caught, five unviable
because their replacement required an unimplemented `Default`, and five equivalent
survivors. Three surviving facade lease-condition substitutions are rejected by
`ExpectedCapability` and the backend's strict renewal guard; the two surviving
TLS-root/frame-condition substitutions are rejected by transport preparation.
All retain the same public `BackendConfiguration` failure before credential
resolution. Direct backend renewal-boundary tests prevent the duplicated facade
guard from hiding a backend validation gap. Earlier meaningful misses led to the
independent boundary tests above; the existing observer constructor was excluded
from this change's mutation scope.
