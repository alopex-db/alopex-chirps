# Durable observability (v0.7)

This guide defines the implemented observability contract for the opt-in
Durable plane. It supplements the [Durable messaging
profile](durable-profile.md); it does not add another success boundary or a
second source of durable truth.

> Release status: this page describes the implemented v0.7 contract in the
> release branch. It is not a published release contract until the v0.7.0 tag
> and its attested artifacts are available.

## Rules at a glance

- Read Control-plane health and Durable-plane health independently. A fault or
  shutdown in one plane does not imply the same state in the other.
- Treat Durable lifecycle, readiness, and per-partition state as independent
  axes. Do not collapse them into one Boolean health value.
- Aggregate metrics only by the four bounded enum dimensions documented below.
  Correlation identity belongs in structured events, never metric labels.
- Correlate a partition chain by `(partition, resource_epoch)` and use
  `owner_epoch` to distinguish reopen or rebind generations.
- Expect event loss after the configured finite FIFO fills. Inspect
  `dropped_events` before treating the retained sequence as complete.
- Capture each runtime's report before dropping its handle. Reports and their
  event FIFOs are runtime-local and are not recovered after restart.
- Use operation results and recovered canonical state as truth. Metrics,
  health snapshots, and events are diagnostic projections only.

## Public access is read-only and runtime-local

`DurableHandle::observability()` returns `Some(DurableObservabilityReport)` for
a connected Durable runtime and `None` for an unconfigured handle. The owned
report is a finite copy: `snapshot()` returns buffer health,
`metric_series()` returns the retained bounded counters, and `events()` returns
the retained event FIFO. Reading a report cannot mutate the runtime or upgrade
an operation result.

The sink belongs to one runtime handle. A caller that needs a chain across
fresh processes must capture each handle's report before dropping the handle,
preserve the reports in runtime order, and merge matching events externally.
The report is diagnostic evidence, not a persisted audit record.

## Health has separate planes and axes

`ChirpsPlaneHealth` keeps `ControlPlaneHealth` separate from `DurableHealth`.
Control health reports either `Available` or an `Unavailable` reason of
`Transport` or `Shutdown`. A Durable failure therefore does not report the
Control transport as failed, and a Control failure does not rewrite Durable
state.

The public `DurableHandle::health()` method returns the Durable projection. Its
three axes have different responsibilities:

| Axis | Implemented values | Operator interpretation |
| --- | --- | --- |
| Lifecycle | `Starting`, `Ready`, `Draining`, `Closed` | Whether this runtime generation is establishing state, admitting work, draining admitted work, or closed. |
| Readiness | `Available`, `Unavailable(reason)`, `RecoveryRequired(reason)` | Whether new Durable work is currently allowed and whether explicit recovery is required. |
| Partition | `Active`, `Faulted(reason)`, `RecoveryRequired(reason)` | Whether one explicit partition may poll and deliver for its current owner. |

The axes are deliberately orthogonal. For example, `Ready + Unavailable` and
`Draining + Available` are representable states. Callers must inspect every
axis relevant to their operation.

Durable readiness distinguishes these bounded causes:

- `Unavailable`: unconfigured, connectivity, capability mismatch,
  authentication, permission, TLS identity mismatch, capacity, or shutdown;
- `RecoveryRequired`: checkpoint indeterminate, retention gap, resource epoch
  mismatch, corrupt local state, or owner epoch mismatch.

Per-partition health separately distinguishes invalid envelopes, invalid poll
observations, offset conflicts, partition capacity, stale delivery handles,
and the applicable recovery-required causes. A global readiness snapshot must
not hide a partition-local fault, and a partition-local fault must not be
promoted into a Control-plane failure.

## Metric schema is finite

`DurableMetricLabels` contains exactly four labels. Every label is a closed
enum; the schema accepts no free-form strings.

| Label | Allowed values |
| --- | --- |
| `operation` | `Send`, `Poll`, `Checkpoint`, `Recovery`, `Capacity` |
| `boundary` | `BeforeMutation`, `AppendInvoked`, `BrokerAccepted`, `OsSyncedAccepted`, `CheckpointInstall` |
| `failure_stage` | `None`, `Prepare`, `Preflight`, `Transport`, `Response`, `PollDecode`, `CheckpointWrite`, `FileSync`, `Rename`, `DirectorySync`, `Recovery`, `Capacity` |
| `outcome` | `Success`, `NotSubmitted`, `Indeterminate`, `NotCommitted`, `Unknown`, `Duplicate`, `Gap`, `RecoveryRequired`, `Unavailable` |

The runtime-owned `BoundedObservability` increments a monotonic `u64` counter
for an existing exact label set. A configured sink admits between 1 and 512
distinct series. When a new label set would exceed that limit, `record_metric` returns
`MetricSeriesCapacity`; it does not evict an existing series. Counter overflow
returns `CounterExhausted`. The Durable runtime currently constructs its owned
sink with 64 metric-series slots and 64 event slots.

The series count and its configured limit are available in
`ObservabilitySnapshot`. These counts describe the in-process sink, not the
number of successful Durable operations.

## Structured events carry correlation

`DurableEvent` consists of one bounded `DurableEventKind` and one
`DurableTraceContext`. The public schema vocabulary is:

- `Fault`, `Recovery`, `Redelivery`, and `Checkpoint`;
- `ResourceResync` and `SessionRebind`;
- `Duplicate` and `Gap`.

The trace context is exactly one of:

- `Attempt(DurableAttemptId)` for one explicit send attempt; or
- `Partition { partition, owner_epoch, resource_epoch }` for partition state,
  where `resource_epoch` contains the verified broker resource ID and its
  fenced epoch.

The v0.7 production runtime emits partition-trace `Fault`, `Recovery`,
`Redelivery`, `Checkpoint`, `SessionRebind`, and `Gap` events. The schema also
reserves the `ResourceResync` and `Duplicate` event kinds and the `Attempt`
trace context, but v0.7 does not claim production emission for those entries.
Send outcomes remain correlated by their public result and bounded metric
series, not by a promised attempt event.

`DurableObservabilityReport::partition_events(partition, resource_epoch)`
returns retained matching events from oldest to newest. The filter deliberately
keeps different `owner_epoch` values, but one production report remains local
to one runtime. Operators can reconstruct a restart chain only by preserving
and concatenating matching events from ordered reports, for example:

```text
Fault(owner 1)
  -> Recovery(owner 2)
  -> Redelivery(owner 2)
  -> Checkpoint(owner 2)
  -> SessionRebind(owner 3)
```

The partition and resource epoch identify one broker-resource incarnation;
the owner epoch shows where local ownership changed. An event does not contain
a timestamp, an operation result, or a checkpoint verdict. Event order is only
the FIFO order inside one report; ordering between reports is supplied by the
caller that captured the runtime sequence.

## Identity and secret boundary

Metric labels intentionally exclude all high-cardinality correlation values:
message and attempt IDs, source or target identities, partition numbers,
owner epochs, resource IDs, and resource epochs. Labels also never contain
ordering keys, payloads, payload-derived application data, credentials, tokens,
private keys, or permission documents.

Structured events allow only the correlation identities in
`DurableTraceContext`: an attempt ID or the partition/owner/resource tuple.
They do not contain a Durable message ID, source or target node, subscription
or consumer identity, ordering key, envelope or payload bytes, credential,
token, private key, or permission document. Applications and exporters must
not append those values as free-form fields when rendering the event.

## The event queue is a finite FIFO

A configured event capacity must be between 1 and 1,024. When the FIFO is
full, recording a new event evicts the oldest retained event, appends the new
event, and saturating-increments `dropped_event_count`. `events()` preserves
oldest-to-newest order for the events that remain.

`DurableObservabilityReport::snapshot()` returns an `ObservabilitySnapshot`
with `retained_events`, `event_capacity`, and
`dropped_events`. A nonzero dropped count means the visible event chain is
incomplete. The counter is cumulative for the lifetime of that in-process
sink; it does not recover evicted events and it is not a persisted audit log.

## Observability is not durable truth

No metric, health value, or event establishes an operation outcome:

- a `Success` metric does not replace `DurableSendResult`, its exact receipt,
  `SubscriptionCreationOutcome`, or `CheckpointOutcome`;
- a `Checkpoint` event does not prove `CheckpointCommitted`;
- a `Recovery` or `SessionRebind` event does not prove that canonical local
  state was accepted;
- absence of an event does not prove that an operation did not occur;
- a complete-looking retained chain does not replace restart validation of the
  canonical journal and broker state.

Callers must branch on the public operation result. After an indeterminate
send or checkpoint, callers must follow the reconciliation rules in the
[Durable messaging profile](durable-profile.md). On restart, the verified
broker binding and recovered canonical local state remain authoritative.
Observability can explain and correlate those decisions, but it cannot make or
upgrade them.
