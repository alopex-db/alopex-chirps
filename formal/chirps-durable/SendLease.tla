----------------------------- MODULE SendLease -----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Int;
    MaxAttempts,
    \* @type: Int;
    LeaseLength,
    \* @type: Str;
    UnsafeMode

Attempts == 1..MaxAttempts
Connections == {"none", "conn-a", "conn-b"}
Digests == {0, 101, 202}
Boundaries == {0, 11, 12}
Stages == {
    "idle", "started", "intent", "invoked", "written", "flushed",
    "message-synced", "index-synced", "terminal"
}
Outcomes == {"none", "not-submitted", "broker-accepted", "os-synced", "indeterminate"}
MutationKinds == {"none", "resource", "config", "security"}
MutationStages == {"stable", "planned", "epoch-durable", "fenced", "applied"}
Principals == {"none", "management", "runtime"}
PartitionStates == {"ready", "faulted", "recovery-required"}
LifecycleStates == {"running", "draining", "closed"}
TransportStates == {"open", "closed"}
Events == {
    "init", "prepare", "report", "bind", "binding-rejected", "binding-invalidated",
    "renew", "time", "expire", "fence",
    "start", "intent", "invoke", "write", "flush", "message-sync",
    "index-sync", "broker-response", "strong-response", "response-loss",
    "pre-failure", "post-failure", "duplicate-delivery", "retention",
    "mutation-plan", "mutation-rejected", "epoch-persist", "mutation-fence", "mutation-apply",
    "storage-fault", "boot-sync-fault", "drain", "transport-close", "join",
    "shutdown", "crash", "restart", "durable-fault", "unsafe"
}
UnsafeModes == {
    "none", "duplicate-append", "premature-receipt", "stale-success",
    "retry-identity", "mutation-before-durable", "outcome-rewrite",
    "missing-oracle", "duplicate-advances-frontier", "fallback",
    "runtime-mutation", "mutation-under-lease", "plane-coupling", "bootstrap-ready",
    "storage-continues", "cross-connection-renew", "resource-reuse",
    "invalid-binding-start", "false-not-submitted", "indeterminate-inexact",
    "broker-promoted", "shutdown-unjoined", "uncorrelated-event",
    "uncorrelated-attempt", "uncorrelated-epoch", "uncorrelated-race",
    "uncorrelated-storage",
    "pre-retention-loss", "unauthenticated-bind"
}
ScenarioIds == {
    "strong-send", "broker-send", "response-loss-retry", "pre-failure",
    "post-failure", "lease-expiry", "resource-mutation", "config-mutation",
    "security-mutation", "shutdown", "crash-pre", "crash-post",
    "storage-fault", "bootstrap-fault", "stale-lease", "stale-resource",
    "binding-missing-report", "binding-auth-failure",
    "binding-projection-mismatch", "binding-stale-epoch", "stale-race",
    "stage-storage-fault", "receipt-crash", "plane-fault"
}
BindingRejectReasons == {"none", "missing-report", "auth-failure", "projection-mismatch", "stale-epoch"}
ReportKinds == BindingRejectReasons \cup {"valid"}
RaceCauses == {
    "lease-expiry", "permission-revoke", "security-change",
    "config-change", "capability-change"
}
RaceStages == {"invoked", "written", "flushed", "message-synced", "index-synced"}

Terminal(o) == o \in (Outcomes \ {"none"})
Accepted(o) == o \in {"broker-accepted", "os-synced"}

EmptyAttemptInt == [a \in Attempts |-> 0]
EmptyAttemptBool == [a \in Attempts |-> FALSE]
EmptyAttemptConnection == [a \in Attempts |-> "none"]
EmptyAttemptOutcome == [a \in Attempts |-> "none"]
EmptyAttemptBoundary == [a \in Attempts |-> 0]
EmptyAttemptReportKind == [a \in Attempts |-> "none"]

VARIABLES
    \* @type: {
    \* scenario: Str, prepared: Bool, preparedDigest: Int, stage: Str, currentAttempt: Int,
    \* started: Int -> Bool, attemptDigest: Int -> Int,
    \* attemptResourceEpoch: Int -> Int, attemptConfigEpoch: Int -> Int,
    \* attemptSecurityEpoch: Int -> Int, attemptCapabilityEpoch: Int -> Int,
    \* attemptBootEpoch: Int -> Int, attemptConnection: Int -> Str,
    \* attemptProjectionDigest: Int -> Int, attemptBoundary: Int -> Int,
    \* attemptReportKind: Int -> Str,
    \* oracleIntent: Int -> Bool,
    \* invocationCount: Int -> Int, appendCount: Int -> Int,
    \* everAppended: Int -> Bool, present: Int -> Bool,
    \* evictedAfterEligibility: Int -> Bool, messageSynced: Int -> Bool,
    \* indexSynced: Int -> Bool, outcome: Int -> Str,
    \* terminalSnapshot: Int -> Str, strongReceipt: Int -> Bool,
    \* receiptDigest: Int -> Int, receiptAttempt: Int -> Int,
    \* receiptBoundary: Int -> Int, receiptResourceEpoch: Int -> Int,
    \* receiptConfigEpoch: Int -> Int, receiptSecurityEpoch: Int -> Int,
    \* receiptCapabilityEpoch: Int -> Int, receiptBootEpoch: Int -> Int,
    \* receiptProjectionDigest: Int -> Int, receiptConnection: Int -> Str,
    \* commitResourceEpoch: Int -> Int, commitConfigEpoch: Int -> Int,
    \* commitSecurityEpoch: Int -> Int, commitCapabilityEpoch: Int -> Int,
    \* commitBootEpoch: Int -> Int,
    \* commitConnection: Int -> Str, commitLeaseValid: Int -> Bool,
    \* reportKind: Str, reportConnection: Str, reportTlsVerified: Bool,
    \* reportAuthenticated: Bool, reportComplete: Bool,
    \* reportResourceEpoch: Int, reportConfigEpoch: Int,
    \* reportSecurityEpoch: Int, reportCapabilityEpoch: Int,
    \* reportBootEpoch: Int, reportProjectionDigest: Int,
    \* bindingConnection: Str, bindingReportKind: Str, bindingTlsVerified: Bool,
    \* bindingAuthenticated: Bool, bindingReportComplete: Bool,
    \* bindingProjectionMatches: Bool, bindingRejected: Bool,
    \* bindingRejectReason: Str, bindingResourceEpoch: Int,
    \* bindingConfigEpoch: Int, bindingSecurityEpoch: Int,
    \* bindingCapabilityEpoch: Int, bindingBootEpoch: Int,
    \* bindingProjectionDigest: Int, lastRenewConnection: Str,
    \* leaseExpiry: Int, now: Int, resourceEpoch: Int, maxResourceEpoch: Int,
    \* configEpoch: Int, securityEpoch: Int, capabilityEpoch: Int,
    \* bootEpoch: Int, projectionDigest: Int, mutationKind: Str,
    \* mutationStage: Str, mutationPrincipal: Str, pendingEpoch: Int,
    \* epochDurable: Bool, oldSessionsFenced: Bool, partitionState: Str,
    \* storageFault: Bool, bootSyncFailed: Bool, lifecycle: Str,
    \* startedSetFrozen: Bool, transport: Str, tasksJoined: Bool,
    \* controlAlive: Bool, durableUnavailable: Bool, fallbackUsed: Bool,
    \* responseLost: Bool,
    \* duplicateObserved: Bool, retentionEligible: Bool, deliveries: Int,
    \* unackedFrontier: Int, crashed: Bool, disrupted: Bool,
    \* raceCause: Str, raceStage: Str, faultStage: Str,
    \* eventAttempt: Int, eventPartition: Int, eventResourceEpoch: Int,
    \* eventOwnerConnection: Str, eventRaceCause: Str,
    \* eventPipelineStage: Str, lastEvent: Str};
    state

LeaseValid ==
    /\ state.bindingConnection # "none"
    /\ state.bindingReportKind = "valid"
    /\ state.bindingTlsVerified
    /\ state.bindingAuthenticated
    /\ state.bindingReportComplete
    /\ state.bindingProjectionMatches
    /\ state.bindingResourceEpoch = state.resourceEpoch
    /\ state.bindingConfigEpoch = state.configEpoch
    /\ state.bindingSecurityEpoch = state.securityEpoch
    /\ state.bindingCapabilityEpoch = state.capabilityEpoch
    /\ state.bindingBootEpoch = state.bootEpoch
    /\ state.bindingProjectionDigest = state.projectionDigest
    /\ state.now < state.leaseExpiry
    /\ state.partitionState = "ready"
    /\ state.lifecycle = "running"

Init ==
    \E selected \in ScenarioIds:
    \E selectedCause \in IF selected = "stale-race" THEN RaceCauses ELSE {"lease-expiry"}:
    \E selectedRaceStage \in IF selected = "stale-race" THEN RaceStages ELSE {"invoked"}:
    \E selectedFaultStage \in IF selected = "stage-storage-fault" THEN RaceStages ELSE {"invoked"}:
    state = [
        scenario |-> selected,
        prepared |-> FALSE,
        preparedDigest |-> 0,
        stage |-> "idle",
        currentAttempt |-> 0,
        started |-> EmptyAttemptBool,
        attemptDigest |-> EmptyAttemptInt,
        attemptResourceEpoch |-> EmptyAttemptInt,
        attemptConfigEpoch |-> EmptyAttemptInt,
        attemptSecurityEpoch |-> EmptyAttemptInt,
        attemptCapabilityEpoch |-> EmptyAttemptInt,
        attemptBootEpoch |-> EmptyAttemptInt,
        attemptConnection |-> EmptyAttemptConnection,
        attemptProjectionDigest |-> EmptyAttemptInt,
        attemptBoundary |-> EmptyAttemptBoundary,
        attemptReportKind |-> EmptyAttemptReportKind,
        oracleIntent |-> EmptyAttemptBool,
        invocationCount |-> EmptyAttemptInt,
        appendCount |-> EmptyAttemptInt,
        everAppended |-> EmptyAttemptBool,
        present |-> EmptyAttemptBool,
        evictedAfterEligibility |-> EmptyAttemptBool,
        messageSynced |-> EmptyAttemptBool,
        indexSynced |-> EmptyAttemptBool,
        outcome |-> EmptyAttemptOutcome,
        terminalSnapshot |-> EmptyAttemptOutcome,
        strongReceipt |-> EmptyAttemptBool,
        receiptDigest |-> EmptyAttemptInt,
        receiptAttempt |-> EmptyAttemptInt,
        receiptBoundary |-> EmptyAttemptBoundary,
        receiptResourceEpoch |-> EmptyAttemptInt,
        receiptConfigEpoch |-> EmptyAttemptInt,
        receiptSecurityEpoch |-> EmptyAttemptInt,
        receiptCapabilityEpoch |-> EmptyAttemptInt,
        receiptBootEpoch |-> EmptyAttemptInt,
        receiptProjectionDigest |-> EmptyAttemptInt,
        receiptConnection |-> EmptyAttemptConnection,
        commitResourceEpoch |-> EmptyAttemptInt,
        commitConfigEpoch |-> EmptyAttemptInt,
        commitSecurityEpoch |-> EmptyAttemptInt,
        commitCapabilityEpoch |-> EmptyAttemptInt,
        commitBootEpoch |-> EmptyAttemptInt,
        commitConnection |-> EmptyAttemptConnection,
        commitLeaseValid |-> EmptyAttemptBool,
        reportKind |-> "none",
        reportConnection |-> "none",
        reportTlsVerified |-> FALSE,
        reportAuthenticated |-> FALSE,
        reportComplete |-> FALSE,
        reportResourceEpoch |-> 0,
        reportConfigEpoch |-> 0,
        reportSecurityEpoch |-> 0,
        reportCapabilityEpoch |-> 0,
        reportBootEpoch |-> 0,
        reportProjectionDigest |-> 0,
        bindingConnection |-> "none",
        bindingReportKind |-> "none",
        bindingTlsVerified |-> FALSE,
        bindingAuthenticated |-> FALSE,
        bindingReportComplete |-> FALSE,
        bindingProjectionMatches |-> FALSE,
        bindingRejected |-> FALSE,
        bindingRejectReason |-> "none",
        bindingResourceEpoch |-> 0,
        bindingConfigEpoch |-> 0,
        bindingSecurityEpoch |-> 0,
        bindingCapabilityEpoch |-> 0,
        bindingBootEpoch |-> 0,
        bindingProjectionDigest |-> 0,
        lastRenewConnection |-> "none",
        leaseExpiry |-> 0,
        now |-> 0,
        resourceEpoch |-> 1,
        maxResourceEpoch |-> 1,
        configEpoch |-> 1,
        securityEpoch |-> 1,
        capabilityEpoch |-> 1,
        bootEpoch |-> 1,
        projectionDigest |-> 101,
        mutationKind |-> "none",
        mutationStage |-> "stable",
        mutationPrincipal |-> "none",
        pendingEpoch |-> 0,
        epochDurable |-> FALSE,
        oldSessionsFenced |-> FALSE,
        partitionState |-> "ready",
        storageFault |-> FALSE,
        bootSyncFailed |-> FALSE,
        lifecycle |-> "running",
        startedSetFrozen |-> FALSE,
        transport |-> "open",
        tasksJoined |-> FALSE,
        controlAlive |-> TRUE,
        durableUnavailable |-> FALSE,
        fallbackUsed |-> FALSE,
        responseLost |-> FALSE,
        duplicateObserved |-> FALSE,
        retentionEligible |-> FALSE,
        deliveries |-> 0,
        unackedFrontier |-> 0,
        crashed |-> FALSE,
        disrupted |-> FALSE,
        raceCause |-> selectedCause,
        raceStage |-> selectedRaceStage,
        faultStage |-> selectedFaultStage,
        eventAttempt |-> 0,
        eventPartition |-> 1,
        eventResourceEpoch |-> 1,
        eventOwnerConnection |-> "none",
        eventRaceCause |-> "init",
        eventPipelineStage |-> "idle",
        lastEvent |-> "init"
    ]

Prepare ==
    /\ ~state.prepared
    /\ state.lifecycle = "running"
    /\ state' = [state EXCEPT
        !.prepared = TRUE,
        !.preparedDigest = 101,
        !.eventAttempt = 0,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = "none",
        !.eventRaceCause = "prepare",
        !.eventPipelineStage = "idle",
        !.lastEvent = "prepare"]

ReportValid ==
    /\ state.reportKind = "valid"
    /\ state.reportConnection # "none"
    /\ state.reportTlsVerified
    /\ state.reportAuthenticated
    /\ state.reportComplete
    /\ state.reportResourceEpoch = state.resourceEpoch
    /\ state.reportConfigEpoch = state.configEpoch
    /\ state.reportSecurityEpoch = state.securityEpoch
    /\ state.reportCapabilityEpoch = state.capabilityEpoch
    /\ state.reportBootEpoch = state.bootEpoch
    /\ state.reportProjectionDigest = state.projectionDigest

PresentReport(kind, connection) ==
    /\ kind \in (ReportKinds \ {"none"})
    /\ connection \in (Connections \ {"none"})
    /\ state.bindingConnection = "none"
    /\ state.reportKind = "none"
    /\ state.lifecycle = "running"
    /\ state' = [state EXCEPT
        !.reportKind = kind,
        !.reportConnection = connection,
        !.reportTlsVerified = TRUE,
        !.reportAuthenticated = kind # "auth-failure",
        !.reportComplete = kind # "missing-report",
        !.reportResourceEpoch = IF kind = "stale-epoch" THEN 0 ELSE state.resourceEpoch,
        !.reportConfigEpoch = state.configEpoch,
        !.reportSecurityEpoch = state.securityEpoch,
        !.reportCapabilityEpoch = state.capabilityEpoch,
        !.reportBootEpoch = state.bootEpoch,
        !.reportProjectionDigest = IF kind = "projection-mismatch" THEN 202 ELSE state.projectionDigest,
        !.eventAttempt = 0,
        !.eventResourceEpoch = IF kind = "stale-epoch" THEN 0 ELSE state.resourceEpoch,
        !.eventOwnerConnection = connection,
        !.eventRaceCause = kind,
        !.eventPipelineStage = state.stage,
        !.lastEvent = "report"]

BindAuthenticated(connection) ==
    /\ connection \in (Connections \ {"none"})
    /\ state.reportConnection = connection
    /\ ReportValid
    /\ state.lifecycle = "running"
    /\ state.partitionState = "ready"
    /\ state.now + LeaseLength <= 4
    /\ state' = [state EXCEPT
        !.bindingConnection = connection,
        !.bindingReportKind = state.reportKind,
        !.bindingTlsVerified = TRUE,
        !.bindingAuthenticated = TRUE,
        !.bindingReportComplete = TRUE,
        !.bindingProjectionMatches = TRUE,
        !.bindingRejected = FALSE,
        !.bindingRejectReason = "none",
        !.bindingResourceEpoch = state.resourceEpoch,
        !.bindingConfigEpoch = state.configEpoch,
        !.bindingSecurityEpoch = state.securityEpoch,
        !.bindingCapabilityEpoch = state.capabilityEpoch,
        !.bindingBootEpoch = state.bootEpoch,
        !.bindingProjectionDigest = state.projectionDigest,
        !.lastRenewConnection = "none",
        !.leaseExpiry = state.now + LeaseLength,
        !.eventAttempt = 0,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = connection,
        !.eventRaceCause = "bind",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "bind"]

RejectBinding(reason) ==
    /\ reason \in (BindingRejectReasons \ {"none"})
    /\ state.reportKind = reason
    /\ ~ReportValid
    /\ state.bindingConnection = "none"
    /\ state.lifecycle = "running"
    /\ state' = [state EXCEPT
        !.bindingRejected = TRUE,
        !.bindingRejectReason = reason,
        !.eventAttempt = 0,
        !.eventResourceEpoch = state.reportResourceEpoch,
        !.eventOwnerConnection = state.reportConnection,
        !.eventRaceCause = reason,
        !.eventPipelineStage = state.stage,
        !.lastEvent = "binding-rejected"]

Renew(connection) ==
    /\ connection = state.bindingConnection
    /\ connection # "none"
    /\ state.now < state.leaseExpiry
    /\ state.now + LeaseLength <= 4
    /\ state' = [state EXCEPT
        !.lastRenewConnection = connection,
        !.leaseExpiry = state.now + LeaseLength,
        !.eventAttempt = 0,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = connection,
        !.eventRaceCause = "renew",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "renew"]

AdvanceTime ==
    /\ state.now < 4
    /\ state' = [state EXCEPT
        !.now = @ + 1,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "time",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "time"]

Expire ==
    /\ state.bindingConnection # "none"
    /\ state.now >= state.leaseExpiry
    /\ state' = [state EXCEPT
        !.bindingConnection = "none",
        !.bindingTlsVerified = FALSE,
        !.bindingAuthenticated = FALSE,
        !.bindingReportComplete = FALSE,
        !.bindingProjectionMatches = FALSE,
        !.lastRenewConnection = "none",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "expire",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "expire"]

Fence ==
    /\ state.bindingConnection # "none"
    /\ state' = [state EXCEPT
        !.bindingConnection = "none",
        !.bindingTlsVerified = FALSE,
        !.bindingAuthenticated = FALSE,
        !.bindingReportComplete = FALSE,
        !.bindingProjectionMatches = FALSE,
        !.lastRenewConnection = "none",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "fence",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "fence"]

CanStart(a) ==
    /\ a \in Attempts
    /\ state.prepared
    /\ LeaseValid
    /\ ~state.startedSetFrozen
    /\ ~state.started[a]
    /\ state.stage \in {"idle", "terminal"}
    /\ (a = 1 \/ (a = 2 /\ state.responseLost /\ state.outcome[1] = "indeterminate"))

StartAttempt(a) ==
    /\ CanStart(a)
    /\ state' = [state EXCEPT
        !.stage = "started",
        !.currentAttempt = a,
        !.started[a] = TRUE,
        !.attemptDigest[a] = state.preparedDigest,
        !.attemptResourceEpoch[a] = state.resourceEpoch,
        !.attemptConfigEpoch[a] = state.configEpoch,
        !.attemptSecurityEpoch[a] = state.securityEpoch,
        !.attemptCapabilityEpoch[a] = state.capabilityEpoch,
        !.attemptBootEpoch[a] = state.bootEpoch,
        !.attemptConnection[a] = state.bindingConnection,
        !.attemptProjectionDigest[a] = state.projectionDigest,
        !.attemptBoundary[a] = IF a = 1 THEN 11 ELSE 12,
        !.attemptReportKind[a] = state.bindingReportKind,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "start",
        !.eventPipelineStage = "started",
        !.lastEvent = "start"]

PersistOracleIntent ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ state.stage = "started"
    /\ state' = [state EXCEPT
        !.stage = "intent",
        !.oracleIntent[a] = TRUE,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "intent",
        !.eventPipelineStage = "intent",
        !.lastEvent = "intent"]

InvokeAppend ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ state.stage = "intent"
    /\ state.oracleIntent[a]
    /\ LeaseValid
    /\ state.bindingConnection = state.attemptConnection[a]
    /\ state.resourceEpoch = state.attemptResourceEpoch[a]
    /\ state.configEpoch = state.attemptConfigEpoch[a]
    /\ state.securityEpoch = state.attemptSecurityEpoch[a]
    /\ state.capabilityEpoch = state.attemptCapabilityEpoch[a]
    /\ state.bootEpoch = state.attemptBootEpoch[a]
    /\ state.projectionDigest = state.attemptProjectionDigest[a]
    /\ state.invocationCount[a] = 0
    /\ state' = [state EXCEPT
        !.stage = "invoked",
        !.invocationCount[a] = 1,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "invoke",
        !.eventPipelineStage = "invoked",
        !.lastEvent = "invoke"]

WriteRecord ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ state.stage = "invoked"
    /\ state.appendCount[a] = 0
    /\ state' = [state EXCEPT
        !.stage = "written",
        !.appendCount[a] = 1,
        !.everAppended[a] = TRUE,
        !.present[a] = TRUE,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "write",
        !.eventPipelineStage = "written",
        !.lastEvent = "write"]

FlushRecord ==
    /\ state.stage = "written"
    /\ state' = [state EXCEPT
        !.stage = "flushed",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.attemptResourceEpoch[state.currentAttempt],
        !.eventOwnerConnection = state.attemptConnection[state.currentAttempt],
        !.eventRaceCause = "flush",
        !.eventPipelineStage = "flushed",
        !.lastEvent = "flush"]

SyncMessage ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ state.stage = "flushed"
    /\ state' = [state EXCEPT
        !.stage = "message-synced",
        !.messageSynced[a] = TRUE,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "message-sync",
        !.eventPipelineStage = "message-synced",
        !.lastEvent = "message-sync"]

SyncIndex ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ state.stage = "message-synced"
    /\ state.messageSynced[a]
    /\ state' = [state EXCEPT
        !.stage = "index-synced",
        !.indexSynced[a] = TRUE,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "index-sync",
        !.eventPipelineStage = "index-synced",
        !.lastEvent = "index-sync"]

CommitMatchesAttempt(a) ==
    /\ LeaseValid
    /\ state.bindingConnection = state.attemptConnection[a]
    /\ state.resourceEpoch = state.attemptResourceEpoch[a]
    /\ state.configEpoch = state.attemptConfigEpoch[a]
    /\ state.securityEpoch = state.attemptSecurityEpoch[a]
    /\ state.capabilityEpoch = state.attemptCapabilityEpoch[a]
    /\ state.bootEpoch = state.attemptBootEpoch[a]
    /\ state.projectionDigest = state.attemptProjectionDigest[a]

BrokerResponse ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ state.stage \in {"written", "flushed", "message-synced", "index-synced"}
    /\ CommitMatchesAttempt(a)
    /\ state' = [state EXCEPT
        !.stage = "terminal",
        !.outcome[a] = "broker-accepted",
        !.terminalSnapshot[a] = "broker-accepted",
        !.commitResourceEpoch[a] = state.resourceEpoch,
        !.commitConfigEpoch[a] = state.configEpoch,
        !.commitSecurityEpoch[a] = state.securityEpoch,
        !.commitCapabilityEpoch[a] = state.capabilityEpoch,
        !.commitBootEpoch[a] = state.bootEpoch,
        !.commitConnection[a] = state.bindingConnection,
        !.commitLeaseValid[a] = TRUE,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "broker-response",
        !.eventPipelineStage = "terminal",
        !.lastEvent = "broker-response"]

StrongResponse ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ state.stage = "index-synced"
    /\ state.appendCount[a] = 1
    /\ state.messageSynced[a]
    /\ state.indexSynced[a]
    /\ CommitMatchesAttempt(a)
    /\ state' = [state EXCEPT
        !.stage = "terminal",
        !.outcome[a] = "os-synced",
        !.terminalSnapshot[a] = "os-synced",
        !.strongReceipt[a] = TRUE,
        !.receiptDigest[a] = state.attemptDigest[a],
        !.receiptAttempt[a] = a,
        !.receiptBoundary[a] = state.attemptBoundary[a],
        !.receiptResourceEpoch[a] = state.resourceEpoch,
        !.receiptConfigEpoch[a] = state.configEpoch,
        !.receiptSecurityEpoch[a] = state.securityEpoch,
        !.receiptCapabilityEpoch[a] = state.capabilityEpoch,
        !.receiptBootEpoch[a] = state.bootEpoch,
        !.receiptProjectionDigest[a] = state.projectionDigest,
        !.receiptConnection[a] = state.bindingConnection,
        !.commitResourceEpoch[a] = state.resourceEpoch,
        !.commitConfigEpoch[a] = state.configEpoch,
        !.commitSecurityEpoch[a] = state.securityEpoch,
        !.commitCapabilityEpoch[a] = state.capabilityEpoch,
        !.commitBootEpoch[a] = state.bootEpoch,
        !.commitConnection[a] = state.bindingConnection,
        !.commitLeaseValid[a] = TRUE,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "strong-response",
        !.eventPipelineStage = "terminal",
        !.lastEvent = "strong-response"]

FinishAttempt(outcome, event) ==
    LET a == state.currentAttempt IN
    /\ a \in Attempts
    /\ outcome \in {"not-submitted", "indeterminate"}
    /\ event \in {"pre-failure", "post-failure", "response-loss", "crash"}
    /\ state' = [state EXCEPT
        !.stage = "terminal",
        !.outcome[a] = outcome,
        !.terminalSnapshot[a] = outcome,
        !.responseLost = state.responseLost \/ (event = "response-loss"),
        !.crashed = state.crashed \/ (event = "crash"),
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = event,
        !.eventPipelineStage = "terminal",
        !.lastEvent = event]

PreInvocationFailure ==
    /\ state.stage \in {"started", "intent"}
    /\ FinishAttempt("not-submitted", "pre-failure")

PostInvocationFailure ==
    /\ state.stage \in {"invoked", "written", "flushed", "message-synced", "index-synced"}
    /\ FinishAttempt("indeterminate", "post-failure")

ResponseLoss ==
    /\ state.stage \in {"invoked", "written", "flushed", "message-synced", "index-synced"}
    /\ FinishAttempt("indeterminate", "response-loss")

ObserveDuplicateDelivery ==
    /\ state.responseLost
    /\ state.everAppended[1]
    /\ state.everAppended[2]
    /\ state.attemptDigest[1] = state.attemptDigest[2]
    /\ ~state.duplicateObserved
    /\ state' = [state EXCEPT
        !.duplicateObserved = TRUE,
        !.deliveries = 2,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.attemptResourceEpoch[state.currentAttempt],
        !.eventOwnerConnection = state.attemptConnection[state.currentAttempt],
        !.eventRaceCause = "duplicate-delivery",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "duplicate-delivery"]

MarkRetentionEligible ==
    /\ \E a \in Attempts: state.strongReceipt[a] /\ state.present[a]
    /\ ~state.retentionEligible
    /\ state' = [state EXCEPT
        !.retentionEligible = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.attemptConnection[state.currentAttempt],
        !.eventRaceCause = "retention",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "retention"]

EvictEligible(a) ==
    /\ a \in Attempts
    /\ state.retentionEligible
    /\ state.present[a]
    /\ state' = [state EXCEPT
        !.present[a] = FALSE,
        !.evictedAfterEligibility[a] = TRUE,
        !.eventAttempt = a,
        !.eventResourceEpoch = state.attemptResourceEpoch[a],
        !.eventOwnerConnection = state.attemptConnection[a],
        !.eventRaceCause = "retention",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "retention"]

PlanMutation(kind) ==
    /\ kind \in (MutationKinds \ {"none"})
    /\ state.mutationStage = "stable"
    /\ (kind = "security" \/ state.bindingConnection = "none")
    /\ ((kind \in {"resource", "config"} /\ state.resourceEpoch = 1)
        \/ (kind = "security" /\ state.securityEpoch = 1))
    /\ state' = [state EXCEPT
        !.mutationKind = kind,
        !.mutationStage = "planned",
        !.mutationPrincipal = "management",
        !.pendingEpoch = 2,
        !.epochDurable = FALSE,
        !.oldSessionsFenced = FALSE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "mutation-plan",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "mutation-plan"]

RejectProtectedMutation(kind) ==
    /\ kind \in {"resource", "config"}
    /\ state.mutationStage = "stable"
    /\ state.bindingConnection # "none"
    /\ state' = [state EXCEPT
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "mutation-rejected",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "mutation-rejected"]

InvalidateBinding(cause) ==
    /\ cause \in RaceCauses
    /\ state.bindingConnection # "none"
    /\ state' = [state EXCEPT
        !.bindingConnection = "none",
        !.bindingTlsVerified = FALSE,
        !.bindingAuthenticated = FALSE,
        !.bindingReportComplete = FALSE,
        !.bindingProjectionMatches = FALSE,
        !.lastRenewConnection = "none",
        !.now = IF cause = "lease-expiry" THEN state.leaseExpiry ELSE @,
        !.securityEpoch = IF cause \in {"permission-revoke", "security-change"} THEN 2 ELSE @,
        !.configEpoch = IF cause = "config-change" THEN 2 ELSE @,
        !.capabilityEpoch = IF cause = "capability-change" THEN 2 ELSE @,
        !.disrupted = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = cause,
        !.eventPipelineStage = state.stage,
        !.lastEvent = "binding-invalidated"]

PersistEpoch ==
    /\ state.mutationStage = "planned"
    /\ state' = [state EXCEPT
        !.mutationStage = "epoch-durable",
        !.resourceEpoch = IF state.mutationKind = "resource" THEN state.pendingEpoch ELSE @,
        !.maxResourceEpoch = IF state.mutationKind = "resource" THEN state.pendingEpoch ELSE @,
        !.configEpoch = IF state.mutationKind = "config" THEN state.pendingEpoch ELSE @,
        !.securityEpoch = IF state.mutationKind = "security" THEN state.pendingEpoch ELSE @,
        !.bindingConnection = IF state.mutationKind = "security" THEN "none" ELSE @,
        !.bindingTlsVerified = IF state.mutationKind = "security" THEN FALSE ELSE @,
        !.bindingAuthenticated = IF state.mutationKind = "security" THEN FALSE ELSE @,
        !.bindingReportComplete = IF state.mutationKind = "security" THEN FALSE ELSE @,
        !.bindingProjectionMatches = IF state.mutationKind = "security" THEN FALSE ELSE @,
        !.lastRenewConnection = IF state.mutationKind = "security" THEN "none" ELSE @,
        !.epochDurable = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = IF state.mutationKind = "resource" THEN state.pendingEpoch ELSE state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "epoch-persist",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "epoch-persist"]

FenceOldSessions ==
    /\ state.mutationStage = "epoch-durable"
    /\ state.epochDurable
    /\ state' = [state EXCEPT
        !.mutationStage = "fenced",
        !.bindingConnection = "none",
        !.bindingTlsVerified = FALSE,
        !.bindingAuthenticated = FALSE,
        !.bindingReportComplete = FALSE,
        !.bindingProjectionMatches = FALSE,
        !.lastRenewConnection = "none",
        !.oldSessionsFenced = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "mutation-fence",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "mutation-fence"]

ApplyMutation ==
    /\ state.mutationStage = "fenced"
    /\ state.epochDurable
    /\ state.oldSessionsFenced
    /\ state.mutationPrincipal = "management"
    /\ state' = [state EXCEPT
        !.mutationStage = "applied",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "mutation-apply",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "mutation-apply"]

StorageFault ==
    /\ ~state.storageFault
    /\ state' = [state EXCEPT
        !.storageFault = TRUE,
        !.partitionState = "faulted",
        !.durableUnavailable = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "storage-fault",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "storage-fault"]

BootSyncFailure ==
    /\ ~state.bootSyncFailed
    /\ state' = [state EXCEPT
        !.bootSyncFailed = TRUE,
        !.partitionState = "recovery-required",
        !.durableUnavailable = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "boot-sync-fault",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "boot-sync-fault"]

DurableFault ==
    /\ ~state.durableUnavailable
    /\ state' = [state EXCEPT
        !.durableUnavailable = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "durable-fault",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "durable-fault"]

BeginDrain ==
    /\ state.lifecycle = "running"
    /\ state' = [state EXCEPT
        !.lifecycle = "draining",
        !.startedSetFrozen = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "drain",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "drain"]

CloseTransport ==
    /\ state.lifecycle = "draining"
    /\ state.transport = "open"
    /\ state' = [state EXCEPT
        !.transport = "closed",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "transport-close",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "transport-close"]

JoinTasks ==
    /\ state.lifecycle = "draining"
    /\ state.transport = "closed"
    /\ ~state.tasksJoined
    /\ state' = [state EXCEPT
        !.tasksJoined = TRUE,
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "join",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "join"]

FinishShutdown ==
    /\ state.lifecycle = "draining"
    /\ state.transport = "closed"
    /\ state.tasksJoined
    /\ state' = [state EXCEPT
        !.lifecycle = "closed",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "shutdown",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "shutdown"]

Crash ==
    LET a == state.currentAttempt IN
    /\ state.lifecycle # "closed"
    /\ state.stage \in {"started", "intent", "invoked", "written", "flushed", "message-synced", "index-synced"}
    /\ a \in Attempts
    /\ FinishAttempt(IF state.invocationCount[a] = 0 THEN "not-submitted" ELSE "indeterminate", "crash")

CrashAfterReceipt ==
    /\ state.stage = "terminal"
    /\ \E a \in Attempts: state.strongReceipt[a] /\ state.present[a]
    /\ ~state.retentionEligible
    /\ ~state.crashed
    /\ state' = [state EXCEPT
        !.crashed = TRUE,
        !.partitionState = "recovery-required",
        !.bindingConnection = "none",
        !.bindingTlsVerified = FALSE,
        !.bindingAuthenticated = FALSE,
        !.bindingReportComplete = FALSE,
        !.bindingProjectionMatches = FALSE,
        !.lastRenewConnection = "none",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.attemptResourceEpoch[state.currentAttempt],
        !.eventOwnerConnection = state.attemptConnection[state.currentAttempt],
        !.eventRaceCause = "crash",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "crash"]

Restart ==
    /\ state.bootEpoch = 1
    /\ state.lifecycle # "closed"
    /\ state.crashed
    /\ state' = [state EXCEPT
        !.bootEpoch = 2,
        !.partitionState = "ready",
        !.crashed = FALSE,
        !.bindingConnection = "none",
        !.bindingTlsVerified = FALSE,
        !.bindingAuthenticated = FALSE,
        !.bindingReportComplete = FALSE,
        !.bindingProjectionMatches = FALSE,
        !.lastRenewConnection = "none",
        !.eventAttempt = state.currentAttempt,
        !.eventResourceEpoch = state.resourceEpoch,
        !.eventOwnerConnection = state.bindingConnection,
        !.eventRaceCause = "restart",
        !.eventPipelineStage = state.stage,
        !.lastEvent = "restart"]

UnsafeDuplicateAppend ==
    /\ state' = [state EXCEPT
        !.invocationCount[1] = 1,
        !.appendCount[1] = 2,
        !.lastEvent = "unsafe"]

UnsafePrematureReceipt ==
    /\ state' = [state EXCEPT
        !.stage = "terminal",
        !.invocationCount[1] = 1,
        !.outcome[1] = "os-synced",
        !.terminalSnapshot[1] = "os-synced",
        !.strongReceipt[1] = TRUE,
        !.receiptDigest[1] = state.preparedDigest,
        !.receiptResourceEpoch[1] = state.resourceEpoch,
        !.receiptConnection[1] = state.bindingConnection,
        !.lastEvent = "unsafe"]

UnsafeStaleSuccess ==
    /\ state' = [state EXCEPT
        !.stage = "terminal",
        !.started[1] = TRUE,
        !.attemptResourceEpoch[1] = 1,
        !.attemptSecurityEpoch[1] = 1,
        !.attemptBootEpoch[1] = 1,
        !.attemptConnection[1] = "conn-a",
        !.outcome[1] = "broker-accepted",
        !.terminalSnapshot[1] = "broker-accepted",
        !.commitResourceEpoch[1] = 2,
        !.commitSecurityEpoch[1] = 1,
        !.commitBootEpoch[1] = 1,
        !.commitConnection[1] = "conn-a",
        !.commitLeaseValid[1] = FALSE,
        !.lastEvent = "unsafe"]

UnsafeRetryIdentity ==
    /\ state' = [state EXCEPT
        !.prepared = TRUE,
        !.preparedDigest = 101,
        !.started[1] = TRUE,
        !.attemptDigest[1] = 202,
        !.lastEvent = "unsafe"]

UnsafeMutationBeforeDurable ==
    /\ state.mutationStage = "planned"
    /\ state' = [state EXCEPT
        !.mutationStage = "applied",
        !.lastEvent = "unsafe"]

UnsafeOutcomeRewrite ==
    /\ state' = [state EXCEPT
        !.outcome[1] = "indeterminate",
        !.terminalSnapshot[1] = "not-submitted",
        !.lastEvent = "unsafe"]

UnsafeMissingOracle ==
    /\ state' = [state EXCEPT
        !.stage = "invoked",
        !.invocationCount[1] = 1,
        !.oracleIntent[1] = FALSE,
        !.lastEvent = "unsafe"]

UnsafeDuplicateAdvancesFrontier ==
    /\ state' = [state EXCEPT
        !.duplicateObserved = TRUE,
        !.deliveries = 2,
        !.attemptDigest[1] = 101,
        !.attemptDigest[2] = 101,
        !.unackedFrontier = 1,
        !.lastEvent = "unsafe"]

UnsafeFallback ==
    /\ ~state.fallbackUsed
    /\ state' = [state EXCEPT
        !.durableUnavailable = TRUE,
        !.fallbackUsed = TRUE,
        !.lastEvent = "unsafe"]

UnsafeRuntimeMutation ==
    /\ state.mutationStage = "stable"
    /\ state' = [state EXCEPT
        !.mutationStage = "applied",
        !.mutationKind = "resource",
        !.mutationPrincipal = "runtime",
        !.lastEvent = "unsafe"]

UnsafeMutationUnderLease ==
    /\ state' = [state EXCEPT
        !.bindingConnection = "conn-a",
        !.leaseExpiry = 2,
        !.mutationKind = "resource",
        !.mutationStage = "planned",
        !.mutationPrincipal = "management",
        !.lastEvent = "unsafe"]

UnsafePlaneCoupling ==
    /\ state.controlAlive
    /\ state' = [state EXCEPT
        !.durableUnavailable = TRUE,
        !.controlAlive = FALSE,
        !.lastEvent = "unsafe"]

UnsafeBootstrapReady ==
    /\ ~state.bootSyncFailed
    /\ state' = [state EXCEPT
        !.bootSyncFailed = TRUE,
        !.partitionState = "ready",
        !.lastEvent = "unsafe"]

UnsafeStorageContinues ==
    /\ ~state.storageFault
    /\ state' = [state EXCEPT
        !.storageFault = TRUE,
        !.partitionState = "ready",
        !.lastEvent = "unsafe"]

UnsafeCrossConnectionRenew ==
    /\ state.bindingConnection = "conn-a"
    /\ state' = [state EXCEPT
        !.lastRenewConnection = "conn-b",
        !.lastEvent = "unsafe"]

UnsafeResourceReuse ==
    /\ state.resourceEpoch = 2
    /\ state' = [state EXCEPT
        !.resourceEpoch = 1,
        !.lastEvent = "unsafe"]

UnsafeInvalidBindingStart ==
    /\ state' = [state EXCEPT
        !.prepared = TRUE,
        !.preparedDigest = 101,
        !.started[1] = TRUE,
        !.attemptDigest[1] = 101,
        !.attemptConnection[1] = "none",
        !.lastEvent = "unsafe"]

UnsafeFalseNotSubmitted ==
    /\ state' = [state EXCEPT
        !.outcome[1] = "not-submitted",
        !.terminalSnapshot[1] = "not-submitted",
        !.invocationCount[1] = 1,
        !.lastEvent = "unsafe"]

UnsafeIndeterminateInexact ==
    /\ state' = [state EXCEPT
        !.prepared = TRUE,
        !.preparedDigest = 101,
        !.started[1] = TRUE,
        !.attemptDigest[1] = 202,
        !.attemptResourceEpoch[1] = 1,
        !.attemptConfigEpoch[1] = 1,
        !.attemptSecurityEpoch[1] = 1,
        !.attemptCapabilityEpoch[1] = 1,
        !.attemptBootEpoch[1] = 1,
        !.attemptConnection[1] = "conn-a",
        !.attemptProjectionDigest[1] = 101,
        !.attemptBoundary[1] = 11,
        !.oracleIntent[1] = TRUE,
        !.invocationCount[1] = 1,
        !.appendCount[1] = 1,
        !.everAppended[1] = TRUE,
        !.present[1] = TRUE,
        !.outcome[1] = "indeterminate",
        !.terminalSnapshot[1] = "indeterminate",
        !.lastEvent = "unsafe"]

UnsafeBrokerPromoted ==
    /\ state' = [state EXCEPT
        !.outcome[1] = "broker-accepted",
        !.terminalSnapshot[1] = "broker-accepted",
        !.strongReceipt[1] = TRUE,
        !.lastEvent = "unsafe"]

UnsafeShutdownUnjoined ==
    /\ state' = [state EXCEPT
        !.lifecycle = "closed",
        !.startedSetFrozen = TRUE,
        !.transport = "closed",
        !.tasksJoined = FALSE,
        !.lastEvent = "unsafe"]

UnsafeUncorrelatedEvent ==
    /\ state' = [state EXCEPT
        !.lastEvent = "strong-response"]

UnsafeUncorrelatedAttempt ==
    /\ state' = [state EXCEPT
        !.strongReceipt[1] = TRUE,
        !.eventAttempt = 0,
        !.eventResourceEpoch = 1,
        !.eventOwnerConnection = "conn-a",
        !.eventRaceCause = "strong-response",
        !.eventPipelineStage = "terminal",
        !.lastEvent = "strong-response"]

UnsafeUncorrelatedEpoch ==
    /\ state' = [state EXCEPT
        !.strongReceipt[1] = TRUE,
        !.attemptResourceEpoch[1] = 1,
        !.attemptConnection[1] = "conn-a",
        !.eventAttempt = 1,
        !.eventResourceEpoch = 2,
        !.eventOwnerConnection = "conn-a",
        !.eventRaceCause = "strong-response",
        !.eventPipelineStage = "terminal",
        !.lastEvent = "strong-response"]

UnsafeUncorrelatedRace ==
    /\ state' = [state EXCEPT
        !.currentAttempt = 1,
        !.attemptResourceEpoch[1] = 1,
        !.attemptConnection[1] = "conn-a",
        !.bindingConnection = "none",
        !.disrupted = TRUE,
        !.eventAttempt = 1,
        !.eventResourceEpoch = 1,
        !.eventOwnerConnection = "conn-a",
        !.eventRaceCause = "config-change",
        !.eventPipelineStage = "written",
        !.lastEvent = "binding-invalidated"]

UnsafeUncorrelatedStorage ==
    /\ state' = [state EXCEPT
        !.storageFault = TRUE,
        !.partitionState = "faulted",
        !.durableUnavailable = TRUE,
        !.eventAttempt = 0,
        !.eventResourceEpoch = 2,
        !.eventOwnerConnection = "conn-b",
        !.eventRaceCause = "storage-fault",
        !.eventPipelineStage = "index-synced",
        !.lastEvent = "storage-fault"]

UnsafePreRetentionLoss ==
    /\ state' = [state EXCEPT
        !.strongReceipt[1] = TRUE,
        !.outcome[1] = "os-synced",
        !.terminalSnapshot[1] = "os-synced",
        !.appendCount[1] = 1,
        !.everAppended[1] = TRUE,
        !.messageSynced[1] = TRUE,
        !.indexSynced[1] = TRUE,
        !.present[1] = FALSE,
        !.retentionEligible = FALSE,
        !.lastEvent = "unsafe"]

UnsafeUnauthenticatedBind ==
    /\ state' = [state EXCEPT
        !.bindingConnection = "conn-a",
        !.bindingTlsVerified = FALSE,
        !.bindingAuthenticated = FALSE,
        !.bindingReportComplete = TRUE,
        !.bindingProjectionMatches = TRUE,
        !.bindingResourceEpoch = state.resourceEpoch,
        !.bindingConfigEpoch = state.configEpoch,
        !.bindingSecurityEpoch = state.securityEpoch,
        !.bindingCapabilityEpoch = state.capabilityEpoch,
        !.bindingBootEpoch = state.bootEpoch,
        !.bindingProjectionDigest = state.projectionDigest,
        !.leaseExpiry = 2,
        !.lastEvent = "unsafe"]

UnsafeAction ==
    CASE UnsafeMode = "duplicate-append" -> UnsafeDuplicateAppend
      [] UnsafeMode = "premature-receipt" -> UnsafePrematureReceipt
      [] UnsafeMode = "stale-success" -> UnsafeStaleSuccess
      [] UnsafeMode = "retry-identity" -> UnsafeRetryIdentity
      [] UnsafeMode = "mutation-before-durable" -> UnsafeMutationBeforeDurable
      [] UnsafeMode = "outcome-rewrite" -> UnsafeOutcomeRewrite
      [] UnsafeMode = "missing-oracle" -> UnsafeMissingOracle
      [] UnsafeMode = "duplicate-advances-frontier" -> UnsafeDuplicateAdvancesFrontier
      [] UnsafeMode = "fallback" -> UnsafeFallback
      [] UnsafeMode = "runtime-mutation" -> UnsafeRuntimeMutation
      [] UnsafeMode = "mutation-under-lease" -> UnsafeMutationUnderLease
      [] UnsafeMode = "plane-coupling" -> UnsafePlaneCoupling
      [] UnsafeMode = "bootstrap-ready" -> UnsafeBootstrapReady
      [] UnsafeMode = "storage-continues" -> UnsafeStorageContinues
      [] UnsafeMode = "cross-connection-renew" -> UnsafeCrossConnectionRenew
      [] UnsafeMode = "resource-reuse" -> UnsafeResourceReuse
      [] UnsafeMode = "invalid-binding-start" -> UnsafeInvalidBindingStart
      [] UnsafeMode = "false-not-submitted" -> UnsafeFalseNotSubmitted
      [] UnsafeMode = "indeterminate-inexact" -> UnsafeIndeterminateInexact
      [] UnsafeMode = "broker-promoted" -> UnsafeBrokerPromoted
      [] UnsafeMode = "shutdown-unjoined" -> UnsafeShutdownUnjoined
      [] UnsafeMode = "uncorrelated-event" -> UnsafeUncorrelatedEvent
      [] UnsafeMode = "uncorrelated-attempt" -> UnsafeUncorrelatedAttempt
      [] UnsafeMode = "uncorrelated-epoch" -> UnsafeUncorrelatedEpoch
      [] UnsafeMode = "uncorrelated-race" -> UnsafeUncorrelatedRace
      [] UnsafeMode = "uncorrelated-storage" -> UnsafeUncorrelatedStorage
      [] UnsafeMode = "pre-retention-loss" -> UnsafePreRetentionLoss
      [] UnsafeMode = "unauthenticated-bind" -> UnsafeUnauthenticatedBind
      [] OTHER -> /\ state' = state
                  /\ FALSE

Stutter == state' = state

AllActions ==
    \/ Prepare
    \/ \E kind \in (ReportKinds \ {"none"}), c \in (Connections \ {"none"}): PresentReport(kind, c)
    \/ \E c \in (Connections \ {"none"}): BindAuthenticated(c)
    \/ \E reason \in (BindingRejectReasons \ {"none"}): RejectBinding(reason)
    \/ \E c \in (Connections \ {"none"}): Renew(c)
    \/ AdvanceTime
    \/ Expire
    \/ Fence
    \/ \E a \in Attempts: StartAttempt(a)
    \/ PersistOracleIntent
    \/ InvokeAppend
    \/ WriteRecord
    \/ FlushRecord
    \/ SyncMessage
    \/ SyncIndex
    \/ BrokerResponse
    \/ StrongResponse
    \/ PreInvocationFailure
    \/ PostInvocationFailure
    \/ ResponseLoss
    \/ ObserveDuplicateDelivery
    \/ MarkRetentionEligible
    \/ \E a \in Attempts: EvictEligible(a)
    \/ \E kind \in (MutationKinds \ {"none"}): PlanMutation(kind)
    \/ \E kind \in {"resource", "config"}: RejectProtectedMutation(kind)
    \/ \E cause \in RaceCauses: InvalidateBinding(cause)
    \/ PersistEpoch
    \/ FenceOldSessions
    \/ ApplyMutation
    \/ StorageFault
    \/ BootSyncFailure
    \/ DurableFault
    \/ BeginDrain
    \/ CloseTransport
    \/ JoinTasks
    \/ FinishShutdown
    \/ Crash
    \/ CrashAfterReceipt
    \/ Restart
    \/ UnsafeAction
    \/ Stutter

\* This deterministic scenario is a reachability harness over production-like
\* actions, not another implementation transition. It proves that response loss
\* followed by an explicit same-ID retry can expose two deliveries while the
\* canonical unacked frontier remains unchanged.
DuplicateWitnessNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" /\ state.currentAttempt = 1 -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> WriteRecord
      [] state.lastEvent = "write" /\ state.currentAttempt = 1 -> ResponseLoss
      [] state.lastEvent = "response-loss" /\ state.currentAttempt = 1 -> StartAttempt(2)
      [] state.lastEvent = "start" /\ state.currentAttempt = 2 -> PersistOracleIntent
      [] state.lastEvent = "write" /\ state.currentAttempt = 2 -> ResponseLoss
      [] state.lastEvent = "response-loss" /\ state.currentAttempt = 2 -> ObserveDuplicateDelivery
      [] OTHER -> Stutter

StrongSendNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> WriteRecord
      [] state.lastEvent = "write" -> FlushRecord
      [] state.lastEvent = "flush" -> SyncMessage
      [] state.lastEvent = "message-sync" -> SyncIndex
      [] state.lastEvent = "index-sync" -> StrongResponse
      [] state.lastEvent = "strong-response" -> MarkRetentionEligible
      [] state.lastEvent = "retention" /\ state.present[1] -> EvictEligible(1)
      [] OTHER -> Stutter

BrokerSendNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> WriteRecord
      [] state.lastEvent = "write" -> BrokerResponse
      [] OTHER -> Stutter

PreFailureNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PreInvocationFailure
      [] OTHER -> Stutter

PostFailureNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> PostInvocationFailure
      [] OTHER -> Stutter

LeaseExpiryNext ==
    CASE state.lastEvent = "init" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> Renew("conn-a")
      [] state.lastEvent = "renew" -> AdvanceTime
      [] state.lastEvent = "time" /\ state.now = 1 -> AdvanceTime
      [] state.lastEvent = "time" /\ state.now = 2 -> Expire
      [] OTHER -> Stutter

ResourceMutationNext(kind) ==
    CASE state.lastEvent = "init" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> IF kind = "security" THEN PlanMutation(kind) ELSE Fence
      [] state.lastEvent = "fence" -> PlanMutation(kind)
      [] state.lastEvent = "mutation-plan" -> PersistEpoch
      [] state.lastEvent = "epoch-persist" -> FenceOldSessions
      [] state.lastEvent = "mutation-fence" -> ApplyMutation
      [] OTHER -> Stutter

ShutdownNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> BeginDrain
      [] state.lastEvent = "drain" -> ResponseLoss
      [] state.lastEvent = "response-loss" -> CloseTransport
      [] state.lastEvent = "transport-close" -> JoinTasks
      [] state.lastEvent = "join" -> FinishShutdown
      [] OTHER -> Stutter

CrashPreNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> Crash
      [] state.lastEvent = "crash" -> Restart
      [] OTHER -> Stutter

CrashPostNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> WriteRecord
      [] state.lastEvent = "write" -> Crash
      [] state.lastEvent = "crash" -> Restart
      [] OTHER -> Stutter

StaleLeaseNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> Fence
      [] state.lastEvent = "fence" -> PostInvocationFailure
      [] OTHER -> Stutter

StaleResourceNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> RejectProtectedMutation("resource")
      [] state.lastEvent = "mutation-rejected" -> WriteRecord
      [] state.lastEvent = "write" -> FlushRecord
      [] state.lastEvent = "flush" -> SyncMessage
      [] state.lastEvent = "message-sync" -> SyncIndex
      [] state.lastEvent = "index-sync" -> StrongResponse
      [] OTHER -> Stutter

RejectBindingNext(reason) ==
    CASE state.lastEvent = "init" -> PresentReport(reason, "conn-a")
      [] state.lastEvent = "report" -> RejectBinding(reason)
      [] OTHER -> Stutter

AdvancePipeline ==
    CASE state.stage = "invoked" -> WriteRecord
      [] state.stage = "written" -> FlushRecord
      [] state.stage = "flushed" -> SyncMessage
      [] state.stage = "message-synced" -> SyncIndex
      [] OTHER -> Stutter

StaleRaceNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.disrupted /\ state.stage \in RaceStages -> PostInvocationFailure
      [] ~state.disrupted /\ state.stage = state.raceStage -> InvalidateBinding(state.raceCause)
      [] ~state.disrupted /\ state.stage \in RaceStages -> AdvancePipeline
      [] OTHER -> Stutter

StageStorageFaultNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.storageFault /\ state.stage \in RaceStages -> PostInvocationFailure
      [] ~state.storageFault /\ state.stage = state.faultStage -> StorageFault
      [] ~state.storageFault /\ state.stage \in RaceStages -> AdvancePipeline
      [] OTHER -> Stutter

ReceiptCrashNext ==
    CASE state.lastEvent = "init" -> Prepare
      [] state.lastEvent = "prepare" -> PresentReport("valid", "conn-a")
      [] state.lastEvent = "report" -> BindAuthenticated("conn-a")
      [] state.lastEvent = "bind" -> StartAttempt(1)
      [] state.lastEvent = "start" -> PersistOracleIntent
      [] state.lastEvent = "intent" -> InvokeAppend
      [] state.lastEvent = "invoke" -> WriteRecord
      [] state.lastEvent = "write" -> FlushRecord
      [] state.lastEvent = "flush" -> SyncMessage
      [] state.lastEvent = "message-sync" -> SyncIndex
      [] state.lastEvent = "index-sync" -> StrongResponse
      [] state.lastEvent = "strong-response" -> CrashAfterReceipt
      [] state.lastEvent = "crash" -> Restart
      [] state.lastEvent = "restart" -> MarkRetentionEligible
      [] state.lastEvent = "retention" /\ state.present[1] -> EvictEligible(1)
      [] OTHER -> Stutter

ScenarioNext ==
    CASE state.scenario = "strong-send" -> StrongSendNext
      [] state.scenario = "broker-send" -> BrokerSendNext
      [] state.scenario = "response-loss-retry" -> DuplicateWitnessNext
      [] state.scenario = "pre-failure" -> PreFailureNext
      [] state.scenario = "post-failure" -> PostFailureNext
      [] state.scenario = "lease-expiry" -> LeaseExpiryNext
      [] state.scenario = "resource-mutation" -> ResourceMutationNext("resource")
      [] state.scenario = "config-mutation" -> ResourceMutationNext("config")
      [] state.scenario = "security-mutation" -> ResourceMutationNext("security")
      [] state.scenario = "shutdown" -> ShutdownNext
      [] state.scenario = "crash-pre" -> CrashPreNext
      [] state.scenario = "crash-post" -> CrashPostNext
      [] state.scenario = "storage-fault" -> IF state.lastEvent = "init" THEN StorageFault ELSE Stutter
      [] state.scenario = "bootstrap-fault" -> IF state.lastEvent = "init" THEN BootSyncFailure ELSE Stutter
      [] state.scenario = "stale-lease" -> StaleLeaseNext
      [] state.scenario = "stale-resource" -> StaleResourceNext
      [] state.scenario = "binding-missing-report" -> RejectBindingNext("missing-report")
      [] state.scenario = "binding-auth-failure" -> RejectBindingNext("auth-failure")
      [] state.scenario = "binding-projection-mismatch" -> RejectBindingNext("projection-mismatch")
      [] state.scenario = "binding-stale-epoch" -> RejectBindingNext("stale-epoch")
      [] state.scenario = "stale-race" -> StaleRaceNext
      [] state.scenario = "stage-storage-fault" -> StageStorageFaultNext
      [] state.scenario = "receipt-crash" -> ReceiptCrashNext
      [] state.scenario = "plane-fault" -> IF state.lastEvent = "init" THEN DurableFault ELSE Stutter
      [] OTHER -> Stutter

Next == ScenarioNext \/ UnsafeAction

TypeOK ==
    /\ MaxAttempts = 2
    /\ LeaseLength = 2
    /\ UnsafeMode \in UnsafeModes
    /\ state.scenario \in ScenarioIds
    /\ state.prepared \in BOOLEAN
    /\ state.preparedDigest \in Digests
    /\ state.stage \in Stages
    /\ state.currentAttempt \in 0..MaxAttempts
    /\ state.started \in [Attempts -> BOOLEAN]
    /\ state.attemptDigest \in [Attempts -> Digests]
    /\ state.attemptResourceEpoch \in [Attempts -> 0..2]
    /\ state.attemptConfigEpoch \in [Attempts -> 0..2]
    /\ state.attemptSecurityEpoch \in [Attempts -> 0..2]
    /\ state.attemptCapabilityEpoch \in [Attempts -> 0..2]
    /\ state.attemptBootEpoch \in [Attempts -> 0..2]
    /\ state.attemptConnection \in [Attempts -> Connections]
    /\ state.attemptProjectionDigest \in [Attempts -> Digests]
    /\ state.attemptBoundary \in [Attempts -> Boundaries]
    /\ state.attemptReportKind \in [Attempts -> ReportKinds]
    /\ state.oracleIntent \in [Attempts -> BOOLEAN]
    /\ state.invocationCount \in [Attempts -> 0..2]
    /\ state.appendCount \in [Attempts -> 0..2]
    /\ state.everAppended \in [Attempts -> BOOLEAN]
    /\ state.present \in [Attempts -> BOOLEAN]
    /\ state.evictedAfterEligibility \in [Attempts -> BOOLEAN]
    /\ state.messageSynced \in [Attempts -> BOOLEAN]
    /\ state.indexSynced \in [Attempts -> BOOLEAN]
    /\ state.outcome \in [Attempts -> Outcomes]
    /\ state.terminalSnapshot \in [Attempts -> Outcomes]
    /\ state.strongReceipt \in [Attempts -> BOOLEAN]
    /\ state.receiptDigest \in [Attempts -> Digests]
    /\ state.receiptAttempt \in [Attempts -> 0..MaxAttempts]
    /\ state.receiptBoundary \in [Attempts -> Boundaries]
    /\ state.receiptResourceEpoch \in [Attempts -> 0..2]
    /\ state.receiptConfigEpoch \in [Attempts -> 0..2]
    /\ state.receiptSecurityEpoch \in [Attempts -> 0..2]
    /\ state.receiptCapabilityEpoch \in [Attempts -> 0..2]
    /\ state.receiptBootEpoch \in [Attempts -> 0..2]
    /\ state.receiptProjectionDigest \in [Attempts -> Digests]
    /\ state.receiptConnection \in [Attempts -> Connections]
    /\ state.commitResourceEpoch \in [Attempts -> 0..2]
    /\ state.commitConfigEpoch \in [Attempts -> 0..2]
    /\ state.commitSecurityEpoch \in [Attempts -> 0..2]
    /\ state.commitCapabilityEpoch \in [Attempts -> 0..2]
    /\ state.commitBootEpoch \in [Attempts -> 0..2]
    /\ state.commitConnection \in [Attempts -> Connections]
    /\ state.commitLeaseValid \in [Attempts -> BOOLEAN]
    /\ state.reportKind \in ReportKinds
    /\ state.reportConnection \in Connections
    /\ state.reportTlsVerified \in BOOLEAN
    /\ state.reportAuthenticated \in BOOLEAN
    /\ state.reportComplete \in BOOLEAN
    /\ state.reportResourceEpoch \in 0..2
    /\ state.reportConfigEpoch \in 0..2
    /\ state.reportSecurityEpoch \in 0..2
    /\ state.reportCapabilityEpoch \in 0..2
    /\ state.reportBootEpoch \in 0..2
    /\ state.reportProjectionDigest \in Digests
    /\ state.bindingConnection \in Connections
    /\ state.bindingReportKind \in ReportKinds
    /\ state.bindingTlsVerified \in BOOLEAN
    /\ state.bindingAuthenticated \in BOOLEAN
    /\ state.bindingReportComplete \in BOOLEAN
    /\ state.bindingProjectionMatches \in BOOLEAN
    /\ state.bindingRejected \in BOOLEAN
    /\ state.bindingRejectReason \in BindingRejectReasons
    /\ state.bindingResourceEpoch \in 0..2
    /\ state.bindingConfigEpoch \in 0..2
    /\ state.bindingSecurityEpoch \in 0..2
    /\ state.bindingCapabilityEpoch \in 0..2
    /\ state.bindingBootEpoch \in 0..2
    /\ state.bindingProjectionDigest \in Digests
    /\ state.lastRenewConnection \in Connections
    /\ state.leaseExpiry \in 0..4
    /\ state.now \in 0..4
    /\ state.resourceEpoch \in 1..2
    /\ state.maxResourceEpoch \in 1..2
    /\ state.configEpoch \in 1..2
    /\ state.securityEpoch \in 1..2
    /\ state.capabilityEpoch \in 1..2
    /\ state.bootEpoch \in 1..2
    /\ state.projectionDigest \in Digests
    /\ state.mutationKind \in MutationKinds
    /\ state.mutationStage \in MutationStages
    /\ state.mutationPrincipal \in Principals
    /\ state.pendingEpoch \in 0..2
    /\ state.epochDurable \in BOOLEAN
    /\ state.oldSessionsFenced \in BOOLEAN
    /\ state.partitionState \in PartitionStates
    /\ state.storageFault \in BOOLEAN
    /\ state.bootSyncFailed \in BOOLEAN
    /\ state.lifecycle \in LifecycleStates
    /\ state.startedSetFrozen \in BOOLEAN
    /\ state.transport \in TransportStates
    /\ state.tasksJoined \in BOOLEAN
    /\ state.controlAlive \in BOOLEAN
    /\ state.durableUnavailable \in BOOLEAN
    /\ state.fallbackUsed \in BOOLEAN
    /\ state.responseLost \in BOOLEAN
    /\ state.duplicateObserved \in BOOLEAN
    /\ state.retentionEligible \in BOOLEAN
    /\ state.deliveries \in 0..2
    /\ state.unackedFrontier \in 0..1
    /\ state.crashed \in BOOLEAN
    /\ state.disrupted \in BOOLEAN
    /\ state.raceCause \in RaceCauses
    /\ state.raceStage \in RaceStages
    /\ state.faultStage \in RaceStages
    /\ state.eventAttempt \in 0..MaxAttempts
    /\ state.eventPartition = 1
    /\ state.eventResourceEpoch \in 0..2
    /\ state.eventOwnerConnection \in Connections
    /\ state.eventRaceCause \in (Events \cup RaceCauses \cup ReportKinds)
    /\ state.eventPipelineStage \in Stages
    /\ state.lastEvent \in Events

AtMostOneAppendPerAttempt ==
    \A a \in Attempts:
        /\ state.invocationCount[a] <= 1
        /\ state.appendCount[a] <= 1
        /\ state.appendCount[a] <= state.invocationCount[a]

OracleIntentPrecedesAppend ==
    \A a \in Attempts: state.invocationCount[a] > 0 => state.oracleIntent[a]

StableIdentity ==
    \A a \in Attempts: state.started[a] => state.attemptDigest[a] = state.preparedDigest

NoOperationWithoutValidBinding ==
    \A a \in Attempts:
        state.started[a] =>
            /\ state.attemptConnection[a] # "none"
            /\ state.attemptResourceEpoch[a] \in 1..2
            /\ state.attemptConfigEpoch[a] \in 1..2
            /\ state.attemptSecurityEpoch[a] \in 1..2
            /\ state.attemptCapabilityEpoch[a] \in 1..2
            /\ state.attemptBootEpoch[a] \in 1..2
            /\ state.attemptProjectionDigest[a] = 101
            /\ state.attemptBoundary[a] # 0
            /\ state.attemptReportKind[a] = "valid"

BindingCreatedOnlyFromAuthenticatedReport ==
    state.bindingConnection # "none" =>
        /\ state.bindingReportKind = "valid"
        /\ state.reportKind = "valid"
        /\ state.reportConnection = state.bindingConnection
        /\ state.reportTlsVerified = state.bindingTlsVerified
        /\ state.reportAuthenticated = state.bindingAuthenticated
        /\ state.reportComplete = state.bindingReportComplete
        /\ state.reportResourceEpoch = state.bindingResourceEpoch
        /\ state.reportConfigEpoch = state.bindingConfigEpoch
        /\ state.reportSecurityEpoch = state.bindingSecurityEpoch
        /\ state.reportCapabilityEpoch = state.bindingCapabilityEpoch
        /\ state.reportBootEpoch = state.bindingBootEpoch
        /\ state.reportProjectionDigest = state.bindingProjectionDigest
        /\ state.bindingTlsVerified
        /\ state.bindingAuthenticated
        /\ state.bindingReportComplete
        /\ state.bindingProjectionMatches
        /\ state.bindingResourceEpoch = state.resourceEpoch
        /\ state.bindingConfigEpoch = state.configEpoch
        /\ state.bindingSecurityEpoch = state.securityEpoch
        /\ state.bindingCapabilityEpoch = state.capabilityEpoch
        /\ state.bindingBootEpoch = state.bootEpoch
        /\ state.bindingProjectionDigest = state.projectionDigest

NotSubmittedImpliesNoAppend ==
    \A a \in Attempts:
        state.outcome[a] = "not-submitted" =>
            /\ state.invocationCount[a] = 0
            /\ state.appendCount[a] = 0

IndeterminateAllowsZeroOrOneRecord ==
    \A a \in Attempts:
        state.outcome[a] = "indeterminate" =>
            /\ state.appendCount[a] \in 0..1
            /\ (state.appendCount[a] = 0 \/
                /\ state.everAppended[a]
                /\ state.present[a]
                /\ state.attemptDigest[a] = state.preparedDigest)

ExactReceipt ==
    \A a \in Attempts:
        state.strongReceipt[a] =>
            /\ state.outcome[a] = "os-synced"
            /\ state.appendCount[a] = 1
            /\ state.everAppended[a]
            /\ state.messageSynced[a]
            /\ state.indexSynced[a]
            /\ state.receiptDigest[a] = state.attemptDigest[a]
            /\ state.receiptAttempt[a] = a
            /\ state.receiptBoundary[a] = state.attemptBoundary[a]
            /\ state.receiptResourceEpoch[a] = state.attemptResourceEpoch[a]
            /\ state.receiptConfigEpoch[a] = state.attemptConfigEpoch[a]
            /\ state.receiptSecurityEpoch[a] = state.attemptSecurityEpoch[a]
            /\ state.receiptCapabilityEpoch[a] = state.attemptCapabilityEpoch[a]
            /\ state.receiptBootEpoch[a] = state.attemptBootEpoch[a]
            /\ state.receiptProjectionDigest[a] = state.attemptProjectionDigest[a]
            /\ state.receiptConnection[a] = state.attemptConnection[a]
            /\ state.commitLeaseValid[a]
            /\ (state.present[a] \/ state.evictedAfterEligibility[a])

NoStaleSuccess ==
    \A a \in Attempts:
        Accepted(state.outcome[a]) =>
            /\ state.commitLeaseValid[a]
            /\ state.commitResourceEpoch[a] = state.attemptResourceEpoch[a]
            /\ state.commitConfigEpoch[a] = state.attemptConfigEpoch[a]
            /\ state.commitSecurityEpoch[a] = state.attemptSecurityEpoch[a]
            /\ state.commitCapabilityEpoch[a] = state.attemptCapabilityEpoch[a]
            /\ state.commitBootEpoch[a] = state.attemptBootEpoch[a]
            /\ state.commitConnection[a] = state.attemptConnection[a]

BrokerAcceptedNeverPromoted ==
    \A a \in Attempts: state.outcome[a] = "broker-accepted" => ~state.strongReceipt[a]

StrongReceiptPresentBeforeEligibility ==
    \A a \in Attempts:
        state.strongReceipt[a] /\ ~state.retentionEligible => state.present[a]

OutcomePreservation ==
    \A a \in Attempts: state.outcome[a] = state.terminalSnapshot[a]

DuplicateDoesNotAdvanceUnackedFrontier ==
    state.duplicateObserved =>
        /\ state.deliveries = 2
        /\ state.unackedFrontier = 0
        /\ state.attemptDigest[1] = state.attemptDigest[2]

EpochDurableBeforeMutation ==
    state.mutationStage = "applied" =>
        /\ state.epochDurable
        /\ state.oldSessionsFenced
        /\ state.mutationPrincipal = "management"

RuntimeCannotMutateResources ==
    state.mutationStage # "stable" => state.mutationPrincipal = "management"

ProtectedResourceMutationRequiresNoLease ==
    state.mutationKind \in {"resource", "config"} /\ state.mutationStage # "stable" =>
        state.bindingConnection = "none"

ResourceEpochNeverReused ==
    state.resourceEpoch = state.maxResourceEpoch

LeaseValidOnlyOnBoundConnection ==
    state.lastRenewConnection = "none" \/ state.lastRenewConnection = state.bindingConnection

StorageFaultStopsPartition ==
    state.storageFault => state.partitionState # "ready"

NoReadyAfterOpenSyncFailure ==
    state.bootSyncFailed => state.partitionState # "ready"

OwnedTransportTerminates ==
    state.lifecycle = "closed" =>
        /\ state.startedSetFrozen
        /\ state.transport = "closed"
        /\ state.tasksJoined

PlaneIsolation == state.durableUnavailable => state.controlAlive

NoDurableFallback == state.durableUnavailable => ~state.fallbackUsed

PipelineEvents == {
    "start", "intent", "invoke", "write", "flush", "message-sync",
    "index-sync", "broker-response", "strong-response", "pre-failure",
    "post-failure", "response-loss", "duplicate-delivery", "retention", "crash"
}

StateContextEvents == {
    "prepare", "renew", "time", "mutation-plan", "mutation-rejected",
    "mutation-fence", "mutation-apply", "storage-fault", "boot-sync-fault",
    "durable-fault", "drain", "transport-close", "join", "shutdown"
}

BindingClearEvents == {"expire", "fence"}

LabelCauseEvents == Events \ {
    "init", "report", "binding-rejected", "binding-invalidated", "unsafe"
}

CorrelatedEvents ==
    /\ state.eventPartition = 1
    /\ (state.lastEvent \in LabelCauseEvents => state.eventRaceCause = state.lastEvent)
    /\ (state.lastEvent = "init" =>
        /\ state.eventAttempt = 0
        /\ state.eventResourceEpoch = state.resourceEpoch
        /\ state.eventOwnerConnection = "none"
        /\ state.eventRaceCause = "init"
        /\ state.eventPipelineStage = "idle")
    /\ (state.lastEvent \in StateContextEvents =>
        /\ state.eventAttempt = state.currentAttempt
        /\ state.eventResourceEpoch = state.resourceEpoch
        /\ state.eventOwnerConnection = state.bindingConnection
        /\ state.eventPipelineStage = state.stage)
    /\ (state.lastEvent \in BindingClearEvents =>
        /\ state.eventAttempt = state.currentAttempt
        /\ state.eventResourceEpoch = state.resourceEpoch
        /\ state.eventOwnerConnection = state.reportConnection
        /\ state.eventPipelineStage = state.stage)
    /\ (state.lastEvent \in PipelineEvents =>
        IF state.eventAttempt \in Attempts
        THEN
            /\ state.eventResourceEpoch = state.attemptResourceEpoch[state.eventAttempt]
            /\ state.eventOwnerConnection = state.attemptConnection[state.eventAttempt]
        ELSE FALSE)
    /\ (state.lastEvent = "report" =>
        /\ state.eventAttempt = 0
        /\ state.eventResourceEpoch = state.reportResourceEpoch
        /\ state.eventOwnerConnection = state.reportConnection
        /\ state.eventRaceCause = state.reportKind
        /\ state.eventPipelineStage = state.stage)
    /\ (state.lastEvent = "bind" =>
        /\ state.eventAttempt = 0
        /\ state.eventResourceEpoch = state.bindingResourceEpoch
        /\ state.eventOwnerConnection = state.bindingConnection
        /\ state.eventPipelineStage = state.stage)
    /\ (state.lastEvent = "binding-rejected" =>
        /\ state.bindingRejected
        /\ state.bindingConnection = "none"
        /\ state.eventAttempt = 0
        /\ state.eventResourceEpoch = state.reportResourceEpoch
        /\ state.eventOwnerConnection = state.reportConnection
        /\ state.eventRaceCause = state.bindingRejectReason
        /\ state.eventPipelineStage = state.stage)
    /\ (state.lastEvent = "binding-invalidated" =>
        /\ state.disrupted
        /\ state.bindingConnection = "none"
        /\ state.eventAttempt = state.currentAttempt
        /\ state.eventResourceEpoch = state.attemptResourceEpoch[state.currentAttempt]
        /\ state.eventOwnerConnection = state.attemptConnection[state.currentAttempt]
        /\ state.eventRaceCause = state.raceCause
        /\ state.eventPipelineStage = state.raceStage)
    /\ (state.lastEvent = "epoch-persist" =>
        /\ state.eventAttempt = state.currentAttempt
        /\ state.eventResourceEpoch = state.resourceEpoch
        /\ state.eventOwnerConnection =
            IF state.mutationKind = "security" THEN state.reportConnection
            ELSE state.bindingConnection
        /\ state.eventPipelineStage = state.stage)
    /\ (state.lastEvent = "restart" =>
        IF state.currentAttempt \in Attempts
        THEN
            /\ state.eventAttempt = state.currentAttempt
            /\ state.eventResourceEpoch = state.resourceEpoch
            /\ (state.eventOwnerConnection = state.bindingConnection
                \/ state.eventOwnerConnection = state.attemptConnection[state.currentAttempt])
            /\ state.eventPipelineStage = state.stage
        ELSE FALSE)
    /\ (state.lastEvent = "start" => state.eventPipelineStage = "started")
    /\ (state.lastEvent = "intent" => state.eventPipelineStage = "intent")
    /\ (state.lastEvent = "invoke" => state.eventPipelineStage = "invoked")
    /\ (state.lastEvent = "write" => state.eventPipelineStage = "written")
    /\ (state.lastEvent = "flush" => state.eventPipelineStage = "flushed")
    /\ (state.lastEvent = "message-sync" => state.eventPipelineStage = "message-synced")
    /\ (state.lastEvent = "index-sync" => state.eventPipelineStage = "index-synced")
    /\ (state.lastEvent \in {"broker-response", "strong-response", "pre-failure", "post-failure", "response-loss", "crash"} =>
        state.eventPipelineStage = "terminal")
    /\ (state.lastEvent \in {"duplicate-delivery", "retention"} =>
        state.eventPipelineStage = state.stage)
    /\ (state.lastEvent = "strong-response" =>
        IF state.eventAttempt \in Attempts THEN state.strongReceipt[state.eventAttempt] ELSE FALSE)
    /\ (state.lastEvent = "duplicate-delivery" => state.duplicateObserved /\ state.deliveries = 2)
    /\ (state.lastEvent = "epoch-persist" => state.epochDurable)
    /\ (state.lastEvent = "mutation-fence" => state.oldSessionsFenced)
    /\ (state.lastEvent = "mutation-apply" => state.mutationStage = "applied")
    /\ (state.lastEvent = "storage-fault" => state.storageFault /\ state.durableUnavailable)
    /\ (state.lastEvent = "boot-sync-fault" => state.bootSyncFailed /\ state.durableUnavailable)
    /\ (state.lastEvent = "durable-fault" => state.durableUnavailable)
    /\ (state.lastEvent = "restart" => state.bootEpoch = 2 /\ state.bindingConnection = "none")

DuplicateWitnessAbsent == ~state.duplicateObserved

=============================================================================
