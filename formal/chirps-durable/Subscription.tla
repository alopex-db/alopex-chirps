--------------------------- MODULE Subscription ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Int;
    MaxOffset,
    \* @type: Int;
    MaxOwnerEpoch,
    \* @type: Int;
    MaxDeliveryAttempts,
    \* @type: Int;
    LivenessBudget,
    \* @type: Str;
    UnsafeMode

Offsets == 0..MaxOffset
OffsetPoints == 0..(MaxOffset + 1)
OwnerEpochs == 0..MaxOwnerEpoch
DeliveryAttempts == 1..MaxDeliveryAttempts
SelectionModes == {"earliest", "latest", "exact"}
CreationStages == {
    "absent", "observed", "resolved", "written", "file-synced",
    "renamed", "unknown", "recovered-old", "recovered-new", "created",
    "rejected"
}
CreationOutcomes == {"none", "not-committed", "unknown", "created"}
OwnerStages == {"none", "genesis", "installing", "committed", "unknown"}
PartitionStates == {
    "inactive", "active", "in-flight", "redelivery", "tail", "gap",
    "checkpoint-indeterminate", "recovery-required", "namespace-mismatch",
    "faulted", "offset-exhausted"
}
PollKinds == {
    "none", "record", "tail", "gap", "conflict", "incoherent",
    "read-failure", "misroute", "corrupt", "identity-conflict"
}
HandleStates == {
    "none", "open", "ack-installing", "ack-retryable",
    "terminal-committed", "terminal-unknown", "released", "timed-out",
    "shutdown-fenced"
}
CheckpointStages == {
    "none", "prewrite", "written", "file-synced", "renamed", "dir-synced"
}
CheckpointOutcomes == {"none", "not-committed", "unknown", "committed"}
RecoveryChoices == {"none", "old", "new"}
MessageIds == {0, 101}
Digests == {0, 201, 202}
Connections == {"none", "owner-a", "owner-b"}

ScenarioIds == {
    "creation-matrix", "creation-invalid-below", "creation-invalid-above",
    "creation-known-old", "creation-unknown-old", "creation-unknown-new",
    "owner-reopen", "poll-record-ack", "checkpoint-known-old-retry",
    "checkpoint-unknown-old", "checkpoint-unknown-new", "nack-redelivery",
    "checkpoint-zero-commit", "checkpoint-present-unknown-old",
    "checkpoint-present-unknown-new",
    "timeout-redelivery", "shutdown-fence", "tail", "retention-gap",
    "checkpoint-conflict", "incoherent-poll", "read-failure", "misroute",
    "corrupt-record", "identity-conflict", "advisory-mirror",
    "effect-crash-redelivery", "duplicate-redelivery", "resource-change",
    "config-change", "offset-exhaustion", "invalid-recovery", "liveness"
}

UnsafeModes == {
    "none", "poll-mixed", "misroute-delivered", "identity-conflict-delivered",
    "delivery-before-identity",
    "resolved-start-change", "owner-fork", "double-inflight", "handle-substitution",
    "old-handle-ack", "stale-handle-ack", "poll-before-checkpoint",
    "known-old-terminal", "unknown-keeps-active", "recovery-neither",
    "invalid-state-active", "advisory-advances", "effect-not-redelivered",
    "invalid-exact-commits", "unknown-creation-polls", "double-handle-winner",
    "offset-wrap", "tail-incoherent", "selected-skip", "namespace-unfenced",
    "retention-as-tail", "corrupt-delivered", "read-failure-advances",
    "invalid-prefix-active", "early-creation-success", "checkpoint-outcome-mismatch",
    "early-checkpoint-success", "poll-owner-change", "delivery-without-reservation",
    "creation-outcome-mismatch", "duplicate-advances-frontier", "liveness-stalled",
    "liveness-weak-selected", "liveness-no-broker", "liveness-no-subscription",
    "liveness-no-retention", "liveness-unfair-poll", "liveness-unfair-redelivery",
    "liveness-offset-before-e0", "liveness-no-recovery", "liveness-corrupt",
    "liveness-no-capacity", "liveness-owner-change",
    "liveness-resource-change", "liveness-inbox-change"
}

Events == {
    "init", "creation-observed", "creation-resolved", "creation-written",
    "creation-file-synced", "creation-renamed", "creation-created",
    "creation-rejected", "creation-known-old", "creation-unknown",
    "creation-recovered-old", "creation-recovered-new", "owner-installing",
    "owner-committed", "poll-record", "poll-tail", "poll-gap",
    "poll-conflict", "poll-incoherent", "poll-read-failure", "poll-misroute",
    "poll-corrupt", "poll-identity-conflict", "identity-persisted", "delivery",
    "ack", "checkpoint-written", "checkpoint-file-synced",
    "checkpoint-renamed", "checkpoint-dir-synced", "checkpoint-committed",
    "checkpoint-known-old", "checkpoint-loaded",
    "ack-retry", "checkpoint-unknown", "checkpoint-recovered-old",
    "checkpoint-recovered-new", "nack", "timeout", "shutdown-fence",
    "redelivery", "duplicate-observed", "retention-advanced", "advisory-updated", "effect-applied",
    "effect-crash", "resource-changed", "config-changed", "offset-exhausted",
    "recovery-fault", "liveness-start", "liveness-poll",
    "liveness-identity", "liveness-delivery", "unsafe"
}

EmptyWinnerCounts == [a \in DeliveryAttempts |-> 0]

VARIABLES
    \* @type: {
    \* scenario: Str, selection: Str, creationOldest: Int, creationH: Int,
    \* requestedExact: Int, resolvedStart: Int, resolvedStartSnapshot: Int,
    \* resolved: Bool, capturedOldest: Int, capturedH: Int,
    \* capturedResourceEpoch: Int, creationStage: Str, creationOutcome: Str,
    \* pendingCreationComplete: Bool, unitVisible: Bool, manifestValid: Bool,
    \* creationFileSynced: Bool, creationRenamed: Bool, creationDirSynced: Bool,
    \* sameDirectoryOnly: Bool, selectionOverwriteAttempted: Bool,
    \* lockHeld: Bool, ownerStage: Str, ownerEpoch: Int, ownerCandidate: Int,
    \* ownerRecordValid: Bool, ownerLinked: Bool, ownerFileSynced: Bool,
    \* ownerDirSynced: Bool, ownerConnection: Str, active: Bool,
    \* partitionState: Str, currentResourceEpoch: Int, currentConfigEpoch: Int,
    \* manifestResourceEpoch: Int, manifestConfigEpoch: Int,
    \* checkpointPresent: Bool, checkpoint: Int, checkpointBefore: Int,
    \* checkpointPresentBefore: Bool, checkpointBeforeOwnerEpoch: Int,
    \* checkpointBeforeAttempt: Int, checkpointBeforeOffset: Int,
    \* pollAllowed: Bool, pollReserved: Bool, pollOwnerEpoch: Int,
    \* pollKind: Str, pollExpected: Int, pollPreviousH: Int,
    \* pollResourceEpoch: Int, pollH: Int, pollOldest: Int,
    \* pollRecordCount: Int, pollRecordOffset: Int, pollRecordId: Int,
    \* pollRecordDigest: Int, pollTargetOk: Bool, pollGenerationOk: Bool,
    \* pollPartitionOk: Bool, pollEnvelopeValid: Bool, pollReadFailed: Bool,
    \* lastH: Int, identityPresent: Bool, identityDurable: Bool,
    \* identityId: Int, identityDigest: Int, identityConflict: Bool,
    \* identityCheckpointed: Bool, inFlight: Bool, inFlightCount: Int,
    \* deliveryOffset: Int, deliveryId: Int, deliveryDigest: Int,
    \* deliveryAttempt: Int, handleState: Str, handleOffset: Int,
    \* handleId: Int, handleDigest: Int, handleAttempt: Int,
    \* handleOwnerEpoch: Int, handleGeneration: Int, previousHandleAttempt: Int,
    \* winnerCounts: Int -> Int, terminalWinner: Str,
    \* checkpointStage: Str, checkpointCandidate: Int,
    \* checkpointFileSynced: Bool, checkpointDirSynced: Bool,
    \* checkpointOutcome: Str, checkpointRecovery: Str,
    \* lastCheckpointAttempt: Int, lastCheckpointOwnerEpoch: Int,
    \* lastCheckpointOffset: Int, journalSequence: Int, appliedThrough: Int,
    \* journalValid: Bool, sequenceContiguous: Bool, advisoryOffset: Int,
    \* advisoryCheckpointSnapshot: Int, advisoryCheckpointPresentSnapshot: Bool,
    \* applicationEffect: Bool,
    \* effectCrash: Bool, redeliveryObserved: Bool, duplicateObserved: Bool,
    \* unackedFrontier: Int, offsetExhausted: Bool,
    \* liveTracked: Bool, liveSelectedStrong: Bool, liveOffsetSelected: Bool,
    \* liveBrokerAvailable: Bool, liveRecoveryAvailable: Bool,
    \* liveSubscriptionAvailable: Bool, liveRetentionAvailable: Bool,
    \* liveFairPoll: Bool, liveFairRedelivery: Bool, liveValidState: Bool,
    \* liveCapacityAvailable: Bool, liveSameOwner: Bool,
    \* liveSameResource: Bool, liveSameInbox: Bool, liveBudget: Int,
    \* liveDelivered: Bool, liveCheckpointed: Bool, lastEvent: Str};
    state

ExpectedOffset ==
    IF state.checkpointPresent THEN state.checkpoint + 1 ELSE state.resolvedStart

CreationInputValid ==
    /\ state.creationOldest <= state.creationH
    /\ (state.creationOldest = state.creationH \/ state.creationOldest < state.creationH)
    /\ (state.selection # "exact"
        \/ (state.creationOldest <= state.requestedExact
            /\ state.requestedExact <= state.creationH))

CreationResolvedValue ==
    CASE state.selection = "earliest" -> state.creationOldest
      [] state.selection = "latest" -> state.creationH
      [] OTHER -> state.requestedExact

AllLivenessAssumptions ==
    /\ state.liveSelectedStrong
    /\ state.liveOffsetSelected
    /\ state.liveBrokerAvailable
    /\ state.liveRecoveryAvailable
    /\ state.liveSubscriptionAvailable
    /\ state.liveRetentionAvailable
    /\ state.liveFairPoll
    /\ state.liveFairRedelivery
    /\ state.liveValidState
    /\ state.liveCapacityAvailable
    /\ state.liveSameOwner
    /\ state.liveSameResource
    /\ state.liveSameInbox

Init ==
    \E selected \in ScenarioIds:
    \E selectedMode \in
        IF selected = "creation-matrix" THEN SelectionModes
        ELSE IF selected \in {"creation-invalid-below", "creation-invalid-above"}
        THEN {"exact"}
        ELSE IF selected = "tail" THEN {"latest"}
        ELSE {"earliest"}:
    \E selectedEmpty \in IF selected = "creation-matrix" THEN BOOLEAN ELSE {FALSE}:
    LET oldest == IF selected = "creation-invalid-below" THEN 2
                  ELSE IF selected = "checkpoint-zero-commit" THEN 0
                  ELSE 1 IN
    LET high == IF selected = "creation-invalid-above" THEN 2
                ELSE IF selectedEmpty THEN 1 ELSE 3 IN
    LET exact == IF selected = "creation-invalid-below" THEN 1
                 ELSE IF selected = "creation-invalid-above" THEN 3
                 ELSE IF selectedEmpty THEN 1 ELSE 2 IN
    state = [
        scenario |-> selected,
        selection |-> selectedMode,
        creationOldest |-> oldest,
        creationH |-> high,
        requestedExact |-> exact,
        resolvedStart |-> 0,
        resolvedStartSnapshot |-> 0,
        resolved |-> FALSE,
        capturedOldest |-> 0,
        capturedH |-> 0,
        capturedResourceEpoch |-> 0,
        creationStage |-> "absent",
        creationOutcome |-> "none",
        pendingCreationComplete |-> FALSE,
        unitVisible |-> FALSE,
        manifestValid |-> FALSE,
        creationFileSynced |-> FALSE,
        creationRenamed |-> FALSE,
        creationDirSynced |-> FALSE,
        sameDirectoryOnly |-> FALSE,
        selectionOverwriteAttempted |-> FALSE,
        lockHeld |-> TRUE,
        ownerStage |-> "none",
        ownerEpoch |-> 0,
        ownerCandidate |-> 0,
        ownerRecordValid |-> FALSE,
        ownerLinked |-> FALSE,
        ownerFileSynced |-> FALSE,
        ownerDirSynced |-> FALSE,
        ownerConnection |-> "none",
        active |-> FALSE,
        partitionState |-> "inactive",
        currentResourceEpoch |-> 1,
        currentConfigEpoch |-> 1,
        manifestResourceEpoch |-> 0,
        manifestConfigEpoch |-> 0,
        checkpointPresent |-> FALSE,
        checkpoint |-> 0,
        checkpointBefore |-> 0,
        checkpointPresentBefore |-> FALSE,
        checkpointBeforeOwnerEpoch |-> 0,
        checkpointBeforeAttempt |-> 0,
        checkpointBeforeOffset |-> 0,
        pollAllowed |-> FALSE,
        pollReserved |-> FALSE,
        pollOwnerEpoch |-> 0,
        pollKind |-> "none",
        pollExpected |-> 0,
        pollPreviousH |-> 0,
        pollResourceEpoch |-> 0,
        pollH |-> 0,
        pollOldest |-> 0,
        pollRecordCount |-> 0,
        pollRecordOffset |-> 0,
        pollRecordId |-> 0,
        pollRecordDigest |-> 0,
        pollTargetOk |-> TRUE,
        pollGenerationOk |-> TRUE,
        pollPartitionOk |-> TRUE,
        pollEnvelopeValid |-> TRUE,
        pollReadFailed |-> FALSE,
        lastH |-> 0,
        identityPresent |-> FALSE,
        identityDurable |-> FALSE,
        identityId |-> 0,
        identityDigest |-> 0,
        identityConflict |-> FALSE,
        identityCheckpointed |-> FALSE,
        inFlight |-> FALSE,
        inFlightCount |-> 0,
        deliveryOffset |-> 0,
        deliveryId |-> 0,
        deliveryDigest |-> 0,
        deliveryAttempt |-> 0,
        handleState |-> "none",
        handleOffset |-> 0,
        handleId |-> 0,
        handleDigest |-> 0,
        handleAttempt |-> 0,
        handleOwnerEpoch |-> 0,
        handleGeneration |-> 0,
        previousHandleAttempt |-> 0,
        winnerCounts |-> EmptyWinnerCounts,
        terminalWinner |-> "none",
        checkpointStage |-> "none",
        checkpointCandidate |-> 0,
        checkpointFileSynced |-> FALSE,
        checkpointDirSynced |-> FALSE,
        checkpointOutcome |-> "none",
        checkpointRecovery |-> "none",
        lastCheckpointAttempt |-> 0,
        lastCheckpointOwnerEpoch |-> 0,
        lastCheckpointOffset |-> 0,
        journalSequence |-> 0,
        appliedThrough |-> 0,
        journalValid |-> TRUE,
        sequenceContiguous |-> TRUE,
        advisoryOffset |-> 0,
        advisoryCheckpointSnapshot |-> 0,
        advisoryCheckpointPresentSnapshot |-> FALSE,
        applicationEffect |-> FALSE,
        effectCrash |-> FALSE,
        redeliveryObserved |-> FALSE,
        duplicateObserved |-> FALSE,
        unackedFrontier |-> 0,
        offsetExhausted |-> FALSE,
        liveTracked |-> FALSE,
        liveSelectedStrong |-> TRUE,
        liveOffsetSelected |-> TRUE,
        liveBrokerAvailable |-> TRUE,
        liveRecoveryAvailable |-> TRUE,
        liveSubscriptionAvailable |-> TRUE,
        liveRetentionAvailable |-> TRUE,
        liveFairPoll |-> TRUE,
        liveFairRedelivery |-> TRUE,
        liveValidState |-> TRUE,
        liveCapacityAvailable |-> TRUE,
        liveSameOwner |-> TRUE,
        liveSameResource |-> TRUE,
        liveSameInbox |-> TRUE,
        liveBudget |-> LivenessBudget,
        liveDelivered |-> FALSE,
        liveCheckpointed |-> FALSE,
        lastEvent |-> "init"
    ]

ObserveCreation ==
    /\ state.creationStage = "absent"
    /\ state.lockHeld
    /\ state' = [state EXCEPT
        !.creationStage = "observed",
        !.capturedOldest = state.creationOldest,
        !.capturedH = state.creationH,
        !.capturedResourceEpoch = state.currentResourceEpoch,
        !.lastH = state.creationH,
        !.lastEvent = "creation-observed"]

ResolveInitialPosition ==
    /\ state.creationStage = "observed"
    /\ CreationInputValid
    /\ state' = [state EXCEPT
        !.resolved = TRUE,
        !.resolvedStart = CreationResolvedValue,
        !.resolvedStartSnapshot = CreationResolvedValue,
        !.unackedFrontier = CreationResolvedValue,
        !.creationStage = "resolved",
        !.lastEvent = "creation-resolved"]

RejectInvalidExact ==
    /\ state.creationStage = "observed"
    /\ state.selection = "exact"
    /\ (state.requestedExact < state.creationOldest \/ state.requestedExact > state.creationH)
    /\ state' = [state EXCEPT
        !.creationStage = "rejected",
        !.creationOutcome = "not-committed",
        !.lastEvent = "creation-rejected"]

WriteCreationUnit ==
    /\ state.creationStage = "resolved"
    /\ state' = [state EXCEPT
        !.creationStage = "written",
        !.lastEvent = "creation-written"]

SyncCreationFile ==
    /\ state.creationStage = "written"
    /\ state' = [state EXCEPT
        !.creationStage = "file-synced",
        !.creationFileSynced = TRUE,
        !.lastEvent = "creation-file-synced"]

RenameCreationUnit ==
    /\ state.creationStage = "file-synced"
    /\ state.creationFileSynced
    /\ state' = [state EXCEPT
        !.creationStage = "renamed",
        !.creationRenamed = TRUE,
        !.lastEvent = "creation-renamed"]

SyncCreationDirectory ==
    /\ state.creationStage = "renamed"
    /\ state.creationFileSynced
    /\ state.creationRenamed
    /\ state' = [state EXCEPT
        !.creationStage = "created",
        !.creationOutcome = "created",
        !.unitVisible = TRUE,
        !.manifestValid = TRUE,
        !.creationDirSynced = TRUE,
        !.manifestResourceEpoch = state.capturedResourceEpoch,
        !.manifestConfigEpoch = state.currentConfigEpoch,
        !.ownerStage = "genesis",
        !.ownerEpoch = 1,
        !.ownerRecordValid = TRUE,
        !.ownerLinked = TRUE,
        !.ownerFileSynced = TRUE,
        !.ownerDirSynced = TRUE,
        !.ownerConnection = "owner-a",
        !.active = TRUE,
        !.partitionState = "active",
        !.pollAllowed = TRUE,
        !.lastEvent = "creation-created"]

CreationKnownOldFailure ==
    /\ state.creationStage = "resolved"
    /\ state' = [state EXCEPT
        !.creationStage = "absent",
        !.creationOutcome = "not-committed",
        !.lastEvent = "creation-known-old"]

CreationUnknownFailure(choice) ==
    /\ choice \in {"old", "new"}
    /\ state.creationStage = "written"
    /\ state' = [state EXCEPT
        !.creationStage = "unknown",
        !.creationOutcome = "unknown",
        !.pendingCreationComplete = choice = "new",
        !.sameDirectoryOnly = TRUE,
        !.lastEvent = "creation-unknown"]

RecoverCreationOld ==
    /\ state.creationStage = "unknown"
    /\ ~state.pendingCreationComplete
    /\ state.sameDirectoryOnly
    /\ state' = [state EXCEPT
        !.creationStage = "recovered-old",
        !.unitVisible = FALSE,
        !.manifestValid = FALSE,
        !.ownerEpoch = 0,
        !.ownerRecordValid = FALSE,
        !.ownerLinked = FALSE,
        !.active = FALSE,
        !.partitionState = "inactive",
        !.pollAllowed = FALSE,
        !.lastEvent = "creation-recovered-old"]

RecoverCreationNew ==
    /\ state.creationStage = "unknown"
    /\ state.pendingCreationComplete
    /\ state.sameDirectoryOnly
    /\ state' = [state EXCEPT
        !.creationStage = "recovered-new",
        !.unitVisible = TRUE,
        !.manifestValid = TRUE,
        !.creationFileSynced = TRUE,
        !.creationRenamed = TRUE,
        !.creationDirSynced = TRUE,
        !.manifestResourceEpoch = state.capturedResourceEpoch,
        !.manifestConfigEpoch = state.currentConfigEpoch,
        !.ownerStage = "genesis",
        !.ownerEpoch = 1,
        !.ownerRecordValid = TRUE,
        !.ownerLinked = TRUE,
        !.ownerFileSynced = TRUE,
        !.ownerDirSynced = TRUE,
        !.ownerConnection = "owner-a",
        !.active = TRUE,
        !.partitionState = "active",
        !.pollAllowed = TRUE,
        !.lastEvent = "creation-recovered-new"]

BeginOwnerInstall ==
    /\ state.creationStage \in {"created", "recovered-new"}
    /\ state.ownerEpoch = 1
    /\ state.ownerRecordValid
    /\ state.lockHeld
    /\ ~state.pollReserved
    /\ ~state.inFlight
    /\ state.checkpointStage = "none"
    /\ state' = [state EXCEPT
        !.ownerStage = "installing",
        !.ownerCandidate = 2,
        !.active = FALSE,
        !.partitionState = "inactive",
        !.pollAllowed = FALSE,
        !.lastEvent = "owner-installing"]

CommitOwnerInstall ==
    /\ state.ownerStage = "installing"
    /\ state.ownerCandidate = state.ownerEpoch + 1
    /\ state.ownerCandidate <= MaxOwnerEpoch
    /\ state' = [state EXCEPT
        !.ownerStage = "committed",
        !.ownerEpoch = state.ownerCandidate,
        !.ownerRecordValid = TRUE,
        !.ownerLinked = TRUE,
        !.ownerFileSynced = TRUE,
        !.ownerDirSynced = TRUE,
        !.ownerConnection = "owner-b",
        !.active = TRUE,
        !.partitionState = "active",
        !.pollAllowed = TRUE,
        !.lastEvent = "owner-committed"]

PollValidRecord ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ ~state.inFlight
    /\ expected < state.creationH
    /\ state.creationOldest <= expected
    /\ state' = [state EXCEPT
        !.pollKind = "record",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 1,
        !.pollRecordOffset = expected,
        !.pollRecordId = 101,
        !.pollRecordDigest = 201,
        !.pollTargetOk = TRUE,
        !.pollGenerationOk = TRUE,
        !.pollPartitionOk = TRUE,
        !.pollEnvelopeValid = TRUE,
        !.pollReadFailed = FALSE,
        !.lastH = state.creationH,
        !.pollAllowed = FALSE,
        !.pollReserved = TRUE,
        !.pollOwnerEpoch = state.ownerEpoch,
        !.lastEvent = "poll-record"]

PollTail ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ expected = state.creationH
    /\ state.creationOldest <= expected
    /\ state' = [state EXCEPT
        !.pollKind = "tail",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 0,
        !.pollRecordOffset = 0,
        !.pollReadFailed = FALSE,
        !.lastH = state.creationH,
        !.partitionState = "tail",
        !.lastEvent = "poll-tail"]

AdvanceRetention ==
    /\ state.active
    /\ state.creationOldest = 1
    /\ ExpectedOffset = 1
    /\ state' = [state EXCEPT
        !.creationOldest = 2,
        !.lastEvent = "retention-advanced"]

PollRetentionGap ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ expected < state.creationOldest
    /\ state' = [state EXCEPT
        !.pollKind = "gap",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 0,
        !.partitionState = "gap",
        !.pollAllowed = FALSE,
        !.lastEvent = "poll-gap"]

LoadRecoveredCheckpointZero ==
    /\ state.active
    /\ state.pollAllowed
    /\ state.ownerEpoch = 1
    /\ state' = [state EXCEPT
        !.checkpointPresent = TRUE,
        !.checkpoint = 0,
        !.checkpointBefore = 0,
        !.checkpointPresentBefore = TRUE,
        !.checkpointBeforeOwnerEpoch = 1,
        !.checkpointBeforeAttempt = 1,
        !.checkpointBeforeOffset = 0,
        !.lastCheckpointOwnerEpoch = 1,
        !.lastCheckpointAttempt = 1,
        !.lastCheckpointOffset = 0,
        !.identityCheckpointed = TRUE,
        !.unackedFrontier = 1,
        !.lastEvent = "checkpoint-loaded"]

LoadRecoveredMaxCheckpoint ==
    /\ state.active
    /\ state.pollAllowed
    /\ state' = [state EXCEPT
        !.checkpointPresent = TRUE,
        !.checkpoint = MaxOffset,
        !.checkpointBefore = MaxOffset,
        !.checkpointPresentBefore = TRUE,
        !.checkpointBeforeOwnerEpoch = state.ownerEpoch,
        !.checkpointBeforeAttempt = 1,
        !.checkpointBeforeOffset = MaxOffset,
        !.lastCheckpointOwnerEpoch = state.ownerEpoch,
        !.lastCheckpointAttempt = 1,
        !.lastCheckpointOffset = MaxOffset,
        !.identityCheckpointed = TRUE,
        !.unackedFrontier = MaxOffset + 1,
        !.lastEvent = "checkpoint-loaded"]

PollOffsetExhausted ==
    /\ state.active
    /\ state.pollAllowed
    /\ state.checkpointPresent
    /\ state.checkpoint = MaxOffset
    /\ state' = [state EXCEPT
        !.offsetExhausted = TRUE,
        !.partitionState = "offset-exhausted",
        !.pollAllowed = FALSE,
        !.lastEvent = "offset-exhausted"]

PollCheckpointConflict ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ expected > state.creationH
    /\ state' = [state EXCEPT
        !.pollKind = "conflict",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 0,
        !.partitionState = "recovery-required",
        !.active = FALSE,
        !.pollAllowed = FALSE,
        !.lastEvent = "poll-conflict"]

PollIncoherent ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ state.creationOldest <= expected
    /\ expected < state.creationH
    /\ state' = [state EXCEPT
        !.pollKind = "incoherent",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 0,
        !.partitionState = "faulted",
        !.active = FALSE,
        !.pollAllowed = FALSE,
        !.lastEvent = "poll-incoherent"]

PollReadFailure ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ state' = [state EXCEPT
        !.pollKind = "read-failure",
        !.pollExpected = expected,
        !.pollReadFailed = TRUE,
        !.checkpointBefore = state.checkpoint,
        !.checkpointPresentBefore = state.checkpointPresent,
        !.checkpointBeforeOwnerEpoch = state.lastCheckpointOwnerEpoch,
        !.checkpointBeforeAttempt = state.lastCheckpointAttempt,
        !.checkpointBeforeOffset = state.lastCheckpointOffset,
        !.lastEvent = "poll-read-failure"]

PollMisroute ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ expected < state.creationH
    /\ state' = [state EXCEPT
        !.pollKind = "misroute",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 1,
        !.pollRecordOffset = expected,
        !.pollRecordId = 101,
        !.pollRecordDigest = 201,
        !.pollTargetOk = FALSE,
        !.pollGenerationOk = TRUE,
        !.pollPartitionOk = TRUE,
        !.pollEnvelopeValid = TRUE,
        !.partitionState = "faulted",
        !.active = FALSE,
        !.pollAllowed = FALSE,
        !.lastEvent = "poll-misroute"]

PollCorrupt ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ expected < state.creationH
    /\ state' = [state EXCEPT
        !.pollKind = "corrupt",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 1,
        !.pollRecordOffset = expected,
        !.pollRecordId = 101,
        !.pollRecordDigest = 202,
        !.pollTargetOk = TRUE,
        !.pollGenerationOk = TRUE,
        !.pollPartitionOk = TRUE,
        !.pollEnvelopeValid = FALSE,
        !.partitionState = "faulted",
        !.active = FALSE,
        !.pollAllowed = FALSE,
        !.lastEvent = "poll-corrupt"]

PollIdentityConflict ==
    LET expected == ExpectedOffset IN
    /\ state.active
    /\ state.pollAllowed
    /\ expected < state.creationH
    /\ state' = [state EXCEPT
        !.pollKind = "identity-conflict",
        !.pollExpected = expected,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 1,
        !.pollRecordOffset = expected,
        !.pollRecordId = 101,
        !.pollRecordDigest = 202,
        !.identityPresent = TRUE,
        !.identityDurable = TRUE,
        !.identityId = 101,
        !.identityDigest = 201,
        !.identityConflict = TRUE,
        !.partitionState = "faulted",
        !.active = FALSE,
        !.pollAllowed = FALSE,
        !.lastEvent = "poll-identity-conflict"]

PersistIdentity ==
    /\ state.pollKind = "record"
    /\ state.pollRecordCount = 1
    /\ state.pollTargetOk
    /\ state.pollGenerationOk
    /\ state.pollPartitionOk
    /\ state.pollEnvelopeValid
    /\ ~state.identityConflict
    /\ state.pollReserved
    /\ state.pollOwnerEpoch = state.ownerEpoch
    /\ (~state.identityPresent
        \/ (state.identityId = state.pollRecordId /\ state.identityDigest = state.pollRecordDigest))
    /\ state' = [state EXCEPT
        !.identityPresent = TRUE,
        !.identityDurable = TRUE,
        !.identityId = state.pollRecordId,
        !.identityDigest = state.pollRecordDigest,
        !.journalSequence = state.journalSequence + 1,
        !.appliedThrough = state.journalSequence + 1,
        !.lastEvent = "identity-persisted"]

DeliverRecord ==
    /\ state.active
    /\ state.pollKind = "record"
    /\ state.pollRecordCount = 1
    /\ state.identityDurable
    /\ state.identityId = state.pollRecordId
    /\ state.identityDigest = state.pollRecordDigest
    /\ state.pollReserved
    /\ state.pollOwnerEpoch = state.ownerEpoch
    /\ ~state.inFlight
    /\ state.deliveryAttempt = 0
    /\ state' = [state EXCEPT
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.partitionState = "in-flight",
        !.pollAllowed = FALSE,
        !.pollReserved = FALSE,
        !.deliveryOffset = state.pollRecordOffset,
        !.deliveryId = state.pollRecordId,
        !.deliveryDigest = state.pollRecordDigest,
        !.deliveryAttempt = 1,
        !.handleState = "open",
        !.handleOffset = state.pollRecordOffset,
        !.handleId = state.pollRecordId,
        !.handleDigest = state.pollRecordDigest,
        !.handleAttempt = 1,
        !.handleOwnerEpoch = state.ownerEpoch,
        !.handleGeneration = 1,
        !.terminalWinner = "none",
        !.lastEvent = "delivery"]

BeginAck ==
    /\ state.active
    /\ state.inFlight
    /\ state.handleState = "open"
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.handleState = "ack-installing",
        !.checkpointStage = "prewrite",
        !.checkpointCandidate = state.handleOffset,
        !.checkpointBefore = state.checkpoint,
        !.checkpointPresentBefore = state.checkpointPresent,
        !.checkpointBeforeOwnerEpoch = state.lastCheckpointOwnerEpoch,
        !.checkpointBeforeAttempt = state.lastCheckpointAttempt,
        !.checkpointBeforeOffset = state.lastCheckpointOffset,
        !.checkpointFileSynced = FALSE,
        !.checkpointDirSynced = FALSE,
        !.lastEvent = "ack"]

WriteCheckpoint ==
    /\ state.handleState = "ack-installing"
    /\ state.checkpointStage = "prewrite"
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.checkpointStage = "written",
        !.journalSequence = state.journalSequence + 1,
        !.lastEvent = "checkpoint-written"]

SyncCheckpointFile ==
    /\ state.handleState = "ack-installing"
    /\ state.checkpointStage = "written"
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.checkpointStage = "file-synced",
        !.checkpointFileSynced = TRUE,
        !.lastEvent = "checkpoint-file-synced"]

SyncCheckpointDirectory ==
    /\ state.handleState = "ack-installing"
    /\ state.checkpointStage = "renamed"
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state.checkpointFileSynced
    /\ state' = [state EXCEPT
        !.checkpointStage = "dir-synced",
        !.checkpointDirSynced = TRUE,
        !.lastEvent = "checkpoint-dir-synced"]

RenameCheckpoint ==
    /\ state.handleState = "ack-installing"
    /\ state.checkpointStage = "file-synced"
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state.checkpointFileSynced
    /\ state' = [state EXCEPT
        !.checkpointStage = "renamed",
        !.lastEvent = "checkpoint-renamed"]

CommitCheckpoint ==
    /\ state.handleState = "ack-installing"
    /\ state.checkpointStage = "dir-synced"
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state.checkpointFileSynced
    /\ state.checkpointDirSynced
    /\ state.checkpointCandidate = state.handleOffset
    /\ state' = [state EXCEPT
        !.checkpointPresent = TRUE,
        !.checkpoint = state.checkpointCandidate,
        !.checkpointOutcome = "committed",
        !.checkpointRecovery = "new",
        !.appliedThrough = state.journalSequence,
        !.identityCheckpointed = TRUE,
        !.handleState = "terminal-committed",
        !.winnerCounts[state.handleAttempt] = @ + 1,
        !.terminalWinner = "ack",
        !.lastCheckpointAttempt = state.handleAttempt,
        !.lastCheckpointOwnerEpoch = state.handleOwnerEpoch,
        !.lastCheckpointOffset = state.handleOffset,
        !.inFlight = FALSE,
        !.inFlightCount = 0,
        !.partitionState = "active",
        !.pollAllowed = TRUE,
        !.unackedFrontier = state.checkpointCandidate + 1,
        !.liveCheckpointed = state.liveTracked \/ state.liveCheckpointed,
        !.lastEvent = "checkpoint-committed"]

CheckpointKnownOld ==
    /\ state.handleState = "ack-installing"
    /\ state.checkpointStage = "prewrite"
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.handleState = "ack-retryable",
        !.checkpointOutcome = "not-committed",
        !.checkpointStage = "none",
        !.lastEvent = "checkpoint-known-old"]

RetryAck ==
    /\ state.handleState = "ack-retryable"
    /\ state.inFlight
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.handleState = "ack-installing",
        !.checkpointStage = "prewrite",
        !.checkpointOutcome = "none",
        !.lastEvent = "ack-retry"]

CheckpointInstallUnknown(choice) ==
    /\ choice \in {"old", "new"}
    /\ state.handleState = "ack-installing"
    /\ state.checkpointStage \in {"written", "file-synced"}
    /\ state.handleAttempt = state.deliveryAttempt
    /\ state.handleOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.checkpointOutcome = "unknown",
        !.checkpointRecovery = choice,
        !.handleState = "terminal-unknown",
        !.winnerCounts[state.handleAttempt] = @ + 1,
        !.terminalWinner = "ack-unknown",
        !.partitionState = "checkpoint-indeterminate",
        !.active = FALSE,
        !.pollAllowed = FALSE,
        !.lastEvent = "checkpoint-unknown"]

RecoverCheckpointOld ==
    /\ state.checkpointOutcome = "unknown"
    /\ state.checkpointRecovery = "old"
    /\ state.partitionState = "checkpoint-indeterminate"
    /\ state.journalValid
    /\ state.sequenceContiguous
    /\ state' = [state EXCEPT
        !.checkpoint = state.checkpointBefore,
        !.checkpointPresent = state.checkpointPresentBefore,
        !.checkpointStage = "none",
        !.lastCheckpointOwnerEpoch = state.checkpointBeforeOwnerEpoch,
        !.lastCheckpointAttempt = state.checkpointBeforeAttempt,
        !.lastCheckpointOffset = state.checkpointBeforeOffset,
        !.inFlight = FALSE,
        !.inFlightCount = 0,
        !.partitionState = "redelivery",
        !.active = TRUE,
        !.pollAllowed = FALSE,
        !.lastEvent = "checkpoint-recovered-old"]

RecoverCheckpointNew ==
    /\ state.checkpointOutcome = "unknown"
    /\ state.checkpointRecovery = "new"
    /\ state.partitionState = "checkpoint-indeterminate"
    /\ state.journalValid
    /\ state.sequenceContiguous
    /\ state' = [state EXCEPT
        !.checkpoint = state.checkpointCandidate,
        !.checkpointPresent = TRUE,
        !.checkpointStage = "none",
        !.identityCheckpointed = TRUE,
        !.lastCheckpointAttempt = state.handleAttempt,
        !.lastCheckpointOwnerEpoch = state.handleOwnerEpoch,
        !.lastCheckpointOffset = state.handleOffset,
        !.inFlight = FALSE,
        !.inFlightCount = 0,
        !.partitionState = "active",
        !.active = TRUE,
        !.pollAllowed = TRUE,
        !.unackedFrontier = state.checkpointCandidate + 1,
        !.lastEvent = "checkpoint-recovered-new"]

ReleaseHandle(kind) ==
    /\ kind \in {"nack", "timeout", "shutdown-fence"}
    /\ state.inFlight
    /\ state.handleState = "open"
    /\ state' = [state EXCEPT
        !.handleState = CASE kind = "nack" -> "released"
                         [] kind = "timeout" -> "timed-out"
                         [] OTHER -> "shutdown-fenced",
        !.winnerCounts[state.handleAttempt] = @ + 1,
        !.terminalWinner = kind,
        !.previousHandleAttempt = state.handleAttempt,
        !.inFlight = FALSE,
        !.inFlightCount = 0,
        !.partitionState = IF kind = "shutdown-fence" THEN "inactive" ELSE "redelivery",
        !.active = kind # "shutdown-fence",
        !.pollAllowed = FALSE,
        !.lastEvent = kind]

Redeliver ==
    /\ state.partitionState = "redelivery"
    /\ state.deliveryAttempt < MaxDeliveryAttempts
    /\ ~state.inFlight
    /\ state.checkpoint = state.checkpointBefore
    /\ state' = [state EXCEPT
        !.deliveryAttempt = state.deliveryAttempt + 1,
        !.handleState = "open",
        !.handleOffset = state.deliveryOffset,
        !.handleId = state.deliveryId,
        !.handleDigest = state.deliveryDigest,
        !.handleAttempt = state.deliveryAttempt + 1,
        !.handleOwnerEpoch = state.ownerEpoch,
        !.handleGeneration = 1,
        !.terminalWinner = "none",
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.partitionState = "in-flight",
        !.redeliveryObserved = TRUE,
        !.lastEvent = "redelivery"]

ObserveDuplicateRedelivery ==
    /\ state.redeliveryObserved
    /\ state.deliveryAttempt = 2
    /\ state.handleId = state.identityId
    /\ state.handleDigest = state.identityDigest
    /\ ~state.duplicateObserved
    /\ state' = [state EXCEPT
        !.duplicateObserved = TRUE,
        !.lastEvent = "duplicate-observed"]

UpdateAdvisoryOffset ==
    /\ state.active
    /\ state' = [state EXCEPT
        !.advisoryCheckpointSnapshot = state.checkpoint,
        !.advisoryCheckpointPresentSnapshot = state.checkpointPresent,
        !.advisoryOffset = state.creationH,
        !.lastEvent = "advisory-updated"]

ApplyApplicationEffect ==
    /\ state.inFlight
    /\ state.handleState = "open"
    /\ ~state.applicationEffect
    /\ state' = [state EXCEPT
        !.applicationEffect = TRUE,
        !.lastEvent = "effect-applied"]

CrashAfterEffect ==
    /\ state.applicationEffect
    /\ ~state.checkpointPresent
    /\ state.handleState = "open"
    /\ state' = [state EXCEPT
        !.effectCrash = TRUE,
        !.inFlight = FALSE,
        !.inFlightCount = 0,
        !.active = FALSE,
        !.partitionState = "recovery-required",
        !.pollAllowed = FALSE,
        !.lastEvent = "effect-crash"]

RecoverEffectOld ==
    /\ state.effectCrash
    /\ state.partitionState = "recovery-required"
    /\ ~state.checkpointPresent
    /\ state' = [state EXCEPT
        !.active = TRUE,
        !.partitionState = "redelivery",
        !.previousHandleAttempt = state.handleAttempt,
        !.lastEvent = "checkpoint-recovered-old"]

ReplaceResourceEpoch ==
    /\ state.active
    /\ state.currentResourceEpoch = 1
    /\ state' = [state EXCEPT
        !.currentResourceEpoch = 2,
        !.active = FALSE,
        !.partitionState = "namespace-mismatch",
        !.pollAllowed = FALSE,
        !.lastEvent = "resource-changed"]

FenceConfigEpoch ==
    /\ state.active
    /\ state.currentConfigEpoch = 1
    /\ state' = [state EXCEPT
        !.currentConfigEpoch = 2,
        !.active = FALSE,
        !.partitionState = "namespace-mismatch",
        !.pollAllowed = FALSE,
        !.lastEvent = "config-changed"]

RejectInvalidRecovery ==
    /\ state.active
    /\ state' = [state EXCEPT
        !.journalValid = FALSE,
        !.sequenceContiguous = FALSE,
        !.active = FALSE,
        !.partitionState = "faulted",
        !.pollAllowed = FALSE,
        !.lastEvent = "recovery-fault"]

StartLivenessObligation ==
    /\ state.creationStage = "created"
    /\ state.active
    /\ AllLivenessAssumptions
    /\ ~state.liveTracked
    /\ state.resolvedStart <= 1
    /\ state' = [state EXCEPT
        !.liveTracked = TRUE,
        !.liveBudget = LivenessBudget,
        !.lastEvent = "liveness-start"]

LivenessPoll ==
    /\ state.liveTracked
    /\ state.liveBudget = LivenessBudget
    /\ AllLivenessAssumptions
    /\ state.active
    /\ state' = [state EXCEPT
        !.pollKind = "record",
        !.pollExpected = state.resolvedStart,
        !.pollPreviousH = state.lastH,
        !.pollResourceEpoch = state.currentResourceEpoch,
        !.pollH = state.creationH,
        !.pollOldest = state.creationOldest,
        !.pollRecordCount = 1,
        !.pollRecordOffset = state.resolvedStart,
        !.pollRecordId = 101,
        !.pollRecordDigest = 201,
        !.lastH = state.creationH,
        !.pollAllowed = FALSE,
        !.pollReserved = TRUE,
        !.pollOwnerEpoch = state.ownerEpoch,
        !.liveBudget = state.liveBudget - 1,
        !.lastEvent = "liveness-poll"]

LivenessPersistIdentity ==
    /\ state.liveTracked
    /\ state.lastEvent = "liveness-poll"
    /\ state.liveBudget = LivenessBudget - 1
    /\ state.pollReserved
    /\ state.pollOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.identityPresent = TRUE,
        !.identityDurable = TRUE,
        !.identityId = state.pollRecordId,
        !.identityDigest = state.pollRecordDigest,
        !.liveBudget = state.liveBudget - 1,
        !.lastEvent = "liveness-identity"]

LivenessDeliver ==
    /\ state.liveTracked
    /\ state.lastEvent = "liveness-identity"
    /\ state.liveBudget = 1
    /\ state.identityDurable
    /\ state.pollReserved
    /\ state.pollOwnerEpoch = state.ownerEpoch
    /\ state' = [state EXCEPT
        !.deliveryOffset = state.pollRecordOffset,
        !.deliveryId = state.pollRecordId,
        !.deliveryDigest = state.pollRecordDigest,
        !.deliveryAttempt = 1,
        !.handleState = "open",
        !.handleOffset = state.pollRecordOffset,
        !.handleId = state.pollRecordId,
        !.handleDigest = state.pollRecordDigest,
        !.handleAttempt = 1,
        !.handleOwnerEpoch = state.ownerEpoch,
        !.handleGeneration = 1,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.partitionState = "in-flight",
        !.pollAllowed = FALSE,
        !.pollReserved = FALSE,
        !.liveBudget = 0,
        !.liveDelivered = TRUE,
        !.lastEvent = "liveness-delivery"]

UnsafePollMixed ==
    /\ state' = [state EXCEPT
        !.resolved = TRUE,
        !.resolvedStart = 1,
        !.pollKind = "record",
        !.pollExpected = 1,
        !.pollPreviousH = 3,
        !.pollH = 2,
        !.pollOldest = 1,
        !.pollRecordCount = 0,
        !.pollRecordOffset = 2,
        !.pollResourceEpoch = 2,
        !.lastEvent = "unsafe"]

UnsafeMisrouteDelivered ==
    /\ state' = [state EXCEPT
        !.pollKind = "misroute",
        !.pollTargetOk = FALSE,
        !.deliveryAttempt = 1,
        !.deliveryOffset = 1,
        !.deliveryId = 101,
        !.deliveryDigest = 201,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.handleState = "open",
        !.lastEvent = "unsafe"]

UnsafeIdentityConflictDelivered ==
    /\ state' = [state EXCEPT
        !.identityPresent = TRUE,
        !.identityDurable = TRUE,
        !.identityId = 101,
        !.identityDigest = 201,
        !.identityConflict = TRUE,
        !.deliveryAttempt = 1,
        !.deliveryId = 101,
        !.deliveryDigest = 202,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.handleState = "open",
        !.lastEvent = "unsafe"]

UnsafeDeliveryBeforeIdentity ==
    /\ state' = [state EXCEPT
        !.identityPresent = FALSE,
        !.identityDurable = FALSE,
        !.deliveryAttempt = 1,
        !.deliveryOffset = 1,
        !.deliveryId = 101,
        !.deliveryDigest = 201,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.handleState = "open",
        !.lastEvent = "unsafe"]

UnsafeResolvedStartChange ==
    /\ state' = [state EXCEPT
        !.resolved = TRUE,
        !.resolvedStartSnapshot = 1,
        !.resolvedStart = 2,
        !.lastEvent = "unsafe"]

UnsafeOwnerFork ==
    /\ state' = [state EXCEPT
        !.unitVisible = TRUE,
        !.manifestValid = TRUE,
        !.creationFileSynced = TRUE,
        !.creationRenamed = TRUE,
        !.creationDirSynced = TRUE,
        !.ownerEpoch = 2,
        !.ownerStage = "committed",
        !.ownerRecordValid = TRUE,
        !.ownerLinked = FALSE,
        !.ownerFileSynced = TRUE,
        !.ownerDirSynced = TRUE,
        !.active = TRUE,
        !.partitionState = "active",
        !.lastEvent = "unsafe"]

UnsafeDoubleInflight ==
    /\ state' = [state EXCEPT
        !.inFlight = TRUE,
        !.inFlightCount = 2,
        !.lastEvent = "unsafe"]

UnsafeHandleSubstitution ==
    /\ state' = [state EXCEPT
        !.identityPresent = TRUE,
        !.identityDurable = TRUE,
        !.identityId = 101,
        !.identityDigest = 201,
        !.deliveryAttempt = 1,
        !.deliveryOffset = 1,
        !.deliveryId = 101,
        !.deliveryDigest = 201,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.handleState = "open",
        !.handleOffset = 2,
        !.handleId = 101,
        !.handleDigest = 201,
        !.handleAttempt = 1,
        !.handleOwnerEpoch = 1,
        !.handleGeneration = 1,
        !.lastEvent = "unsafe"]

UnsafeOldHandleAck ==
    /\ state' = [state EXCEPT
        !.ownerEpoch = 1,
        !.deliveryAttempt = 2,
        !.deliveryOffset = 1,
        !.previousHandleAttempt = 1,
        !.handleAttempt = 2,
        !.handleOwnerEpoch = 1,
        !.checkpointPresent = TRUE,
        !.checkpointBefore = 0,
        !.checkpoint = 1,
        !.lastCheckpointAttempt = 1,
        !.lastCheckpointOwnerEpoch = 1,
        !.lastCheckpointOffset = 1,
        !.lastEvent = "unsafe"]

UnsafeStaleHandleAck ==
    /\ state' = [state EXCEPT
        !.ownerEpoch = 2,
        !.deliveryAttempt = 1,
        !.handleAttempt = 1,
        !.handleOwnerEpoch = 2,
        !.checkpointPresent = TRUE,
        !.checkpointBefore = 0,
        !.checkpoint = 1,
        !.lastCheckpointAttempt = 1,
        !.lastCheckpointOwnerEpoch = 1,
        !.lastCheckpointOffset = 1,
        !.lastEvent = "checkpoint-committed"]

UnsafePollBeforeCheckpoint ==
    /\ state' = [state EXCEPT
        !.handleState = "ack-installing",
        !.checkpointStage = "written",
        !.checkpointOutcome = "none",
        !.pollAllowed = TRUE,
        !.lastEvent = "unsafe"]

UnsafeKnownOldTerminal ==
    /\ state' = [state EXCEPT
        !.checkpointOutcome = "not-committed",
        !.handleState = "terminal-committed",
        !.lastEvent = "unsafe"]

UnsafeUnknownKeepsActive ==
    /\ state' = [state EXCEPT
        !.checkpointOutcome = "unknown",
        !.checkpointRecovery = "old",
        !.partitionState = "active",
        !.active = TRUE,
        !.pollAllowed = TRUE,
        !.lastEvent = "unsafe"]

UnsafeRecoveryNeither ==
    /\ state' = [state EXCEPT
        !.checkpointOutcome = "unknown",
        !.checkpointRecovery = "old",
        !.checkpointBefore = 0,
        !.checkpointCandidate = 1,
        !.checkpointPresent = TRUE,
        !.checkpoint = 2,
        !.lastEvent = "unsafe"]

UnsafeInvalidStateActive ==
    /\ state' = [state EXCEPT
        !.unitVisible = TRUE,
        !.manifestValid = FALSE,
        !.ownerEpoch = 1,
        !.ownerRecordValid = FALSE,
        !.journalValid = FALSE,
        !.sequenceContiguous = FALSE,
        !.active = TRUE,
        !.partitionState = "active",
        !.lastEvent = "unsafe"]

UnsafeAdvisoryAdvances ==
    /\ state' = [state EXCEPT
        !.advisoryCheckpointSnapshot = 0,
        !.advisoryCheckpointPresentSnapshot = FALSE,
        !.advisoryOffset = 2,
        !.checkpointPresent = TRUE,
        !.checkpoint = 1,
        !.lastEvent = "advisory-updated"]

UnsafeEffectNotRedelivered ==
    /\ state' = [state EXCEPT
        !.applicationEffect = TRUE,
        !.effectCrash = TRUE,
        !.deliveryAttempt = 1,
        !.redeliveryObserved = FALSE,
        !.checkpointPresent = FALSE,
        !.partitionState = "active",
        !.lastEvent = "unsafe"]

UnsafeInvalidExactCommits ==
    /\ state' = [state EXCEPT
        !.selection = "exact",
        !.creationOldest = 2,
        !.creationH = 3,
        !.requestedExact = 1,
        !.creationStage = "created",
        !.creationOutcome = "created",
        !.unitVisible = TRUE,
        !.lastEvent = "unsafe"]

UnsafeUnknownCreationPolls ==
    /\ state' = [state EXCEPT
        !.creationStage = "unknown",
        !.creationOutcome = "unknown",
        !.sameDirectoryOnly = TRUE,
        !.active = TRUE,
        !.partitionState = "active",
        !.pollAllowed = TRUE,
        !.selectionOverwriteAttempted = TRUE,
        !.lastEvent = "unsafe"]

UnsafeDoubleHandleWinner ==
    /\ state' = [state EXCEPT
        !.winnerCounts[1] = 2,
        !.lastEvent = "unsafe"]

UnsafeOffsetWrap ==
    /\ state' = [state EXCEPT
        !.checkpointPresent = TRUE,
        !.checkpoint = MaxOffset,
        !.offsetExhausted = FALSE,
        !.active = TRUE,
        !.pollAllowed = TRUE,
        !.lastEvent = "unsafe"]

UnsafeTailIncoherent ==
    /\ state' = [state EXCEPT
        !.resolved = TRUE,
        !.resolvedStart = 1,
        !.pollKind = "tail",
        !.pollExpected = 1,
        !.pollPreviousH = 1,
        !.pollH = 2,
        !.pollOldest = 1,
        !.pollRecordCount = 1,
        !.lastEvent = "unsafe"]

UnsafeSelectedSkip ==
    /\ state' = [state EXCEPT
        !.resolved = TRUE,
        !.resolvedStart = 1,
        !.pollKind = "record",
        !.pollExpected = 1,
        !.pollH = 3,
        !.pollOldest = 1,
        !.pollRecordCount = 1,
        !.pollRecordOffset = 2,
        !.deliveryOffset = 2,
        !.deliveryAttempt = 1,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.lastEvent = "unsafe"]

UnsafeNamespaceUnfenced ==
    /\ state' = [state EXCEPT
        !.manifestResourceEpoch = 1,
        !.currentResourceEpoch = 2,
        !.manifestConfigEpoch = 1,
        !.currentConfigEpoch = 1,
        !.active = TRUE,
        !.partitionState = "active",
        !.pollAllowed = TRUE,
        !.lastEvent = "unsafe"]

UnsafeRetentionAsTail ==
    /\ state' = [state EXCEPT
        !.resolved = TRUE,
        !.resolvedStart = 1,
        !.creationOldest = 2,
        !.creationH = 3,
        !.pollKind = "tail",
        !.pollExpected = 1,
        !.pollOldest = 2,
        !.pollH = 3,
        !.pollRecordCount = 0,
        !.partitionState = "tail",
        !.lastEvent = "unsafe"]

UnsafeCorruptDelivered ==
    /\ state' = [state EXCEPT
        !.pollKind = "corrupt",
        !.pollEnvelopeValid = FALSE,
        !.deliveryAttempt = 1,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.lastEvent = "unsafe"]

UnsafeReadFailureAdvances ==
    /\ state' = [state EXCEPT
        !.pollKind = "read-failure",
        !.pollReadFailed = TRUE,
        !.checkpointBefore = 0,
        !.checkpointPresentBefore = FALSE,
        !.checkpointPresent = TRUE,
        !.checkpoint = 1,
        !.lastEvent = "unsafe"]

UnsafeInvalidPrefixActive ==
    /\ state' = [state EXCEPT
        !.journalValid = FALSE,
        !.sequenceContiguous = FALSE,
        !.active = TRUE,
        !.partitionState = "active",
        !.lastEvent = "unsafe"]

UnsafeEarlyCreationSuccess ==
    /\ state' = [state EXCEPT
        !.creationOutcome = "created",
        !.creationStage = "created",
        !.unitVisible = TRUE,
        !.creationFileSynced = FALSE,
        !.creationRenamed = FALSE,
        !.creationDirSynced = FALSE,
        !.lastEvent = "unsafe"]

UnsafeEarlyCheckpointSuccess ==
    /\ state' = [state EXCEPT
        !.checkpointStage = "written",
        !.checkpointOutcome = "committed",
        !.checkpointCandidate = 1,
        !.checkpointPresent = TRUE,
        !.checkpoint = 1,
        !.checkpointFileSynced = FALSE,
        !.checkpointDirSynced = FALSE,
        !.handleAttempt = 1,
        !.handleOwnerEpoch = 1,
        !.handleOffset = 1,
        !.lastCheckpointAttempt = 1,
        !.lastCheckpointOwnerEpoch = 1,
        !.lastCheckpointOffset = 1,
        !.lastEvent = "unsafe"]

UnsafePollOwnerChange ==
    /\ state' = [state EXCEPT
        !.ownerEpoch = 2,
        !.pollAllowed = FALSE,
        !.pollReserved = TRUE,
        !.pollOwnerEpoch = 1,
        !.pollKind = "record",
        !.lastEvent = "unsafe"]

UnsafeDeliveryWithoutReservation ==
    /\ state' = [state EXCEPT
        !.ownerEpoch = 1,
        !.pollReserved = FALSE,
        !.pollOwnerEpoch = 0,
        !.deliveryAttempt = 1,
        !.handleOwnerEpoch = 1,
        !.inFlight = TRUE,
        !.inFlightCount = 1,
        !.lastEvent = "unsafe"]

UnsafeCheckpointOutcomeMismatch ==
    /\ state' = [state EXCEPT
        !.checkpointOutcome = "committed",
        !.checkpointBefore = 0,
        !.checkpointPresentBefore = FALSE,
        !.checkpointCandidate = 1,
        !.checkpointPresent = FALSE,
        !.checkpoint = 0,
        !.lastEvent = "unsafe"]

UnsafeCreationOutcomeMismatch ==
    /\ state' = [state EXCEPT
        !.creationOutcome = "not-committed",
        !.unitVisible = TRUE,
        !.ownerEpoch = 1,
        !.lastEvent = "unsafe"]

UnsafeDuplicateAdvancesFrontier ==
    /\ state' = [state EXCEPT
        !.duplicateObserved = TRUE,
        !.deliveryAttempt = 2,
        !.unackedFrontier = 1,
        !.lastEvent = "unsafe"]

UnsafeLivenessStalled ==
    /\ state' = [state EXCEPT
        !.liveTracked = TRUE,
        !.liveSelectedStrong = TRUE,
        !.liveOffsetSelected = TRUE,
        !.liveBrokerAvailable = TRUE,
        !.liveRecoveryAvailable = TRUE,
        !.liveSubscriptionAvailable = TRUE,
        !.liveRetentionAvailable = TRUE,
        !.liveFairPoll = TRUE,
        !.liveFairRedelivery = TRUE,
        !.liveValidState = TRUE,
        !.liveCapacityAvailable = TRUE,
        !.liveSameOwner = TRUE,
        !.liveSameResource = TRUE,
        !.liveSameInbox = TRUE,
        !.liveBudget = 0,
        !.liveDelivered = FALSE,
        !.liveCheckpointed = FALSE,
        !.lastEvent = "unsafe"]

UnsafeLiveness(mode) ==
    /\ mode \in {
        "liveness-weak-selected", "liveness-offset-before-e0",
        "liveness-no-broker", "liveness-no-recovery",
        "liveness-no-subscription", "liveness-no-retention",
        "liveness-unfair-poll", "liveness-unfair-redelivery",
        "liveness-corrupt", "liveness-no-capacity", "liveness-owner-change",
        "liveness-resource-change", "liveness-inbox-change"
        }
    /\ state' = [state EXCEPT
        !.liveTracked = TRUE,
        !.liveBudget = 0,
        !.liveDelivered = FALSE,
        !.liveCheckpointed = FALSE,
        !.liveSelectedStrong = mode # "liveness-weak-selected",
        !.liveOffsetSelected = mode # "liveness-offset-before-e0",
        !.liveBrokerAvailable = mode # "liveness-no-broker",
        !.liveRecoveryAvailable = mode # "liveness-no-recovery",
        !.liveSubscriptionAvailable = mode # "liveness-no-subscription",
        !.liveRetentionAvailable = mode # "liveness-no-retention",
        !.liveFairPoll = mode # "liveness-unfair-poll",
        !.liveFairRedelivery = mode # "liveness-unfair-redelivery",
        !.liveValidState = mode # "liveness-corrupt",
        !.liveCapacityAvailable = mode # "liveness-no-capacity",
        !.liveSameOwner = mode # "liveness-owner-change",
        !.liveSameResource = mode # "liveness-resource-change",
        !.liveSameInbox = mode # "liveness-inbox-change",
        !.lastEvent = "unsafe"]

UnsafeAction ==
    CASE UnsafeMode = "poll-mixed" -> UnsafePollMixed
      [] UnsafeMode = "misroute-delivered" -> UnsafeMisrouteDelivered
      [] UnsafeMode = "identity-conflict-delivered" -> UnsafeIdentityConflictDelivered
      [] UnsafeMode = "delivery-before-identity" -> UnsafeDeliveryBeforeIdentity
      [] UnsafeMode = "resolved-start-change" -> UnsafeResolvedStartChange
      [] UnsafeMode = "owner-fork" -> UnsafeOwnerFork
      [] UnsafeMode = "double-inflight" -> UnsafeDoubleInflight
      [] UnsafeMode = "handle-substitution" -> UnsafeHandleSubstitution
      [] UnsafeMode = "old-handle-ack" -> UnsafeOldHandleAck
      [] UnsafeMode = "stale-handle-ack" -> UnsafeStaleHandleAck
      [] UnsafeMode = "poll-before-checkpoint" -> UnsafePollBeforeCheckpoint
      [] UnsafeMode = "known-old-terminal" -> UnsafeKnownOldTerminal
      [] UnsafeMode = "unknown-keeps-active" -> UnsafeUnknownKeepsActive
      [] UnsafeMode = "recovery-neither" -> UnsafeRecoveryNeither
      [] UnsafeMode = "invalid-state-active" -> UnsafeInvalidStateActive
      [] UnsafeMode = "advisory-advances" -> UnsafeAdvisoryAdvances
      [] UnsafeMode = "effect-not-redelivered" -> UnsafeEffectNotRedelivered
      [] UnsafeMode = "invalid-exact-commits" -> UnsafeInvalidExactCommits
      [] UnsafeMode = "unknown-creation-polls" -> UnsafeUnknownCreationPolls
      [] UnsafeMode = "double-handle-winner" -> UnsafeDoubleHandleWinner
      [] UnsafeMode = "offset-wrap" -> UnsafeOffsetWrap
      [] UnsafeMode = "tail-incoherent" -> UnsafeTailIncoherent
      [] UnsafeMode = "selected-skip" -> UnsafeSelectedSkip
      [] UnsafeMode = "namespace-unfenced" -> UnsafeNamespaceUnfenced
      [] UnsafeMode = "retention-as-tail" -> UnsafeRetentionAsTail
      [] UnsafeMode = "corrupt-delivered" -> UnsafeCorruptDelivered
      [] UnsafeMode = "read-failure-advances" -> UnsafeReadFailureAdvances
      [] UnsafeMode = "invalid-prefix-active" -> UnsafeInvalidPrefixActive
      [] UnsafeMode = "early-creation-success" -> UnsafeEarlyCreationSuccess
      [] UnsafeMode = "early-checkpoint-success" -> UnsafeEarlyCheckpointSuccess
      [] UnsafeMode = "poll-owner-change" -> UnsafePollOwnerChange
      [] UnsafeMode = "delivery-without-reservation" -> UnsafeDeliveryWithoutReservation
      [] UnsafeMode = "checkpoint-outcome-mismatch" -> UnsafeCheckpointOutcomeMismatch
      [] UnsafeMode = "creation-outcome-mismatch" -> UnsafeCreationOutcomeMismatch
      [] UnsafeMode = "duplicate-advances-frontier" -> UnsafeDuplicateAdvancesFrontier
      [] UnsafeMode = "liveness-stalled" -> UnsafeLivenessStalled
      [] UnsafeMode \in {
            "liveness-weak-selected", "liveness-offset-before-e0",
            "liveness-no-broker", "liveness-no-recovery",
            "liveness-no-subscription", "liveness-no-retention",
            "liveness-unfair-poll", "liveness-unfair-redelivery",
            "liveness-corrupt", "liveness-no-capacity", "liveness-owner-change",
            "liveness-resource-change", "liveness-inbox-change"
         } -> UnsafeLiveness(UnsafeMode)
      [] OTHER -> /\ state' = state
                  /\ FALSE

Stutter == state' = state

CreationPrefixEvents == {
    "init", "creation-observed", "creation-resolved", "creation-written",
    "creation-file-synced", "creation-renamed"
}

CreationSuccessStep ==
    CASE state.lastEvent = "init" -> ObserveCreation
      [] state.lastEvent = "creation-observed" -> ResolveInitialPosition
      [] state.lastEvent = "creation-resolved" -> WriteCreationUnit
      [] state.lastEvent = "creation-written" -> SyncCreationFile
      [] state.lastEvent = "creation-file-synced" -> RenameCreationUnit
      [] state.lastEvent = "creation-renamed" -> SyncCreationDirectory
      [] OTHER -> /\ state' = state
                  /\ FALSE

DeliveryPrefixEvents == CreationPrefixEvents \cup {
    "creation-created", "poll-record", "identity-persisted"
}

DeliveryPrefixStep ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> PollValidRecord
      [] state.lastEvent = "poll-record" -> PersistIdentity
      [] state.lastEvent = "identity-persisted" -> DeliverRecord
      [] OTHER -> /\ state' = state
                  /\ FALSE

CreationMatrixNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] OTHER -> Stutter

InvalidCreationNext ==
    CASE state.lastEvent = "init" -> ObserveCreation
      [] state.lastEvent = "creation-observed" -> RejectInvalidExact
      [] OTHER -> Stutter

CreationKnownOldNext ==
    CASE state.lastEvent = "init" -> ObserveCreation
      [] state.lastEvent = "creation-observed" -> ResolveInitialPosition
      [] state.lastEvent = "creation-resolved" -> CreationKnownOldFailure
      [] OTHER -> Stutter

CreationUnknownNext(choice) ==
    CASE state.lastEvent = "init" -> ObserveCreation
      [] state.lastEvent = "creation-observed" -> ResolveInitialPosition
      [] state.lastEvent = "creation-resolved" -> WriteCreationUnit
      [] state.lastEvent = "creation-written" -> CreationUnknownFailure(choice)
      [] state.lastEvent = "creation-unknown" ->
            IF choice = "old" THEN RecoverCreationOld ELSE RecoverCreationNew
      [] OTHER -> Stutter

OwnerReopenNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> BeginOwnerInstall
      [] state.lastEvent = "owner-installing" -> CommitOwnerInstall
      [] OTHER -> Stutter

CheckpointCommitNext ==
    CASE state.lastEvent \in DeliveryPrefixEvents -> DeliveryPrefixStep
      [] state.lastEvent = "delivery" -> BeginAck
      [] state.lastEvent = "ack" -> WriteCheckpoint
      [] state.lastEvent = "checkpoint-written" -> SyncCheckpointFile
      [] state.lastEvent = "checkpoint-file-synced" -> RenameCheckpoint
      [] state.lastEvent = "checkpoint-renamed" -> SyncCheckpointDirectory
      [] state.lastEvent = "checkpoint-dir-synced" -> CommitCheckpoint
      [] OTHER -> Stutter

CheckpointKnownOldNext ==
    CASE state.lastEvent \in DeliveryPrefixEvents -> DeliveryPrefixStep
      [] state.lastEvent = "delivery" -> BeginAck
      [] state.lastEvent = "ack" -> CheckpointKnownOld
      [] state.lastEvent = "checkpoint-known-old" -> RetryAck
      [] state.lastEvent = "ack-retry" -> WriteCheckpoint
      [] state.lastEvent = "checkpoint-written" -> SyncCheckpointFile
      [] state.lastEvent = "checkpoint-file-synced" -> RenameCheckpoint
      [] state.lastEvent = "checkpoint-renamed" -> SyncCheckpointDirectory
      [] state.lastEvent = "checkpoint-dir-synced" -> CommitCheckpoint
      [] OTHER -> Stutter

CheckpointUnknownNext(choice) ==
    CASE state.lastEvent \in DeliveryPrefixEvents -> DeliveryPrefixStep
      [] state.lastEvent = "delivery" -> BeginAck
      [] state.lastEvent = "ack" -> WriteCheckpoint
      [] state.lastEvent = "checkpoint-written" -> SyncCheckpointFile
      [] state.lastEvent = "checkpoint-file-synced" -> CheckpointInstallUnknown(choice)
      [] state.lastEvent = "checkpoint-unknown" ->
            IF choice = "old" THEN RecoverCheckpointOld ELSE RecoverCheckpointNew
      [] OTHER -> Stutter

CheckpointPresentUnknownNext(choice) ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> LoadRecoveredCheckpointZero
      [] state.lastEvent = "checkpoint-loaded" -> BeginOwnerInstall
      [] state.lastEvent = "owner-installing" -> CommitOwnerInstall
      [] state.lastEvent = "owner-committed" -> PollValidRecord
      [] state.lastEvent = "poll-record" -> PersistIdentity
      [] state.lastEvent = "identity-persisted" -> DeliverRecord
      [] state.lastEvent = "delivery" -> BeginAck
      [] state.lastEvent = "ack" -> WriteCheckpoint
      [] state.lastEvent = "checkpoint-written" -> SyncCheckpointFile
      [] state.lastEvent = "checkpoint-file-synced" -> CheckpointInstallUnknown(choice)
      [] state.lastEvent = "checkpoint-unknown" ->
            IF choice = "old" THEN RecoverCheckpointOld ELSE RecoverCheckpointNew
      [] OTHER -> Stutter

ZeroCheckpointNext ==
    /\ state.scenario = "checkpoint-zero-commit"
    /\ CheckpointCommitNext

PresentOldCheckpointNext ==
    /\ state.scenario = "checkpoint-present-unknown-old"
    /\ CheckpointPresentUnknownNext("old")

PresentNewCheckpointNext ==
    /\ state.scenario = "checkpoint-present-unknown-new"
    /\ CheckpointPresentUnknownNext("new")

ReleaseRedeliveryNext(kind) ==
    CASE state.lastEvent \in DeliveryPrefixEvents -> DeliveryPrefixStep
      [] state.lastEvent = "delivery" -> ReleaseHandle(kind)
      [] state.lastEvent = kind /\ kind # "shutdown-fence" -> Redeliver
      [] OTHER -> Stutter

DuplicateRedeliveryNext ==
    CASE state.lastEvent \in DeliveryPrefixEvents -> DeliveryPrefixStep
      [] state.lastEvent = "delivery" -> ReleaseHandle("nack")
      [] state.lastEvent = "nack" -> Redeliver
      [] state.lastEvent = "redelivery" -> ObserveDuplicateRedelivery
      [] state.lastEvent = "duplicate-observed" -> BeginAck
      [] state.lastEvent = "ack" -> WriteCheckpoint
      [] state.lastEvent = "checkpoint-written" -> SyncCheckpointFile
      [] state.lastEvent = "checkpoint-file-synced" -> RenameCheckpoint
      [] state.lastEvent = "checkpoint-renamed" -> SyncCheckpointDirectory
      [] state.lastEvent = "checkpoint-dir-synced" -> CommitCheckpoint
      [] OTHER -> Stutter

TailNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> PollTail
      [] OTHER -> Stutter

RetentionGapNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> AdvanceRetention
      [] state.lastEvent = "retention-advanced" -> PollRetentionGap
      [] OTHER -> Stutter

ConflictNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> LoadRecoveredMaxCheckpoint
      [] state.lastEvent = "checkpoint-loaded" -> PollCheckpointConflict
      [] OTHER -> Stutter

OffsetExhaustionNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> LoadRecoveredMaxCheckpoint
      [] state.lastEvent = "checkpoint-loaded" -> PollOffsetExhausted
      [] OTHER -> Stutter

IncoherentPollNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> PollIncoherent
      [] OTHER -> Stutter

ReadFailureNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> PollReadFailure
      [] OTHER -> Stutter

MisrouteNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> PollMisroute
      [] OTHER -> Stutter

CorruptNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> PollCorrupt
      [] OTHER -> Stutter

IdentityConflictNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> PollIdentityConflict
      [] OTHER -> Stutter

AdvisoryNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> UpdateAdvisoryOffset
      [] OTHER -> Stutter

EffectCrashNext ==
    CASE state.lastEvent \in DeliveryPrefixEvents -> DeliveryPrefixStep
      [] state.lastEvent = "delivery" -> ApplyApplicationEffect
      [] state.lastEvent = "effect-applied" -> CrashAfterEffect
      [] state.lastEvent = "effect-crash" -> RecoverEffectOld
      [] state.lastEvent = "checkpoint-recovered-old" -> Redeliver
      [] OTHER -> Stutter

ResourceChangeNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> ReplaceResourceEpoch
      [] OTHER -> Stutter

ConfigChangeNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> FenceConfigEpoch
      [] OTHER -> Stutter

InvalidRecoveryNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> RejectInvalidRecovery
      [] OTHER -> Stutter

LivenessNext ==
    CASE state.lastEvent \in CreationPrefixEvents -> CreationSuccessStep
      [] state.lastEvent = "creation-created" -> StartLivenessObligation
      [] state.lastEvent = "liveness-start" -> LivenessPoll
      [] state.lastEvent = "liveness-poll" -> LivenessPersistIdentity
      [] state.lastEvent = "liveness-identity" -> LivenessDeliver
      [] OTHER -> Stutter

ScenarioNext ==
    CASE state.scenario = "creation-matrix" -> CreationMatrixNext
      [] state.scenario \in {"creation-invalid-below", "creation-invalid-above"} -> InvalidCreationNext
      [] state.scenario = "creation-known-old" -> CreationKnownOldNext
      [] state.scenario = "creation-unknown-old" -> CreationUnknownNext("old")
      [] state.scenario = "creation-unknown-new" -> CreationUnknownNext("new")
      [] state.scenario = "owner-reopen" -> OwnerReopenNext
      [] state.scenario = "poll-record-ack" -> CheckpointCommitNext
      [] state.scenario = "checkpoint-known-old-retry" -> CheckpointKnownOldNext
      [] state.scenario = "checkpoint-unknown-old" -> CheckpointUnknownNext("old")
      [] state.scenario = "checkpoint-unknown-new" -> CheckpointUnknownNext("new")
      [] state.scenario = "checkpoint-zero-commit" -> CheckpointCommitNext
      [] state.scenario = "checkpoint-present-unknown-old" -> CheckpointPresentUnknownNext("old")
      [] state.scenario = "checkpoint-present-unknown-new" -> CheckpointPresentUnknownNext("new")
      [] state.scenario = "nack-redelivery" -> ReleaseRedeliveryNext("nack")
      [] state.scenario = "timeout-redelivery" -> ReleaseRedeliveryNext("timeout")
      [] state.scenario = "shutdown-fence" -> ReleaseRedeliveryNext("shutdown-fence")
      [] state.scenario = "tail" -> TailNext
      [] state.scenario = "retention-gap" -> RetentionGapNext
      [] state.scenario = "checkpoint-conflict" -> ConflictNext
      [] state.scenario = "incoherent-poll" -> IncoherentPollNext
      [] state.scenario = "read-failure" -> ReadFailureNext
      [] state.scenario = "misroute" -> MisrouteNext
      [] state.scenario = "corrupt-record" -> CorruptNext
      [] state.scenario = "identity-conflict" -> IdentityConflictNext
      [] state.scenario = "advisory-mirror" -> AdvisoryNext
      [] state.scenario = "effect-crash-redelivery" -> EffectCrashNext
      [] state.scenario = "duplicate-redelivery" -> DuplicateRedeliveryNext
      [] state.scenario = "resource-change" -> ResourceChangeNext
      [] state.scenario = "config-change" -> ConfigChangeNext
      [] state.scenario = "offset-exhaustion" -> OffsetExhaustionNext
      [] state.scenario = "invalid-recovery" -> InvalidRecoveryNext
      [] state.scenario = "liveness" -> LivenessNext
      [] OTHER -> Stutter

AllActions ==
    \/ ObserveCreation
    \/ ResolveInitialPosition
    \/ RejectInvalidExact
    \/ WriteCreationUnit
    \/ SyncCreationFile
    \/ RenameCreationUnit
    \/ SyncCreationDirectory
    \/ CreationKnownOldFailure
    \/ \E choice \in {"old", "new"}: CreationUnknownFailure(choice)
    \/ RecoverCreationOld
    \/ RecoverCreationNew
    \/ BeginOwnerInstall
    \/ CommitOwnerInstall
    \/ PollValidRecord
    \/ PollTail
    \/ AdvanceRetention
    \/ PollRetentionGap
    \/ LoadRecoveredCheckpointZero
    \/ LoadRecoveredMaxCheckpoint
    \/ PollOffsetExhausted
    \/ PollCheckpointConflict
    \/ PollIncoherent
    \/ PollReadFailure
    \/ PollMisroute
    \/ PollCorrupt
    \/ PollIdentityConflict
    \/ PersistIdentity
    \/ DeliverRecord
    \/ BeginAck
    \/ WriteCheckpoint
    \/ SyncCheckpointFile
    \/ RenameCheckpoint
    \/ SyncCheckpointDirectory
    \/ CommitCheckpoint
    \/ CheckpointKnownOld
    \/ RetryAck
    \/ \E choice \in {"old", "new"}: CheckpointInstallUnknown(choice)
    \/ RecoverCheckpointOld
    \/ RecoverCheckpointNew
    \/ \E kind \in {"nack", "timeout", "shutdown-fence"}: ReleaseHandle(kind)
    \/ Redeliver
    \/ ObserveDuplicateRedelivery
    \/ UpdateAdvisoryOffset
    \/ ApplyApplicationEffect
    \/ CrashAfterEffect
    \/ RecoverEffectOld
    \/ ReplaceResourceEpoch
    \/ FenceConfigEpoch
    \/ RejectInvalidRecovery
    \/ StartLivenessObligation
    \/ LivenessPoll
    \/ LivenessPersistIdentity
    \/ LivenessDeliver
    \/ UnsafeAction
    \/ Stutter

Next == ScenarioNext \/ UnsafeAction

TypeOK ==
    /\ MaxOffset = 3
    /\ MaxOwnerEpoch = 2
    /\ MaxDeliveryAttempts = 2
    /\ LivenessBudget = 3
    /\ UnsafeMode \in UnsafeModes
    /\ state.scenario \in ScenarioIds
    /\ state.selection \in SelectionModes
    /\ state.creationOldest \in Offsets
    /\ state.creationH \in Offsets
    /\ state.requestedExact \in Offsets
    /\ state.resolvedStart \in Offsets
    /\ state.resolvedStartSnapshot \in Offsets
    /\ state.resolved \in BOOLEAN
    /\ state.capturedOldest \in Offsets
    /\ state.capturedH \in Offsets
    /\ state.capturedResourceEpoch \in 0..2
    /\ state.creationStage \in CreationStages
    /\ state.creationOutcome \in CreationOutcomes
    /\ state.pendingCreationComplete \in BOOLEAN
    /\ state.unitVisible \in BOOLEAN
    /\ state.manifestValid \in BOOLEAN
    /\ state.creationFileSynced \in BOOLEAN
    /\ state.creationRenamed \in BOOLEAN
    /\ state.creationDirSynced \in BOOLEAN
    /\ state.sameDirectoryOnly \in BOOLEAN
    /\ state.selectionOverwriteAttempted \in BOOLEAN
    /\ state.lockHeld \in BOOLEAN
    /\ state.ownerStage \in OwnerStages
    /\ state.ownerEpoch \in OwnerEpochs
    /\ state.ownerCandidate \in OwnerEpochs
    /\ state.ownerRecordValid \in BOOLEAN
    /\ state.ownerLinked \in BOOLEAN
    /\ state.ownerFileSynced \in BOOLEAN
    /\ state.ownerDirSynced \in BOOLEAN
    /\ state.ownerConnection \in Connections
    /\ state.active \in BOOLEAN
    /\ state.partitionState \in PartitionStates
    /\ state.currentResourceEpoch \in 1..2
    /\ state.currentConfigEpoch \in 1..2
    /\ state.manifestResourceEpoch \in 0..2
    /\ state.manifestConfigEpoch \in 0..2
    /\ state.checkpointPresent \in BOOLEAN
    /\ state.checkpoint \in Offsets
    /\ state.checkpointBefore \in Offsets
    /\ state.checkpointPresentBefore \in BOOLEAN
    /\ state.checkpointBeforeOwnerEpoch \in OwnerEpochs
    /\ state.checkpointBeforeAttempt \in 0..MaxDeliveryAttempts
    /\ state.checkpointBeforeOffset \in Offsets
    /\ state.pollAllowed \in BOOLEAN
    /\ state.pollReserved \in BOOLEAN
    /\ state.pollOwnerEpoch \in OwnerEpochs
    /\ state.pollKind \in PollKinds
    /\ state.pollExpected \in OffsetPoints
    /\ state.pollPreviousH \in Offsets
    /\ state.pollResourceEpoch \in 0..2
    /\ state.pollH \in Offsets
    /\ state.pollOldest \in Offsets
    /\ state.pollRecordCount \in 0..2
    /\ state.pollRecordOffset \in Offsets
    /\ state.pollRecordId \in MessageIds
    /\ state.pollRecordDigest \in Digests
    /\ state.pollTargetOk \in BOOLEAN
    /\ state.pollGenerationOk \in BOOLEAN
    /\ state.pollPartitionOk \in BOOLEAN
    /\ state.pollEnvelopeValid \in BOOLEAN
    /\ state.pollReadFailed \in BOOLEAN
    /\ state.lastH \in Offsets
    /\ state.identityPresent \in BOOLEAN
    /\ state.identityDurable \in BOOLEAN
    /\ state.identityId \in MessageIds
    /\ state.identityDigest \in Digests
    /\ state.identityConflict \in BOOLEAN
    /\ state.identityCheckpointed \in BOOLEAN
    /\ state.inFlight \in BOOLEAN
    /\ state.inFlightCount \in 0..2
    /\ state.deliveryOffset \in Offsets
    /\ state.deliveryId \in MessageIds
    /\ state.deliveryDigest \in Digests
    /\ state.deliveryAttempt \in 0..MaxDeliveryAttempts
    /\ state.handleState \in HandleStates
    /\ state.handleOffset \in Offsets
    /\ state.handleId \in MessageIds
    /\ state.handleDigest \in Digests
    /\ state.handleAttempt \in 0..MaxDeliveryAttempts
    /\ state.handleOwnerEpoch \in OwnerEpochs
    /\ state.handleGeneration \in 0..1
    /\ state.previousHandleAttempt \in 0..MaxDeliveryAttempts
    /\ state.winnerCounts \in [DeliveryAttempts -> 0..2]
    /\ state.terminalWinner \in {
        "none", "ack", "ack-unknown", "nack", "timeout", "shutdown-fence"
        }
    /\ state.checkpointStage \in CheckpointStages
    /\ state.checkpointCandidate \in Offsets
    /\ state.checkpointFileSynced \in BOOLEAN
    /\ state.checkpointDirSynced \in BOOLEAN
    /\ state.checkpointOutcome \in CheckpointOutcomes
    /\ state.checkpointRecovery \in RecoveryChoices
    /\ state.lastCheckpointAttempt \in 0..MaxDeliveryAttempts
    /\ state.lastCheckpointOwnerEpoch \in OwnerEpochs
    /\ state.lastCheckpointOffset \in Offsets
    /\ state.journalSequence \in 0..3
    /\ state.appliedThrough \in 0..3
    /\ state.journalValid \in BOOLEAN
    /\ state.sequenceContiguous \in BOOLEAN
    /\ state.advisoryOffset \in Offsets
    /\ state.advisoryCheckpointSnapshot \in Offsets
    /\ state.advisoryCheckpointPresentSnapshot \in BOOLEAN
    /\ state.applicationEffect \in BOOLEAN
    /\ state.effectCrash \in BOOLEAN
    /\ state.redeliveryObserved \in BOOLEAN
    /\ state.duplicateObserved \in BOOLEAN
    /\ state.unackedFrontier \in OffsetPoints
    /\ state.offsetExhausted \in BOOLEAN
    /\ state.liveTracked \in BOOLEAN
    /\ state.liveSelectedStrong \in BOOLEAN
    /\ state.liveOffsetSelected \in BOOLEAN
    /\ state.liveBrokerAvailable \in BOOLEAN
    /\ state.liveRecoveryAvailable \in BOOLEAN
    /\ state.liveSubscriptionAvailable \in BOOLEAN
    /\ state.liveRetentionAvailable \in BOOLEAN
    /\ state.liveFairPoll \in BOOLEAN
    /\ state.liveFairRedelivery \in BOOLEAN
    /\ state.liveValidState \in BOOLEAN
    /\ state.liveCapacityAvailable \in BOOLEAN
    /\ state.liveSameOwner \in BOOLEAN
    /\ state.liveSameResource \in BOOLEAN
    /\ state.liveSameInbox \in BOOLEAN
    /\ state.liveBudget \in 0..LivenessBudget
    /\ state.liveDelivered \in BOOLEAN
    /\ state.liveCheckpointed \in BOOLEAN
    /\ state.lastEvent \in Events

OldCheckpointImage ==
    /\ state.checkpointPresent = state.checkpointPresentBefore
    /\ state.checkpoint = state.checkpointBefore
    /\ state.lastCheckpointOwnerEpoch = state.checkpointBeforeOwnerEpoch
    /\ state.lastCheckpointAttempt = state.checkpointBeforeAttempt
    /\ state.lastCheckpointOffset = state.checkpointBeforeOffset

NewCheckpointImage ==
    /\ state.checkpointPresent
    /\ state.checkpoint = state.checkpointCandidate
    /\ state.lastCheckpointOwnerEpoch = state.handleOwnerEpoch
    /\ state.lastCheckpointAttempt = state.handleAttempt
    /\ state.lastCheckpointOffset = state.handleOffset

PollObservationCoherent ==
    CASE state.pollKind = "none" -> TRUE
      [] state.pollKind = "record" ->
            /\ state.pollRecordCount = 1
            /\ state.pollRecordOffset = state.pollExpected
            /\ state.pollOldest <= state.pollExpected
            /\ state.pollExpected < state.pollH
            /\ state.pollH >= state.pollPreviousH
            /\ state.pollResourceEpoch = state.currentResourceEpoch
            /\ state.pollTargetOk
            /\ state.pollGenerationOk
            /\ state.pollPartitionOk
            /\ state.pollEnvelopeValid
            /\ ~state.pollReadFailed
      [] state.pollKind = "tail" ->
            /\ state.pollRecordCount = 0
            /\ state.pollExpected = state.pollH
            /\ state.pollOldest <= state.pollH
            /\ state.pollH >= state.pollPreviousH
      [] state.pollKind = "gap" ->
            /\ state.pollRecordCount = 0
            /\ state.pollExpected < state.pollOldest
            /\ state.partitionState = "gap"
            /\ ~state.pollAllowed
      [] state.pollKind = "conflict" ->
            /\ state.pollRecordCount = 0
            /\ state.pollExpected > state.pollH
            /\ ~state.pollAllowed
      [] state.pollKind = "read-failure" -> state.pollReadFailed
      [] state.pollKind = "incoherent" ->
            /\ state.partitionState = "faulted"
            /\ ~state.active
            /\ ~state.pollAllowed
      [] state.pollKind \in {"misroute", "corrupt", "identity-conflict"} ->
            /\ state.partitionState = "faulted"
            /\ ~state.active
            /\ ~state.pollAllowed
      [] OTHER -> FALSE

MisrouteNeverDeliveredOrAcked ==
    (~state.pollTargetOk \/ ~state.pollGenerationOk \/ ~state.pollPartitionOk) =>
        /\ state.deliveryAttempt = 0
        /\ state.checkpointOutcome # "committed"

IdentityConflictStopsPartition ==
    state.identityConflict =>
        /\ state.deliveryAttempt = 0
        /\ state.partitionState = "faulted"
        /\ ~state.active

IdentityPersistedBeforeDelivery ==
    state.deliveryAttempt > 0 =>
        /\ state.identityPresent
        /\ state.identityDurable
        /\ state.deliveryId = state.identityId
        /\ state.deliveryDigest = state.identityDigest

ResolvedStartNeverChanges ==
    state.resolved => state.resolvedStart = state.resolvedStartSnapshot

SingleOwnerEpoch ==
    state.active =>
        /\ state.lockHeld
        /\ state.unitVisible
        /\ state.manifestValid
        /\ state.creationFileSynced
        /\ state.creationRenamed
        /\ state.creationDirSynced
        /\ state.ownerEpoch > 0
        /\ state.ownerRecordValid
        /\ state.ownerLinked
        /\ state.ownerFileSynced
        /\ state.ownerDirSynced
        /\ state.ownerConnection # "none"

SingleInflightPerPartition ==
    /\ state.inFlightCount <= 1
    /\ state.inFlight = (state.inFlightCount = 1)
    /\ (state.pollKind = "record" /\ state.deliveryAttempt = 0) => ~state.pollAllowed

HandleBoundToExactDelivery ==
    state.deliveryAttempt > 0 /\ state.handleState # "none" =>
        /\ state.handleOffset = state.deliveryOffset
        /\ state.handleId = state.deliveryId
        /\ state.handleDigest = state.deliveryDigest
        /\ state.handleAttempt = state.deliveryAttempt
        /\ state.handleGeneration = 1

OldHandleCannotAckRedelivery ==
    state.deliveryAttempt = 2 /\ state.checkpointPresent
        /\ state.lastCheckpointOffset = state.deliveryOffset =>
        state.lastCheckpointAttempt = 2

StaleHandleNeverAdvances ==
    state.lastEvent \in {"checkpoint-committed", "checkpoint-recovered-new"} =>
        /\ state.lastCheckpointOwnerEpoch = state.ownerEpoch
        /\ state.lastCheckpointAttempt = state.deliveryAttempt
        /\ state.lastCheckpointOffset = state.checkpoint

PollReservationFencesOwner ==
    /\ (state.pollReserved =>
            /\ state.pollKind = "record"
            /\ ~state.pollAllowed
            /\ ~state.inFlight
            /\ state.pollOwnerEpoch = state.ownerEpoch)
    /\ (state.deliveryAttempt > 0 /\ state.inFlight =>
            /\ ~state.pollReserved
            /\ state.pollOwnerEpoch = state.handleOwnerEpoch
            /\ state.handleOwnerEpoch = state.ownerEpoch)

NoPollPastUncommittedCheckpoint ==
    /\ (state.checkpointStage # "none" /\ state.checkpointOutcome # "committed")
            => ~state.pollAllowed
    /\ state.checkpointOutcome = "not-committed"
            => ~state.pollAllowed
    /\ state.checkpointOutcome = "unknown"
            /\ (state.partitionState = "checkpoint-indeterminate"
                \/ state.checkpointRecovery = "old") => ~state.pollAllowed

KnownOldCheckpointIsRetryable ==
    state.checkpointOutcome = "not-committed" =>
        /\ state.handleState = "ack-retryable"
        /\ OldCheckpointImage

UnknownCheckpointStopsPartition ==
    state.checkpointOutcome = "unknown" =>
        CASE state.lastEvent = "checkpoint-recovered-old" ->
                /\ state.partitionState = "redelivery"
                /\ state.active
                /\ ~state.pollAllowed
          [] state.lastEvent = "checkpoint-recovered-new" ->
                /\ state.partitionState = "active"
                /\ state.active
                /\ state.pollAllowed
          [] OTHER ->
                /\ state.partitionState = "checkpoint-indeterminate"
                /\ ~state.active
                /\ ~state.pollAllowed

RecoveryChoosesCompleteOldOrNew ==
    state.checkpointOutcome = "unknown" =>
        \/ state.partitionState = "checkpoint-indeterminate"
        \/ /\ state.checkpointRecovery = "old"
           /\ OldCheckpointImage
        \/ /\ state.checkpointRecovery = "new"
           /\ NewCheckpointImage

NoActiveFromInvalidState ==
    state.active =>
        /\ state.unitVisible
        /\ state.manifestValid
        /\ state.ownerRecordValid
        /\ state.ownerLinked
        /\ state.journalValid
        /\ state.sequenceContiguous
        /\ state.manifestResourceEpoch = state.currentResourceEpoch
        /\ state.manifestConfigEpoch = state.currentConfigEpoch

AdvisoryOffsetNeverOwnsFrontier ==
    state.lastEvent = "advisory-updated" =>
        /\ state.checkpoint = state.advisoryCheckpointSnapshot
        /\ state.checkpointPresent = state.advisoryCheckpointPresentSnapshot

EffectBeforeCheckpointMayRedeliver ==
    state.effectCrash =>
        /\ ~state.checkpointPresent
        /\ (state.partitionState \in {"recovery-required", "redelivery"}
            \/ state.redeliveryObserved)

InvalidExactNeverCommitsManifest ==
    state.selection = "exact"
        /\ (state.requestedExact < state.creationOldest
            \/ state.requestedExact > state.creationH) =>
        /\ ~state.unitVisible
        /\ state.creationOutcome # "created"

UnknownCreationBlocksPollAndOverwrite ==
    state.creationStage = "unknown" =>
        /\ ~state.active
        /\ ~state.pollAllowed
        /\ state.sameDirectoryOnly
        /\ ~state.selectionOverwriteAttempted

SingleHandleTerminalWinner ==
    \A attempt \in DeliveryAttempts: state.winnerCounts[attempt] <= 1

ExpectedIsCheckedSuccessor ==
    state.checkpointPresent /\ state.checkpoint = MaxOffset =>
        \/ state.offsetExhausted
        \/ ~state.pollAllowed
        \/ state.lastEvent = "checkpoint-loaded"

HMonotonicAndTailCoherent ==
    /\ state.pollKind \in PollKinds \ {"none", "read-failure"} =>
            state.pollH >= state.pollPreviousH
    /\ state.pollKind = "tail" =>
        /\ state.pollRecordCount = 0
        /\ state.pollExpected = state.pollH

NoSelectedOffsetSkip ==
    state.pollKind = "record" =>
        /\ state.pollRecordOffset = state.pollExpected
        /\ (state.deliveryAttempt = 0 \/ state.deliveryOffset = state.pollExpected)

DestructiveChangeFencesNamespace ==
    /\ state.active =>
        /\ state.manifestResourceEpoch = state.currentResourceEpoch
        /\ state.manifestConfigEpoch = state.currentConfigEpoch
    /\ state.checkpointPresent /\ state.lastCheckpointAttempt > 0 =>
        /\ state.manifestResourceEpoch = state.currentResourceEpoch
        /\ state.manifestConfigEpoch = state.currentConfigEpoch

AtLeastOnceUnderAssumptions ==
    state.liveTracked /\ AllLivenessAssumptions /\ state.liveBudget = 0 =>
        state.liveDelivered

LivenessAssumptionsPresent ==
    state.liveTracked => AllLivenessAssumptions

RetentionLossNeverLooksLikeTail ==
    state.pollExpected < state.pollOldest =>
        /\ state.pollKind = "gap"
        /\ state.partitionState = "gap"
        /\ ~state.pollAllowed

CorruptRecordNeverDelivered ==
    state.pollKind = "corrupt" =>
        /\ state.deliveryAttempt = 0
        /\ state.partitionState = "faulted"
        /\ ~state.active

ReadFailureNeverAdvancesFrontier ==
    state.pollKind = "read-failure" =>
        /\ state.checkpoint = state.checkpointBefore
        /\ state.checkpointPresent = state.checkpointPresentBefore

RecoveryUsesContiguousValidPrefix ==
    (~state.journalValid \/ ~state.sequenceContiguous) =>
        /\ ~state.active
        /\ state.partitionState = "faulted"

SuccessAfterRequiredSyncs ==
    /\ state.creationOutcome = "created" =>
        /\ state.creationFileSynced
        /\ state.creationRenamed
        /\ state.creationDirSynced
        /\ state.unitVisible
    /\ state.checkpointOutcome = "committed" =>
        /\ state.checkpointStage = "dir-synced"
        /\ state.checkpointFileSynced
        /\ state.checkpointDirSynced

CheckpointOutcomeMatchesRecoveredState ==
    /\ state.checkpointOutcome = "committed" => NewCheckpointImage
    /\ state.checkpointOutcome = "not-committed" => OldCheckpointImage
    /\ state.checkpointOutcome = "unknown" =>
        \/ state.partitionState = "checkpoint-indeterminate"
        \/ OldCheckpointImage
        \/ NewCheckpointImage

CreationOutcomeMatchesRecoveredState ==
    /\ state.creationOutcome = "created" =>
        /\ state.creationStage = "created"
        /\ state.unitVisible
        /\ state.manifestValid
        /\ state.ownerEpoch > 0
        /\ state.ownerRecordValid
        /\ state.ownerLinked
    /\ state.creationOutcome = "not-committed" =>
        /\ ~state.unitVisible
        /\ state.ownerEpoch = 0
    /\ state.creationOutcome = "unknown" =>
        state.creationStage \in {"unknown", "recovered-old", "recovered-new"}

DuplicateDoesNotAdvanceUnackedFrontier ==
    state.duplicateObserved =>
        /\ state.redeliveryObserved
        /\ (\/ state.unackedFrontier =
                (IF state.checkpointPresentBefore
                 THEN state.checkpointBefore + 1
                 ELSE state.resolvedStart)
            \/ /\ state.checkpointOutcome = "committed"
               /\ state.lastCheckpointAttempt = state.deliveryAttempt
               /\ state.lastCheckpointOwnerEpoch = state.ownerEpoch)

LivenessWitnessAbsent == ~state.liveDelivered

DuplicateAckWitnessAbsent ==
    ~(state.duplicateObserved
      /\ state.checkpointOutcome = "committed"
      /\ state.lastCheckpointAttempt = 2
      /\ state.unackedFrontier = 2)

ZeroCheckpointWitnessAbsent ==
    ~(state.scenario = "checkpoint-zero-commit"
      /\ state.checkpointPresent
      /\ state.checkpoint = 0
      /\ state.checkpointOutcome = "committed"
      /\ state.checkpointStage = "dir-synced"
      /\ state.unackedFrontier = 1)

PresentOldCheckpointWitnessAbsent ==
    ~(state.scenario = "checkpoint-present-unknown-old"
      /\ state.lastEvent = "checkpoint-recovered-old"
      /\ state.ownerEpoch = 2
      /\ state.checkpointPresent
      /\ state.checkpoint = 0
      /\ state.lastCheckpointOwnerEpoch = 1
      /\ state.lastCheckpointAttempt = 1
      /\ state.lastCheckpointOffset = 0)

PresentNewCheckpointWitnessAbsent ==
    ~(state.scenario = "checkpoint-present-unknown-new"
      /\ state.lastEvent = "checkpoint-recovered-new"
      /\ state.ownerEpoch = 2
      /\ state.checkpointPresent
      /\ state.checkpoint = 1
      /\ state.lastCheckpointOwnerEpoch = 2
      /\ state.lastCheckpointAttempt = 1
      /\ state.lastCheckpointOffset = 1)

=============================================================================
