-------------------------- MODULE LifecycleState --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Int;
    MaxCapacity,
    \* @type: Int;
    MaxSequence,
    \* @type: Str;
    UnsafeMode

Categories == {"payload", "in-flight", "journal", "identity", "queue", "concurrency"}
CapacityLimits == {"count", "byte"}
Identities == 1..2
Frames == 1..MaxSequence
Operations == {"send", "checkpoint", "worker", "compaction"}

LifecycleStates == {"starting", "ready", "draining", "closed"}
ReadinessStates == {"available", "unavailable", "recovery-required"}
TransportStates == {"open", "closed"}
SendPhases == {"idle", "prepared", "append-invoked", "terminal"}
SendOutcomes == {"none", "not-submitted", "indeterminate", "os-synced", "shutdown-cancelled"}
CheckpointPhases == {"idle", "prewrite", "known-old", "install-unknown", "confirmed"}
CheckpointOutcomes == {"none", "not-committed", "unknown", "committed", "shutdown-cancelled"}
CompactionStages == {
    "idle", "barrier", "base-written", "base-synced", "suffix-synced",
    "generation-dir-synced", "root-written", "root-file-synced",
    "root-renamed", "root-dir-synced", "cutover",
    "old-gc", "crashed", "failed"
}
IdentityGcStages == {"kept", "marked", "frame-synced", "installed"}
OldCrashStages == {
    "barrier", "base-written", "base-synced", "suffix-synced",
    "generation-dir-synced", "root-written", "root-file-synced", "root-renamed"
}
NewCrashStages == {"root-dir-synced", "cutover"}

CapacityScenarios == {"capacity-admit", "capacity-release", "capacity-exhaustion"}
IdentityScenarios == {"identity-gc", "identity-clock-rollback"}
CompactionScenarios == {
    "compaction-pre-barrier", "compaction-post-barrier", "compaction-crash-old",
    "compaction-crash-new", "compaction-stage-failure"
}
SendShutdownScenarios == {
    "shutdown-send-prepared", "shutdown-send-invoked", "shutdown-send-terminal"
}
CheckpointShutdownScenarios == {
    "shutdown-checkpoint-prewrite", "shutdown-checkpoint-known-old",
    "shutdown-checkpoint-unknown", "shutdown-checkpoint-confirmed"
}
ShutdownScenarios == SendShutdownScenarios \cup CheckpointShutdownScenarios
    \cup {"shutdown-worker", "shutdown-repeat", "shutdown-registration"}
ScenarioIds == CapacityScenarios \cup IdentityScenarios \cup CompactionScenarios
    \cup ShutdownScenarios
    \cup {"startup-reserve", "startup-no-reserve", "readiness-orthogonal"}

UnsafeModes == {
    "none", "count-overflow", "byte-overflow", "capacity-continues",
    "live-eviction", "gc-before-checkpoint", "gc-before-oldest",
    "gc-before-age", "gc-after-clock-rollback", "gc-undurable",
    "startup-without-reserve", "barrier-loses-frame", "mixed-recovery",
    "barrier-misroutes-frame", "reserve-reallocated", "cutover-before-sync",
    "old-gc-before-root", "admission-after-drain",
    "axes-coupled", "freeze-misses-operation", "send-cancellation-overwrite",
    "checkpoint-cancellation-overwrite", "close-before-join",
    "repeat-report-changes", "repeat-reruns-side-effects", "terminal-outcome-overwrite",
    "cancellation-advances-frontier"
}

ZeroCapacity == [c \in Categories |-> 0]
ZeroIdentityBool == [i \in Identities |-> FALSE]
ZeroIdentityStage == [i \in Identities |-> "kept"]

IsShutdownScenario(s) == s \in ShutdownScenarios
IsCompactionScenario(s) == s \in CompactionScenarios
IsSendTerminal(o) == o \in (SendOutcomes \ {"none"})
IsCheckpointTerminal(o) == o \in (CheckpointOutcomes \ {"none"})

ExpectedSendOutcome(phase, existing) ==
    IF existing # "none" THEN existing
    ELSE IF phase \in {"idle", "prepared"} THEN "not-submitted"
    ELSE "indeterminate"

ExpectedCheckpointOutcome(phase, existing) ==
    IF existing # "none" THEN existing
    ELSE IF phase \in {"idle", "prewrite", "known-old"} THEN "not-committed"
    ELSE IF phase = "install-unknown" THEN "unknown"
    ELSE "committed"

SendOutcomeCode(o) ==
    CASE o = "none" -> 0
      [] o = "not-submitted" -> 1
      [] o = "indeterminate" -> 2
      [] o = "os-synced" -> 3
      [] OTHER -> 4

CheckpointOutcomeCode(o) ==
    CASE o = "none" -> 0
      [] o = "not-committed" -> 1
      [] o = "unknown" -> 2
      [] o = "committed" -> 3
      [] OTHER -> 4

VARIABLES
    \* @type: {
    \* scenario: Str, selectedCategory: Str, selectedLimit: Str, selectedIdentity: Int,
    \* selectedCrashStage: Str, step: Int,
    \* counts: Str -> Int, bytes: Str -> Int,
    \* reserveAvailable: Bool, reserveHeld: Bool, capacityRejected: Bool,
    \* admissionOpen: Bool, pollOpen: Bool, liveRecords: Int,
    \* evictedLiveState: Bool,
    \* identityCheckpointed: Int -> Bool,
    \* identityBeforeOldest: Int -> Bool,
    \* identityRetryAgeElapsed: Int -> Bool,
    \* identityRemoved: Int -> Bool, identityGcDurable: Int -> Bool,
    \* identityGcStage: Int -> Str,
    \* gcCapturedCheckpointed: Int -> Bool,
    \* gcCapturedBeforeOldest: Int -> Bool,
    \* gcCapturedRetryAge: Int -> Bool,
    \* gcCapturedClockSafe: Int -> Bool,
    \* clock: Int, clockHighWater: Int, clockRolledBack: Bool,
    \* compactionStage: Str, compactionCutoff: Int,
    \* committedFrames: Set(Int), oldFrames: Set(Int),
    \* oldCompleteFrames: Set(Int), newBaseFrames: Set(Int),
    \* newSuffixFrames: Set(Int), activeFrames: Set(Int),
    \* generation: Int, rootGeneration: Int,
    \* baseDurable: Bool, suffixDurable: Bool,
    \* rootRenamed: Bool, rootDirectorySynced: Bool,
    \* oldGenerationGc: Bool, recovered: Bool, failStop: Bool,
    \* mutationInFlight: Bool, mutationRegisteredBeforeBarrier: Bool,
    \* compactionReserveHeld: Bool,
    \* lifecycle: Str, readiness: Str, partitionFault: Bool,
    \* lifecycleGeneration: Int, admissionGeneration: Int,
    \* drainStarted: Bool, shutdownFrozen: Bool,
    \* startedOperations: Set(Str), startedAtDrain: Set(Str),
    \* frozenOperations: Set(Str), handlesFenced: Bool,
    \* deadlineReached: Bool, cancellationIssued: Bool,
    \* transport: Str, workersJoined: Bool,
    \* sendPhase: Str, sendPhaseAtDeadline: Str,
    \* sendOutcome: Str, sendOutcomeAtFreeze: Str,
    \* checkpointPhase: Str, checkpointPhaseAtDeadline: Str,
    \* checkpointOutcome: Str, checkpointOutcomeAtFreeze: Str,
    \* frontier: Int, frontierAtFreeze: Int,
    \* shutdownClassified: Bool, terminalReportCached: Bool,
    \* terminalReportDigest: Int, repeatedReportDigest: Int,
    \* transportCloseCount: Int, stateFinalizeCount: Int,
    \* repeatCount: Int, axesCoupled: Bool, lastEvent: Str};
    state

Init ==
    \E selected \in ScenarioIds:
    \E category \in IF selected \in CapacityScenarios THEN Categories ELSE {"payload"}:
    \E limit \in IF selected = "capacity-exhaustion" THEN CapacityLimits ELSE {"count"}:
    \E identity \in IF selected \in IdentityScenarios THEN Identities ELSE {1}:
    \E crashStage \in
        IF selected \in {"compaction-crash-old", "compaction-stage-failure"}
        THEN OldCrashStages
        ELSE IF selected = "compaction-crash-new" THEN NewCrashStages
        ELSE {"barrier"}:
    LET isStartup == selected \in {"startup-reserve", "startup-no-reserve"} IN
    LET isIdentity == selected \in IdentityScenarios IN
    LET identityEligible == selected \in {"identity-gc", "identity-clock-rollback"} IN
    LET sendPhase0 ==
        CASE selected = "shutdown-send-prepared" -> "prepared"
          [] selected = "shutdown-send-invoked" -> "append-invoked"
          [] selected \in {"shutdown-send-terminal", "shutdown-repeat"} -> "terminal"
          [] OTHER -> "idle"
    IN
    LET sendOutcome0 ==
        IF selected \in {"shutdown-send-terminal", "shutdown-repeat"}
        THEN "os-synced" ELSE "none"
    IN
    LET checkpointPhase0 ==
        CASE selected = "shutdown-checkpoint-prewrite" -> "prewrite"
          [] selected = "shutdown-checkpoint-known-old" -> "known-old"
          [] selected = "shutdown-checkpoint-unknown" -> "install-unknown"
          [] selected \in {"shutdown-checkpoint-confirmed", "shutdown-repeat"} -> "confirmed"
          [] OTHER -> "idle"
    IN
    LET checkpointOutcome0 ==
        IF selected \in {"shutdown-checkpoint-confirmed", "shutdown-repeat"}
        THEN "committed" ELSE "none"
    IN
    LET started0 ==
        CASE selected \in SendShutdownScenarios -> {"send", "worker"}
          [] selected \in CheckpointShutdownScenarios -> {"checkpoint", "worker"}
          [] selected = "shutdown-worker" -> {"worker"}
          [] selected = "shutdown-repeat" -> {"send", "checkpoint", "worker"}
          [] OTHER -> {}
    IN
    state = [
        scenario |-> selected,
        selectedCategory |-> category,
        selectedLimit |-> limit,
        selectedIdentity |-> identity,
        selectedCrashStage |-> crashStage,
        step |-> 0,
        counts |-> ZeroCapacity,
        bytes |-> ZeroCapacity,
        reserveAvailable |-> selected # "startup-no-reserve",
        reserveHeld |-> ~isStartup,
        capacityRejected |-> FALSE,
        admissionOpen |-> ~isStartup,
        pollOpen |-> ~isStartup,
        liveRecords |-> 1,
        evictedLiveState |-> FALSE,
        identityCheckpointed |-> [i \in Identities |-> i = identity /\ identityEligible],
        identityBeforeOldest |-> [i \in Identities |-> i = identity /\ identityEligible],
        identityRetryAgeElapsed |-> [i \in Identities |-> i = identity /\ identityEligible],
        identityRemoved |-> ZeroIdentityBool,
        identityGcDurable |-> ZeroIdentityBool,
        identityGcStage |-> ZeroIdentityStage,
        gcCapturedCheckpointed |-> ZeroIdentityBool,
        gcCapturedBeforeOldest |-> ZeroIdentityBool,
        gcCapturedRetryAge |-> ZeroIdentityBool,
        gcCapturedClockSafe |-> ZeroIdentityBool,
        clock |-> IF isIdentity THEN 2 ELSE 0,
        clockHighWater |-> IF isIdentity THEN 2 ELSE 0,
        clockRolledBack |-> FALSE,
        compactionStage |-> "idle",
        compactionCutoff |-> 0,
        committedFrames |-> {1},
        oldFrames |-> {1},
        oldCompleteFrames |-> {1},
        newBaseFrames |-> {},
        newSuffixFrames |-> {},
        activeFrames |-> {1},
        generation |-> 1,
        rootGeneration |-> 1,
        baseDurable |-> FALSE,
        suffixDurable |-> FALSE,
        rootRenamed |-> FALSE,
        rootDirectorySynced |-> FALSE,
        oldGenerationGc |-> FALSE,
        recovered |-> FALSE,
        failStop |-> FALSE,
        mutationInFlight |-> FALSE,
        mutationRegisteredBeforeBarrier |-> FALSE,
        compactionReserveHeld |-> TRUE,
        lifecycle |-> IF isStartup THEN "starting" ELSE "ready",
        readiness |-> IF isStartup THEN "unavailable" ELSE "available",
        partitionFault |-> FALSE,
        lifecycleGeneration |-> 1,
        admissionGeneration |-> 1,
        drainStarted |-> FALSE,
        shutdownFrozen |-> FALSE,
        startedOperations |-> started0,
        startedAtDrain |-> {},
        frozenOperations |-> {},
        handlesFenced |-> FALSE,
        deadlineReached |-> FALSE,
        cancellationIssued |-> FALSE,
        transport |-> "open",
        workersJoined |-> FALSE,
        sendPhase |-> sendPhase0,
        sendPhaseAtDeadline |-> "idle",
        sendOutcome |-> sendOutcome0,
        sendOutcomeAtFreeze |-> "none",
        checkpointPhase |-> checkpointPhase0,
        checkpointPhaseAtDeadline |-> "idle",
        checkpointOutcome |-> checkpointOutcome0,
        checkpointOutcomeAtFreeze |-> "none",
        frontier |-> IF checkpointOutcome0 = "committed" THEN 1 ELSE 0,
        frontierAtFreeze |-> 0,
        shutdownClassified |-> FALSE,
        terminalReportCached |-> FALSE,
        terminalReportDigest |-> 0,
        repeatedReportDigest |-> 0,
        transportCloseCount |-> 0,
        stateFinalizeCount |-> 0,
        repeatCount |-> 0,
        axesCoupled |-> FALSE,
        lastEvent |-> "init"
    ]

ReserveStartup ==
    /\ state.scenario = "startup-reserve"
    /\ state.step = 0
    /\ state.reserveAvailable
    /\ state' = [state EXCEPT
        !.reserveHeld = TRUE,
        !.step = 1,
        !.lastEvent = "reserve-startup"]

BecomeReady ==
    /\ state.scenario = "startup-reserve"
    /\ state.step = 1
    /\ state.reserveHeld
    /\ state' = [state EXCEPT
        !.lifecycle = "ready",
        !.readiness = "available",
        !.admissionOpen = TRUE,
        !.pollOpen = TRUE,
        !.step = 2,
        !.lastEvent = "ready"]

StartupUnavailable ==
    /\ state.scenario = "startup-no-reserve"
    /\ state.step = 0
    /\ ~state.reserveAvailable
    /\ state' = [state EXCEPT
        !.readiness = "unavailable",
        !.admissionOpen = FALSE,
        !.pollOpen = FALSE,
        !.step = 1,
        !.lastEvent = "startup-unavailable"]

AdmitState(c) ==
    /\ state.scenario \in CapacityScenarios
    /\ c = state.selectedCategory
    /\ state.step = 0
    /\ state.admissionOpen
    /\ state.counts[c] < MaxCapacity
    /\ state.bytes[c] < MaxCapacity
    /\ state' = [state EXCEPT
        !.counts[c] = @ + 1,
        !.bytes[c] = @ + 1,
        !.step = 1,
        !.lastEvent = "admit"]

FillCategory(c) ==
    /\ state.scenario = "capacity-release"
    /\ c = state.selectedCategory
    /\ state.step = 1
    /\ state' = [state EXCEPT
        !.counts[c] = MaxCapacity,
        !.bytes[c] = MaxCapacity,
        !.capacityRejected = TRUE,
        !.admissionOpen = FALSE,
        !.pollOpen = FALSE,
        !.step = 2,
        !.lastEvent = "fill-capacity"]

ReleaseState(c) ==
    /\ state.scenario = "capacity-release"
    /\ c = state.selectedCategory
    /\ state.step = 2
    /\ state.counts[c] > 0
    /\ state.bytes[c] > 0
    /\ state.capacityRejected
    /\ state' = [state EXCEPT
        !.counts[c] = @ - 1,
        !.bytes[c] = @ - 1,
        !.capacityRejected = FALSE,
        !.admissionOpen = TRUE,
        !.pollOpen = TRUE,
        !.step = 3,
        !.lastEvent = "release"]

RejectCapacity(c) ==
    /\ state.scenario = "capacity-exhaustion"
    /\ c = state.selectedCategory
    /\ state.step = 1
    /\ CASE state.selectedLimit = "count" -> state.counts[c] < MaxCapacity
          [] OTHER -> state.bytes[c] < MaxCapacity
    /\ state' = [state EXCEPT
        !.counts[c] = IF state.selectedLimit = "count" THEN MaxCapacity ELSE @,
        !.bytes[c] = IF state.selectedLimit = "byte" THEN MaxCapacity ELSE @,
        !.capacityRejected = TRUE,
        !.admissionOpen = FALSE,
        !.pollOpen = FALSE,
        !.step = 3,
        !.lastEvent = "capacity-rejected"]

AdvanceClock ==
    /\ state.scenario = "identity-gc"
    /\ state.step = 0
    /\ state.clock < 3
    /\ state' = [state EXCEPT
        !.clock = @ + 1,
        !.clockHighWater = @ + 1,
        !.step = 1,
        !.lastEvent = "clock-advance"]

RollbackClock ==
    /\ state.scenario = "identity-clock-rollback"
    /\ state.step = 0
    /\ state.clock > 0
    /\ state' = [state EXCEPT
        !.clock = @ - 1,
        !.clockRolledBack = TRUE,
        !.step = 1,
        !.lastEvent = "clock-rollback"]

StopPollOnClockUncertainty ==
    /\ state.scenario = "identity-clock-rollback"
    /\ state.step = 1
    /\ state.clockRolledBack
    /\ state' = [state EXCEPT
        !.capacityRejected = TRUE,
        !.admissionOpen = FALSE,
        !.pollOpen = FALSE,
        !.step = 2,
        !.lastEvent = "gc-deferred"]

MarkIdentityGc(i) ==
    /\ state.scenario = "identity-gc"
    /\ i = state.selectedIdentity
    /\ state.step = 1
    /\ state.identityCheckpointed[i]
    /\ state.identityBeforeOldest[i]
    /\ state.identityRetryAgeElapsed[i]
    /\ ~state.clockRolledBack
    /\ state.clock = state.clockHighWater
    /\ state' = [state EXCEPT
        !.identityGcStage[i] = "marked",
        !.gcCapturedCheckpointed[i] = state.identityCheckpointed[i],
        !.gcCapturedBeforeOldest[i] = state.identityBeforeOldest[i],
        !.gcCapturedRetryAge[i] = state.identityRetryAgeElapsed[i],
        !.gcCapturedClockSafe[i] = TRUE,
        !.step = 2,
        !.lastEvent = "gc-mark"]

SyncIdentityGc(i) ==
    /\ state.scenario = "identity-gc"
    /\ i = state.selectedIdentity
    /\ state.step = 2
    /\ state.identityGcStage[i] = "marked"
    /\ state' = [state EXCEPT
        !.identityGcStage[i] = "frame-synced",
        !.identityGcDurable[i] = TRUE,
        !.step = 3,
        !.lastEvent = "gc-sync"]

InstallIdentityGc(i) ==
    /\ state.scenario = "identity-gc"
    /\ i = state.selectedIdentity
    /\ state.step = 3
    /\ state.identityGcStage[i] = "frame-synced"
    /\ state.identityGcDurable[i]
    /\ state' = [state EXCEPT
        !.identityGcStage[i] = "installed",
        !.identityRemoved[i] = TRUE,
        !.step = 4,
        !.lastEvent = "gc-install"]

RegisterPreBarrierMutation ==
    /\ state.scenario = "compaction-pre-barrier"
    /\ state.compactionStage = "idle"
    /\ state' = [state EXCEPT
        !.mutationInFlight = TRUE,
        !.mutationRegisteredBeforeBarrier = TRUE,
        !.step = 1,
        !.lastEvent = "mutation-register"]

AcquireGenerationBarrier ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "idle"
    /\ (state.scenario # "compaction-pre-barrier" \/ state.mutationInFlight)
    /\ state.compactionReserveHeld
    /\ state' = [state EXCEPT
        !.compactionStage = "barrier",
        !.compactionCutoff = 1,
        !.step = @ + 1,
        !.lastEvent = "barrier-acquire"]

CommitRegisteredMutation ==
    /\ state.scenario = "compaction-pre-barrier"
    /\ state.compactionStage = "barrier"
    /\ state.mutationInFlight
    /\ state.mutationRegisteredBeforeBarrier
    /\ state' = [state EXCEPT
        !.committedFrames = @ \cup {2},
        !.oldFrames = @ \cup {2},
        !.oldCompleteFrames = @ \cup {2},
        !.mutationInFlight = FALSE,
        !.compactionCutoff = 2,
        !.step = @ + 1,
        !.lastEvent = "mutation-terminal"]

WriteCompactionBase ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "barrier"
    /\ ~state.mutationInFlight
    /\ state' = [state EXCEPT
        !.newBaseFrames = state.committedFrames,
        !.compactionStage = "base-written",
        !.step = @ + 1,
        !.lastEvent = "base-write"]

SyncCompactionBase ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "base-written"
    /\ state' = [state EXCEPT
        !.baseDurable = TRUE,
        !.compactionStage = "base-synced",
        !.step = @ + 1,
        !.lastEvent = "base-sync"]

SyncEmptySuffix ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "base-synced"
    /\ state.baseDurable
    /\ state' = [state EXCEPT
        !.suffixDurable = TRUE,
        !.compactionStage = "suffix-synced",
        !.step = @ + 1,
        !.lastEvent = "suffix-sync"]

SyncGenerationDirectory ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "suffix-synced"
    /\ state.baseDurable
    /\ state.suffixDurable
    /\ state' = [state EXCEPT
        !.compactionStage = "generation-dir-synced",
        !.step = @ + 1,
        !.lastEvent = "generation-dir-sync"]

WriteRootPointerTemp ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "generation-dir-synced"
    /\ state' = [state EXCEPT
        !.compactionStage = "root-written",
        !.step = @ + 1,
        !.lastEvent = "root-write"]

SyncRootPointerFile ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "root-written"
    /\ state' = [state EXCEPT
        !.compactionStage = "root-file-synced",
        !.step = @ + 1,
        !.lastEvent = "root-file-sync"]

RenameRootPointer ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "root-file-synced"
    /\ state' = [state EXCEPT
        !.rootRenamed = TRUE,
        !.compactionStage = "root-renamed",
        !.step = @ + 1,
        !.lastEvent = "root-rename"]

SyncRootDirectory ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "root-renamed"
    /\ state.rootRenamed
    /\ state' = [state EXCEPT
        !.rootDirectorySynced = TRUE,
        !.rootGeneration = 2,
        !.compactionStage = "root-dir-synced",
        !.step = @ + 1,
        !.lastEvent = "root-dir-sync"]

AppendPostBarrierFrame ==
    /\ state.scenario = "compaction-post-barrier"
    /\ state.compactionStage = "root-dir-synced"
    /\ state.rootDirectorySynced
    /\ state' = [state EXCEPT
        !.committedFrames = @ \cup {2},
        !.newSuffixFrames = @ \cup {2},
        !.step = @ + 1,
        !.lastEvent = "post-barrier-append"]

ActivateNewGeneration ==
    /\ IsCompactionScenario(state.scenario)
    /\ state.compactionStage = "root-dir-synced"
    /\ (state.scenario # "compaction-post-barrier" \/ 2 \in state.newSuffixFrames)
    /\ state.rootDirectorySynced
    /\ state' = [state EXCEPT
        !.activeFrames = state.newBaseFrames \cup state.newSuffixFrames,
        !.generation = 2,
        !.compactionStage = "cutover",
        !.step = @ + 1,
        !.lastEvent = "cutover"]

GarbageCollectOldGeneration ==
    /\ state.scenario \in {"compaction-pre-barrier", "compaction-post-barrier"}
    /\ state.compactionStage = "cutover"
    /\ state.rootDirectorySynced
    /\ state.committedFrames \subseteq (state.newBaseFrames \cup state.newSuffixFrames)
    /\ state' = [state EXCEPT
        !.oldFrames = {},
        !.oldGenerationGc = TRUE,
        !.compactionStage = "old-gc",
        !.step = @ + 1,
        !.lastEvent = "old-gc"]

CrashCompactionOld ==
    /\ state.scenario = "compaction-crash-old"
    /\ state.compactionStage = state.selectedCrashStage
    /\ state.selectedCrashStage \in OldCrashStages
    /\ state' = [state EXCEPT
        !.compactionStage = "crashed",
        !.lastEvent = "compaction-crash-old"]

RecoverOldGeneration ==
    /\ state.scenario = "compaction-crash-old"
    /\ state.compactionStage = "crashed"
    /\ state' = [state EXCEPT
        !.activeFrames = state.oldCompleteFrames,
        !.generation = 1,
        !.rootGeneration = 1,
        !.newBaseFrames = {},
        !.newSuffixFrames = {},
        !.baseDurable = FALSE,
        !.suffixDurable = FALSE,
        !.rootRenamed = FALSE,
        !.rootDirectorySynced = FALSE,
        !.recovered = TRUE,
        !.step = @ + 1,
        !.lastEvent = "recover-old"]

CrashCompactionNew ==
    /\ state.scenario = "compaction-crash-new"
    /\ state.compactionStage = state.selectedCrashStage
    /\ state.selectedCrashStage \in NewCrashStages
    /\ state' = [state EXCEPT
        !.compactionStage = "crashed",
        !.lastEvent = "compaction-crash-new"]

RecoverNewGeneration ==
    /\ state.scenario = "compaction-crash-new"
    /\ state.compactionStage = "crashed"
    /\ state.rootDirectorySynced
    /\ state' = [state EXCEPT
        !.activeFrames = state.newBaseFrames \cup state.newSuffixFrames,
        !.generation = 2,
        !.rootGeneration = 2,
        !.recovered = TRUE,
        !.step = @ + 1,
        !.lastEvent = "recover-new"]

FailCompactionStage ==
    /\ state.scenario = "compaction-stage-failure"
    /\ state.compactionStage = state.selectedCrashStage
    /\ state.selectedCrashStage \in OldCrashStages
    /\ state' = [state EXCEPT
        !.compactionStage = "failed",
        !.activeFrames = state.oldCompleteFrames,
        !.failStop = TRUE,
        !.admissionOpen = FALSE,
        !.pollOpen = FALSE,
        !.lastEvent = "compaction-fail-stop"]

RegisterOperation(op) ==
    /\ state.scenario = "shutdown-registration"
    /\ state.step = 0
    /\ op \in Operations
    /\ state.lifecycle = "ready"
    /\ state.admissionOpen
    /\ state.admissionGeneration = state.lifecycleGeneration
    /\ state' = [state EXCEPT
        !.startedOperations = @ \cup {op},
        !.step = 1,
        !.lastEvent = "operation-register"]

BeginShutdown ==
    /\ IsShutdownScenario(state.scenario)
    /\ state.lifecycle = "ready"
    /\ \/ state.step = 0
       \/ /\ state.scenario = "shutdown-registration"
          /\ state.step = 1
    /\ state' = [state EXCEPT
        !.lifecycle = "draining",
        !.lifecycleGeneration = @ + 1,
        !.admissionOpen = FALSE,
        !.pollOpen = FALSE,
        !.drainStarted = TRUE,
        !.step = @ + 1,
        !.lastEvent = "shutdown-begin"]

FreezeStartedOperations ==
    /\ IsShutdownScenario(state.scenario)
    /\ state.drainStarted
    /\ ~state.shutdownFrozen
    /\ state' = [state EXCEPT
        !.startedAtDrain = state.startedOperations,
        !.frozenOperations = state.startedOperations,
        !.shutdownFrozen = TRUE,
        !.handlesFenced = TRUE,
        !.sendOutcomeAtFreeze = state.sendOutcome,
        !.checkpointOutcomeAtFreeze = state.checkpointOutcome,
        !.frontierAtFreeze = state.frontier,
        !.step = @ + 1,
        !.lastEvent = "shutdown-freeze"]

ReachShutdownDeadline ==
    /\ IsShutdownScenario(state.scenario)
    /\ state.shutdownFrozen
    /\ ~state.deadlineReached
    /\ state' = [state EXCEPT
        !.deadlineReached = TRUE,
        !.shutdownClassified = TRUE,
        !.sendPhaseAtDeadline = state.sendPhase,
        !.checkpointPhaseAtDeadline = state.checkpointPhase,
        !.sendOutcome = ExpectedSendOutcome(state.sendPhase, state.sendOutcome),
        !.checkpointOutcome = ExpectedCheckpointOutcome(state.checkpointPhase, state.checkpointOutcome),
        !.step = @ + 1,
        !.lastEvent = "shutdown-deadline"]

CloseOwnedTransport ==
    /\ IsShutdownScenario(state.scenario)
    /\ state.deadlineReached
    /\ state.transport = "open"
    /\ state' = [state EXCEPT
        !.transport = "closed",
        !.transportCloseCount = @ + 1,
        !.step = @ + 1,
        !.lastEvent = "transport-close"]

CancelBlockedWorkers ==
    /\ IsShutdownScenario(state.scenario)
    /\ state.transport = "closed"
    /\ ~state.cancellationIssued
    /\ state' = [state EXCEPT
        !.cancellationIssued = TRUE,
        !.step = @ + 1,
        !.lastEvent = "worker-cancel"]

JoinRegisteredWorkers ==
    /\ IsShutdownScenario(state.scenario)
    /\ state.cancellationIssued
    /\ ~state.workersJoined
    /\ state' = [state EXCEPT
        !.workersJoined = TRUE,
        !.step = @ + 1,
        !.lastEvent = "worker-join"]

FinishShutdown ==
    /\ IsShutdownScenario(state.scenario)
    /\ state.lifecycle = "draining"
    /\ state.workersJoined
    /\ state.transport = "closed"
    /\ state' = [state EXCEPT
        !.lifecycle = "closed",
        !.terminalReportCached = TRUE,
        !.terminalReportDigest = SendOutcomeCode(state.sendOutcome) * 10
            + CheckpointOutcomeCode(state.checkpointOutcome),
        !.repeatedReportDigest = SendOutcomeCode(state.sendOutcome) * 10
            + CheckpointOutcomeCode(state.checkpointOutcome),
        !.stateFinalizeCount = @ + 1,
        !.step = @ + 1,
        !.lastEvent = "shutdown-closed"]

RepeatShutdown ==
    /\ state.scenario = "shutdown-repeat"
    /\ state.lifecycle = "closed"
    /\ state.terminalReportCached
    /\ state.repeatCount = 0
    /\ state' = [state EXCEPT
        !.repeatedReportDigest = state.terminalReportDigest,
        !.repeatCount = @ + 1,
        !.step = @ + 1,
        !.lastEvent = "shutdown-repeat"]

SetReadinessUnavailable ==
    /\ state.scenario = "readiness-orthogonal"
    /\ state.step = 0
    /\ state.lifecycle = "ready"
    /\ state' = [state EXCEPT
        !.readiness = "unavailable",
        !.step = 1,
        !.lastEvent = "readiness-unavailable"]

SetPartitionRecoveryRequired ==
    /\ state.scenario = "readiness-orthogonal"
    /\ state.step = 1
    /\ state.lifecycle = "ready"
    /\ state' = [state EXCEPT
        !.readiness = "recovery-required",
        !.partitionFault = TRUE,
        !.step = 2,
        !.lastEvent = "partition-recovery-required"]

UnsafeCountOverflow ==
    state' = [state EXCEPT
        !.counts[state.selectedCategory] = MaxCapacity + 1,
        !.lastEvent = "unsafe-count-overflow"]

UnsafeByteOverflow ==
    state' = [state EXCEPT
        !.bytes[state.selectedCategory] = MaxCapacity + 1,
        !.lastEvent = "unsafe-byte-overflow"]

UnsafeCapacityContinues ==
    state' = [state EXCEPT
        !.counts[state.selectedCategory] =
            IF state.selectedLimit = "count" THEN MaxCapacity ELSE @,
        !.bytes[state.selectedCategory] =
            IF state.selectedLimit = "byte" THEN MaxCapacity ELSE @,
        !.capacityRejected = FALSE,
        !.admissionOpen = TRUE,
        !.pollOpen = TRUE,
        !.lastEvent = "unsafe-capacity-continues"]

UnsafeLiveEviction ==
    state' = [state EXCEPT
        !.evictedLiveState = TRUE,
        !.lastEvent = "unsafe-live-eviction"]

UnsafeGcCaptured(checkpointed, beforeOldest, age, clockSafe, durable) ==
    LET i == state.selectedIdentity IN
    state' = [state EXCEPT
        !.identityRemoved[i] = TRUE,
        !.identityGcDurable[i] = durable,
        !.identityGcStage[i] = "installed",
        !.gcCapturedCheckpointed[i] = checkpointed,
        !.gcCapturedBeforeOldest[i] = beforeOldest,
        !.gcCapturedRetryAge[i] = age,
        !.gcCapturedClockSafe[i] = clockSafe,
        !.clockRolledBack = ~clockSafe,
        !.lastEvent = "unsafe-gc"]

UnsafeStartupWithoutReserve ==
    state' = [state EXCEPT
        !.reserveAvailable = FALSE,
        !.reserveHeld = FALSE,
        !.lifecycle = "ready",
        !.readiness = "available",
        !.lastEvent = "unsafe-startup-ready"]

UnsafeBarrierLosesFrame ==
    state' = [state EXCEPT
        !.committedFrames = {1, 2},
        !.oldFrames = {},
        !.oldCompleteFrames = {1, 2},
        !.newBaseFrames = {1},
        !.newSuffixFrames = {},
        !.activeFrames = {1},
        !.oldGenerationGc = TRUE,
        !.rootDirectorySynced = TRUE,
        !.rootGeneration = 2,
        !.generation = 2,
        !.compactionStage = "old-gc",
        !.lastEvent = "unsafe-barrier-loss"]

UnsafeMixedRecovery ==
    state' = [state EXCEPT
        !.oldCompleteFrames = {1},
        !.newBaseFrames = {1},
        !.newSuffixFrames = {},
        !.activeFrames = {2},
        !.recovered = TRUE,
        !.compactionStage = "crashed",
        !.lastEvent = "unsafe-mixed-recovery"]

UnsafeBarrierMisroutesFrame ==
    state' = [state EXCEPT
        !.committedFrames = {1, 2},
        !.compactionCutoff = 2,
        !.newBaseFrames = {1},
        !.baseDurable = TRUE,
        !.compactionStage = "base-synced",
        !.lastEvent = "unsafe-barrier-misroute"]

UnsafeReserveReallocated ==
    state' = [state EXCEPT
        !.compactionStage = "barrier",
        !.compactionReserveHeld = FALSE,
        !.lastEvent = "unsafe-reserve-reallocated"]

UnsafeCutoverBeforeSync ==
    state' = [state EXCEPT
        !.generation = 2,
        !.rootGeneration = 2,
        !.rootRenamed = TRUE,
        !.rootDirectorySynced = FALSE,
        !.baseDurable = FALSE,
        !.suffixDurable = FALSE,
        !.compactionStage = "cutover",
        !.lastEvent = "unsafe-cutover-before-sync"]

UnsafeOldGcBeforeRoot ==
    state' = [state EXCEPT
        !.oldFrames = {},
        !.oldGenerationGc = TRUE,
        !.rootDirectorySynced = FALSE,
        !.rootGeneration = 1,
        !.compactionStage = "old-gc",
        !.lastEvent = "unsafe-old-gc-before-root"]

UnsafeAdmissionAfterDrain ==
    state' = [state EXCEPT
        !.lifecycle = "draining",
        !.drainStarted = TRUE,
        !.admissionOpen = TRUE,
        !.lifecycleGeneration = 2,
        !.admissionGeneration = 2,
        !.lastEvent = "unsafe-admission-after-drain"]

UnsafeAxesCoupled ==
    state' = [state EXCEPT
        !.axesCoupled = TRUE,
        !.lastEvent = "unsafe-axes-coupled"]

UnsafeFreezeMissesOperation ==
    state' = [state EXCEPT
        !.lifecycle = "draining",
        !.drainStarted = TRUE,
        !.shutdownFrozen = TRUE,
        !.admissionOpen = FALSE,
        !.startedOperations = {"send"},
        !.startedAtDrain = {"send"},
        !.frozenOperations = {},
        !.lastEvent = "unsafe-freeze-miss"]

UnsafeSendCancellationOverwrite ==
    state' = [state EXCEPT
        !.shutdownClassified = TRUE,
        !.sendPhaseAtDeadline = "append-invoked",
        !.sendOutcomeAtFreeze = "none",
        !.sendOutcome = "shutdown-cancelled",
        !.lastEvent = "unsafe-send-cancel"]

UnsafeCheckpointCancellationOverwrite ==
    state' = [state EXCEPT
        !.shutdownClassified = TRUE,
        !.checkpointPhaseAtDeadline = "install-unknown",
        !.checkpointOutcomeAtFreeze = "none",
        !.checkpointOutcome = "shutdown-cancelled",
        !.lastEvent = "unsafe-checkpoint-cancel"]

UnsafeCloseBeforeJoin ==
    state' = [state EXCEPT
        !.lifecycle = "closed",
        !.transport = "closed",
        !.workersJoined = FALSE,
        !.lastEvent = "unsafe-close-before-join"]

UnsafeRepeatReportChanges ==
    state' = [state EXCEPT
        !.lifecycle = "closed",
        !.workersJoined = TRUE,
        !.terminalReportCached = TRUE,
        !.terminalReportDigest = 11,
        !.repeatedReportDigest = 12,
        !.repeatCount = 1,
        !.lastEvent = "unsafe-repeat-report"]

UnsafeRepeatRerunsSideEffects ==
    state' = [state EXCEPT
        !.lifecycle = "closed",
        !.workersJoined = TRUE,
        !.transport = "closed",
        !.terminalReportCached = TRUE,
        !.terminalReportDigest = 11,
        !.repeatedReportDigest = 11,
        !.repeatCount = 1,
        !.transportCloseCount = 2,
        !.stateFinalizeCount = 2,
        !.lastEvent = "unsafe-repeat-side-effects"]

UnsafeTerminalOutcomeOverwrite ==
    state' = [state EXCEPT
        !.shutdownFrozen = TRUE,
        !.sendOutcomeAtFreeze = "os-synced",
        !.sendOutcome = "shutdown-cancelled",
        !.lastEvent = "unsafe-terminal-overwrite"]

UnsafeCancellationAdvancesFrontier ==
    state' = [state EXCEPT
        !.shutdownFrozen = TRUE,
        !.shutdownClassified = TRUE,
        !.checkpointPhaseAtDeadline = "prewrite",
        !.checkpointOutcome = "not-committed",
        !.frontierAtFreeze = 0,
        !.frontier = 1,
        !.lastEvent = "unsafe-frontier-advance"]

UnsafeAction ==
    /\ state.step = 0
    /\ CASE UnsafeMode = "count-overflow" -> UnsafeCountOverflow
          [] UnsafeMode = "byte-overflow" -> UnsafeByteOverflow
          [] UnsafeMode = "capacity-continues" -> UnsafeCapacityContinues
          [] UnsafeMode = "live-eviction" -> UnsafeLiveEviction
          [] UnsafeMode = "gc-before-checkpoint" -> UnsafeGcCaptured(FALSE, TRUE, TRUE, TRUE, TRUE)
          [] UnsafeMode = "gc-before-oldest" -> UnsafeGcCaptured(TRUE, FALSE, TRUE, TRUE, TRUE)
          [] UnsafeMode = "gc-before-age" -> UnsafeGcCaptured(TRUE, TRUE, FALSE, TRUE, TRUE)
          [] UnsafeMode = "gc-after-clock-rollback" -> UnsafeGcCaptured(TRUE, TRUE, TRUE, FALSE, TRUE)
          [] UnsafeMode = "gc-undurable" -> UnsafeGcCaptured(TRUE, TRUE, TRUE, TRUE, FALSE)
          [] UnsafeMode = "startup-without-reserve" -> UnsafeStartupWithoutReserve
          [] UnsafeMode = "barrier-loses-frame" -> UnsafeBarrierLosesFrame
          [] UnsafeMode = "mixed-recovery" -> UnsafeMixedRecovery
          [] UnsafeMode = "barrier-misroutes-frame" -> UnsafeBarrierMisroutesFrame
          [] UnsafeMode = "reserve-reallocated" -> UnsafeReserveReallocated
          [] UnsafeMode = "cutover-before-sync" -> UnsafeCutoverBeforeSync
          [] UnsafeMode = "old-gc-before-root" -> UnsafeOldGcBeforeRoot
          [] UnsafeMode = "admission-after-drain" -> UnsafeAdmissionAfterDrain
          [] UnsafeMode = "axes-coupled" -> UnsafeAxesCoupled
          [] UnsafeMode = "freeze-misses-operation" -> UnsafeFreezeMissesOperation
          [] UnsafeMode = "send-cancellation-overwrite" -> UnsafeSendCancellationOverwrite
          [] UnsafeMode = "checkpoint-cancellation-overwrite" -> UnsafeCheckpointCancellationOverwrite
          [] UnsafeMode = "close-before-join" -> UnsafeCloseBeforeJoin
          [] UnsafeMode = "repeat-report-changes" -> UnsafeRepeatReportChanges
          [] UnsafeMode = "repeat-reruns-side-effects" -> UnsafeRepeatRerunsSideEffects
          [] UnsafeMode = "terminal-outcome-overwrite" -> UnsafeTerminalOutcomeOverwrite
          [] UnsafeMode = "cancellation-advances-frontier" -> UnsafeCancellationAdvancesFrontier
          [] OTHER -> /\ state' = state
                      /\ FALSE

Stutter == state' = state

Quiescent ==
    CASE state.scenario = "startup-reserve" -> state.step >= 2
      [] state.scenario = "startup-no-reserve" -> state.step >= 1
      [] state.scenario = "capacity-admit" -> state.step >= 1
      [] state.scenario = "capacity-release" -> state.step >= 3
      [] state.scenario = "capacity-exhaustion" -> state.step >= 3
      [] state.scenario = "identity-gc" -> state.step >= 4
      [] state.scenario = "identity-clock-rollback" -> state.step >= 2
      [] state.scenario \in {"compaction-pre-barrier", "compaction-post-barrier"} ->
            state.compactionStage = "old-gc"
      [] state.scenario \in {"compaction-crash-old", "compaction-crash-new"} ->
            state.recovered \/ state.compactionStage = "cutover"
      [] state.scenario = "compaction-stage-failure" ->
            state.failStop \/ state.compactionStage = "cutover"
      [] state.scenario = "shutdown-repeat" ->
            state.lifecycle = "closed" /\ state.repeatCount = 1
      [] state.scenario \in (ShutdownScenarios \ {"shutdown-repeat"}) ->
            state.lifecycle = "closed"
      [] state.scenario = "readiness-orthogonal" -> state.step >= 2
      [] OTHER -> FALSE

AllActions ==
    \/ ReserveStartup
    \/ BecomeReady
    \/ StartupUnavailable
    \/ \E c \in Categories: AdmitState(c)
    \/ \E c \in Categories: FillCategory(c)
    \/ \E c \in Categories: ReleaseState(c)
    \/ \E c \in Categories: RejectCapacity(c)
    \/ AdvanceClock
    \/ RollbackClock
    \/ StopPollOnClockUncertainty
    \/ \E i \in Identities: MarkIdentityGc(i)
    \/ \E i \in Identities: SyncIdentityGc(i)
    \/ \E i \in Identities: InstallIdentityGc(i)
    \/ RegisterPreBarrierMutation
    \/ AcquireGenerationBarrier
    \/ CommitRegisteredMutation
    \/ WriteCompactionBase
    \/ SyncCompactionBase
    \/ SyncEmptySuffix
    \/ SyncGenerationDirectory
    \/ WriteRootPointerTemp
    \/ SyncRootPointerFile
    \/ RenameRootPointer
    \/ SyncRootDirectory
    \/ AppendPostBarrierFrame
    \/ ActivateNewGeneration
    \/ GarbageCollectOldGeneration
    \/ CrashCompactionOld
    \/ RecoverOldGeneration
    \/ CrashCompactionNew
    \/ RecoverNewGeneration
    \/ FailCompactionStage
    \/ \E op \in Operations: RegisterOperation(op)
    \/ BeginShutdown
    \/ FreezeStartedOperations
    \/ ReachShutdownDeadline
    \/ CloseOwnedTransport
    \/ CancelBlockedWorkers
    \/ JoinRegisteredWorkers
    \/ FinishShutdown
    \/ RepeatShutdown
    \/ SetReadinessUnavailable
    \/ SetPartitionRecoveryRequired
    \/ UnsafeAction
    \/ /\ Quiescent
       /\ Stutter

CompactionScenarioNext ==
    \/ RegisterPreBarrierMutation
    \/ AcquireGenerationBarrier
    \/ CommitRegisteredMutation
    \/ WriteCompactionBase
    \/ SyncCompactionBase
    \/ SyncEmptySuffix
    \/ SyncGenerationDirectory
    \/ WriteRootPointerTemp
    \/ SyncRootPointerFile
    \/ RenameRootPointer
    \/ SyncRootDirectory
    \/ AppendPostBarrierFrame
    \/ ActivateNewGeneration
    \/ GarbageCollectOldGeneration
    \/ CrashCompactionOld
    \/ RecoverOldGeneration
    \/ CrashCompactionNew
    \/ RecoverNewGeneration
    \/ FailCompactionStage
    \/ Stutter

ShutdownScenarioNext ==
    \/ \E op \in Operations: RegisterOperation(op)
    \/ BeginShutdown
    \/ FreezeStartedOperations
    \/ ReachShutdownDeadline
    \/ CloseOwnedTransport
    \/ CancelBlockedWorkers
    \/ JoinRegisteredWorkers
    \/ FinishShutdown
    \/ RepeatShutdown
    \/ Stutter

ScenarioNext ==
    CASE state.scenario = "startup-reserve" -> ReserveStartup \/ BecomeReady \/ Stutter
      [] state.scenario = "startup-no-reserve" -> StartupUnavailable \/ Stutter
      [] state.scenario \in CapacityScenarios ->
            (\E c \in Categories: AdmitState(c) \/ FillCategory(c) \/ ReleaseState(c) \/ RejectCapacity(c)) \/ Stutter
      [] state.scenario = "identity-gc" ->
            AdvanceClock
            \/ (\E i \in Identities: MarkIdentityGc(i) \/ SyncIdentityGc(i) \/ InstallIdentityGc(i))
            \/ Stutter
      [] state.scenario = "identity-clock-rollback" -> RollbackClock \/ StopPollOnClockUncertainty \/ Stutter
      [] IsCompactionScenario(state.scenario) -> CompactionScenarioNext
      [] IsShutdownScenario(state.scenario) -> ShutdownScenarioNext
      [] state.scenario = "readiness-orthogonal" ->
            SetReadinessUnavailable \/ SetPartitionRecoveryRequired \/ Stutter
      [] OTHER -> Stutter

Next == AllActions

TypeOK ==
    /\ MaxCapacity = 2
    /\ MaxSequence = 3
    /\ UnsafeMode \in UnsafeModes
    /\ state.scenario \in ScenarioIds
    /\ state.selectedCategory \in Categories
    /\ state.selectedLimit \in CapacityLimits
    /\ state.selectedIdentity \in Identities
    /\ state.selectedCrashStage \in (OldCrashStages \cup NewCrashStages)
    /\ state.step \in 0..24
    /\ state.counts \in [Categories -> 0..(MaxCapacity + 1)]
    /\ state.bytes \in [Categories -> 0..(MaxCapacity + 1)]
    /\ state.reserveAvailable \in BOOLEAN
    /\ state.reserveHeld \in BOOLEAN
    /\ state.capacityRejected \in BOOLEAN
    /\ state.admissionOpen \in BOOLEAN
    /\ state.pollOpen \in BOOLEAN
    /\ state.liveRecords \in 0..2
    /\ state.evictedLiveState \in BOOLEAN
    /\ state.identityCheckpointed \in [Identities -> BOOLEAN]
    /\ state.identityBeforeOldest \in [Identities -> BOOLEAN]
    /\ state.identityRetryAgeElapsed \in [Identities -> BOOLEAN]
    /\ state.identityRemoved \in [Identities -> BOOLEAN]
    /\ state.identityGcDurable \in [Identities -> BOOLEAN]
    /\ state.identityGcStage \in [Identities -> IdentityGcStages]
    /\ state.gcCapturedCheckpointed \in [Identities -> BOOLEAN]
    /\ state.gcCapturedBeforeOldest \in [Identities -> BOOLEAN]
    /\ state.gcCapturedRetryAge \in [Identities -> BOOLEAN]
    /\ state.gcCapturedClockSafe \in [Identities -> BOOLEAN]
    /\ state.clock \in 0..4
    /\ state.clockHighWater \in 0..4
    /\ state.clockRolledBack \in BOOLEAN
    /\ state.compactionStage \in CompactionStages
    /\ state.compactionCutoff \in 0..MaxSequence
    /\ state.committedFrames \subseteq Frames
    /\ state.oldFrames \subseteq Frames
    /\ state.oldCompleteFrames \subseteq Frames
    /\ state.newBaseFrames \subseteq Frames
    /\ state.newSuffixFrames \subseteq Frames
    /\ state.activeFrames \subseteq Frames
    /\ state.generation \in 1..2
    /\ state.rootGeneration \in 1..2
    /\ state.baseDurable \in BOOLEAN
    /\ state.suffixDurable \in BOOLEAN
    /\ state.rootRenamed \in BOOLEAN
    /\ state.rootDirectorySynced \in BOOLEAN
    /\ state.oldGenerationGc \in BOOLEAN
    /\ state.recovered \in BOOLEAN
    /\ state.failStop \in BOOLEAN
    /\ state.mutationInFlight \in BOOLEAN
    /\ state.mutationRegisteredBeforeBarrier \in BOOLEAN
    /\ state.compactionReserveHeld \in BOOLEAN
    /\ state.lifecycle \in LifecycleStates
    /\ state.readiness \in ReadinessStates
    /\ state.partitionFault \in BOOLEAN
    /\ state.lifecycleGeneration \in 1..2
    /\ state.admissionGeneration \in 1..2
    /\ state.drainStarted \in BOOLEAN
    /\ state.shutdownFrozen \in BOOLEAN
    /\ state.startedOperations \subseteq Operations
    /\ state.startedAtDrain \subseteq Operations
    /\ state.frozenOperations \subseteq Operations
    /\ state.handlesFenced \in BOOLEAN
    /\ state.deadlineReached \in BOOLEAN
    /\ state.cancellationIssued \in BOOLEAN
    /\ state.transport \in TransportStates
    /\ state.workersJoined \in BOOLEAN
    /\ state.sendPhase \in SendPhases
    /\ state.sendPhaseAtDeadline \in SendPhases
    /\ state.sendOutcome \in SendOutcomes
    /\ state.sendOutcomeAtFreeze \in SendOutcomes
    /\ state.checkpointPhase \in CheckpointPhases
    /\ state.checkpointPhaseAtDeadline \in CheckpointPhases
    /\ state.checkpointOutcome \in CheckpointOutcomes
    /\ state.checkpointOutcomeAtFreeze \in CheckpointOutcomes
    /\ state.frontier \in 0..1
    /\ state.frontierAtFreeze \in 0..1
    /\ state.shutdownClassified \in BOOLEAN
    /\ state.terminalReportCached \in BOOLEAN
    /\ state.terminalReportDigest \in 0..44
    /\ state.repeatedReportDigest \in 0..44
    /\ state.transportCloseCount \in 0..2
    /\ state.stateFinalizeCount \in 0..2
    /\ state.repeatCount \in 0..1
    /\ state.axesCoupled \in BOOLEAN

StateNeverExceedsHardBound ==
    \A c \in Categories:
        /\ state.counts[c] <= MaxCapacity
        /\ state.bytes[c] <= MaxCapacity

CapacityNeverEvictsLiveState ==
    state.liveRecords > 0 => ~state.evictedLiveState

CapacityExhaustionStopsAdmissionAndPoll ==
    /\ state.capacityRejected => (~state.admissionOpen /\ ~state.pollOpen)
    /\ \A c \in Categories:
        (state.counts[c] = MaxCapacity \/ state.bytes[c] = MaxCapacity) =>
            /\ state.capacityRejected
            /\ ~state.admissionOpen
            /\ ~state.pollOpen

IdentityRemovedOnlyAfterHorizon ==
    \A i \in Identities:
        state.identityRemoved[i] =>
            /\ state.gcCapturedCheckpointed[i]
            /\ state.gcCapturedBeforeOldest[i]
            /\ state.gcCapturedRetryAge[i]
            /\ state.gcCapturedClockSafe[i]

IdentityGcIsCrashSafe ==
    \A i \in Identities:
        state.identityRemoved[i] => state.identityGcDurable[i]

StartupRequiresCapacityReserve ==
    (state.lifecycle = "ready" /\ state.readiness = "available") => state.reserveHeld

CompactionPreservesCommittedFrames ==
    state.committedFrames \subseteq
        (state.oldFrames \cup state.newBaseFrames \cup state.newSuffixFrames \cup state.activeFrames)

CompactionPreservesOldOrNewComplete ==
    state.recovered =>
        (state.activeFrames = state.oldCompleteFrames
         \/ state.activeFrames = (state.newBaseFrames \cup state.newSuffixFrames))

GenerationBarrierOrdersFrames ==
    /\ state.baseDurable =>
        {f \in state.committedFrames : f <= state.compactionCutoff} \subseteq state.newBaseFrames
    /\ (state.oldGenerationGc /\ state.compactionCutoff > 0) =>
        {f \in state.committedFrames : f > state.compactionCutoff} \subseteq state.newSuffixFrames

CompactionReserveHeldThroughBarrier ==
    state.compactionStage \in {
        "barrier", "base-written", "base-synced", "suffix-synced",
        "generation-dir-synced", "root-written", "root-file-synced",
        "root-renamed", "root-dir-synced", "cutover"
    } => state.compactionReserveHeld

CutoverAfterRequiredSyncs ==
    state.generation = 2 =>
        /\ state.baseDurable
        /\ state.suffixDurable
        /\ state.rootRenamed
        /\ state.rootDirectorySynced
        /\ state.rootGeneration = 2

OldGenerationReclaimedOnlyAfterDurableRoot ==
    state.oldGenerationGc =>
        /\ state.rootDirectorySynced
        /\ state.rootGeneration = 2

NoAdmissionAfterDrainGeneration ==
    state.lifecycle \in {"draining", "closed"} =>
        /\ ~state.admissionOpen
        /\ state.admissionGeneration < state.lifecycleGeneration

LifecycleReadinessAxesDistinct == ~state.axesCoupled

ShutdownFreezesStartedSet ==
    state.shutdownFrozen =>
        /\ state.frozenOperations = state.startedAtDrain
        /\ state.startedOperations \subseteq state.frozenOperations
        /\ state.handlesFenced

ShutdownPreservesSendPhase ==
    state.shutdownClassified =>
        state.sendOutcome = ExpectedSendOutcome(state.sendPhaseAtDeadline, state.sendOutcomeAtFreeze)

ShutdownPreservesCheckpointPhase ==
    state.shutdownClassified =>
        state.checkpointOutcome =
            ExpectedCheckpointOutcome(state.checkpointPhaseAtDeadline, state.checkpointOutcomeAtFreeze)

ClosedImpliesWorkersJoined ==
    state.lifecycle = "closed" =>
        /\ state.workersJoined
        /\ state.transport = "closed"

ShutdownTerminalReportIdempotent ==
    (state.terminalReportCached /\ state.repeatCount > 0) =>
        /\ state.repeatedReportDigest = state.terminalReportDigest
        /\ state.transportCloseCount = 1
        /\ state.stateFinalizeCount = 1

ShutdownSideEffectsAtMostOnce ==
    /\ state.transportCloseCount <= 1
    /\ state.stateFinalizeCount <= 1

ShutdownPreservesOperationOutcome ==
    /\ (state.shutdownFrozen /\ IsSendTerminal(state.sendOutcomeAtFreeze)) =>
        state.sendOutcome = state.sendOutcomeAtFreeze
    /\ (state.shutdownFrozen /\ IsCheckpointTerminal(state.checkpointOutcomeAtFreeze)) =>
        state.checkpointOutcome = state.checkpointOutcomeAtFreeze

UncommittedCheckpointDoesNotAdvanceFrontier ==
    (state.shutdownClassified /\ state.checkpointOutcome # "committed") =>
        state.frontier = state.frontierAtFreeze

CompactionWitnessInit ==
    /\ Init
    /\ state.scenario = "compaction-pre-barrier"
    /\ state.selectedCategory = "payload"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

CompactionWitnessNext ==
    CASE state.lastEvent = "init" -> RegisterPreBarrierMutation
      [] state.lastEvent = "mutation-register" -> AcquireGenerationBarrier
      [] state.lastEvent = "barrier-acquire" -> CommitRegisteredMutation
      [] state.lastEvent = "mutation-terminal" -> WriteCompactionBase
      [] state.lastEvent = "base-write" -> SyncCompactionBase
      [] state.lastEvent = "base-sync" -> SyncEmptySuffix
      [] state.lastEvent = "suffix-sync" -> SyncGenerationDirectory
      [] state.lastEvent = "generation-dir-sync" -> WriteRootPointerTemp
      [] state.lastEvent = "root-write" -> SyncRootPointerFile
      [] state.lastEvent = "root-file-sync" -> RenameRootPointer
      [] state.lastEvent = "root-rename" -> SyncRootDirectory
      [] state.lastEvent = "root-dir-sync" -> ActivateNewGeneration
      [] state.lastEvent = "cutover" -> GarbageCollectOldGeneration
      [] OTHER -> Stutter

ShutdownRepeatWitnessInit ==
    /\ Init
    /\ state.scenario = "shutdown-repeat"
    /\ state.selectedCategory = "payload"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

ShutdownRepeatWitnessNext ==
    CASE state.lastEvent = "init" -> BeginShutdown
      [] state.lastEvent = "shutdown-begin" -> FreezeStartedOperations
      [] state.lastEvent = "shutdown-freeze" -> ReachShutdownDeadline
      [] state.lastEvent = "shutdown-deadline" -> CloseOwnedTransport
      [] state.lastEvent = "transport-close" -> CancelBlockedWorkers
      [] state.lastEvent = "worker-cancel" -> JoinRegisteredWorkers
      [] state.lastEvent = "worker-join" -> FinishShutdown
      [] state.lastEvent = "shutdown-closed" -> RepeatShutdown
      [] OTHER -> Stutter

ClockRollbackWitnessInit ==
    /\ Init
    /\ state.scenario = "identity-clock-rollback"
    /\ state.selectedCategory = "identity"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

ClockRollbackWitnessNext ==
    CASE state.lastEvent = "init" -> RollbackClock
      [] state.lastEvent = "clock-rollback" -> StopPollOnClockUncertainty
      [] OTHER -> Stutter

CompactionWitnessAbsent ==
    ~(state.scenario = "compaction-pre-barrier"
      /\ state.compactionStage = "old-gc"
      /\ state.activeFrames = {1, 2}
      /\ state.committedFrames = {1, 2})

ShutdownRepeatWitnessAbsent ==
    ~(state.scenario = "shutdown-repeat"
      /\ state.repeatCount = 1
      /\ state.terminalReportCached
      /\ state.repeatedReportDigest = state.terminalReportDigest
      /\ state.transportCloseCount = 1
      /\ state.stateFinalizeCount = 1)

ClockRollbackWitnessAbsent ==
    ~(state.scenario = "identity-clock-rollback"
      /\ state.clockRolledBack
      /\ ~state.pollOpen
      /\ ~state.identityRemoved[state.selectedIdentity])

DrainWinsRegistrationWitnessInit ==
    /\ Init
    /\ state.scenario = "shutdown-registration"
    /\ state.selectedCategory = "payload"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

DrainWinsRegistrationWitnessNext ==
    CASE state.lastEvent = "init" -> BeginShutdown
      [] state.lastEvent = "shutdown-begin" -> FreezeStartedOperations
      [] state.lastEvent = "shutdown-freeze" -> ReachShutdownDeadline
      [] state.lastEvent = "shutdown-deadline" -> CloseOwnedTransport
      [] state.lastEvent = "transport-close" -> CancelBlockedWorkers
      [] state.lastEvent = "worker-cancel" -> JoinRegisteredWorkers
      [] state.lastEvent = "worker-join" -> FinishShutdown
      [] OTHER -> Stutter

DrainWinsRegistrationWitnessAbsent ==
    ~(state.scenario = "shutdown-registration"
      /\ state.lifecycle = "closed"
      /\ state.startedOperations = {}
      /\ state.frozenOperations = {})

RegisterWinsShutdownWitnessInit ==
    /\ Init
    /\ state.scenario = "shutdown-registration"
    /\ state.selectedCategory = "payload"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

RegisterWinsShutdownWitnessNext ==
    CASE state.lastEvent = "init" -> RegisterOperation("send")
      [] state.lastEvent = "operation-register" -> BeginShutdown
      [] state.lastEvent = "shutdown-begin" -> FreezeStartedOperations
      [] state.lastEvent = "shutdown-freeze" -> ReachShutdownDeadline
      [] state.lastEvent = "shutdown-deadline" -> CloseOwnedTransport
      [] state.lastEvent = "transport-close" -> CancelBlockedWorkers
      [] state.lastEvent = "worker-cancel" -> JoinRegisteredWorkers
      [] state.lastEvent = "worker-join" -> FinishShutdown
      [] OTHER -> Stutter

RegisterWinsShutdownWitnessAbsent ==
    ~(state.scenario = "shutdown-registration"
      /\ state.lifecycle = "closed"
      /\ state.startedOperations = {"send"}
      /\ state.frozenOperations = {"send"})

CapacityCountExhaustionWitnessInit ==
    /\ Init
    /\ state.scenario = "capacity-exhaustion"
    /\ state.selectedCategory = "payload"
    /\ state.selectedLimit = "count"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

CapacityByteExhaustionWitnessInit ==
    /\ Init
    /\ state.scenario = "capacity-exhaustion"
    /\ state.selectedCategory = "payload"
    /\ state.selectedLimit = "byte"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

CapacityExhaustionWitnessNext ==
    CASE state.lastEvent = "init" -> AdmitState("payload")
      [] state.lastEvent = "admit" -> RejectCapacity("payload")
      [] OTHER -> Stutter

CapacityCountExhaustionWitnessAbsent ==
    ~(state.scenario = "capacity-exhaustion"
      /\ state.selectedLimit = "count"
      /\ state.counts["payload"] = MaxCapacity
      /\ state.capacityRejected
      /\ ~state.admissionOpen
      /\ ~state.pollOpen)

CapacityByteExhaustionWitnessAbsent ==
    ~(state.scenario = "capacity-exhaustion"
      /\ state.selectedLimit = "byte"
      /\ state.bytes["payload"] = MaxCapacity
      /\ state.capacityRejected
      /\ ~state.admissionOpen
      /\ ~state.pollOpen)

CapacityReleaseWitnessInit ==
    /\ Init
    /\ state.scenario = "capacity-release"
    /\ state.selectedCategory = "payload"
    /\ state.selectedIdentity = 1
    /\ state.selectedCrashStage = "barrier"

CapacityReleaseWitnessNext ==
    CASE state.lastEvent = "init" -> AdmitState("payload")
      [] state.lastEvent = "admit" -> FillCategory("payload")
      [] state.lastEvent = "fill-capacity" -> ReleaseState("payload")
      [] OTHER -> Stutter

CapacityReleaseWitnessAbsent ==
    ~(state.scenario = "capacity-release"
      /\ state.step = 3
      /\ state.counts["payload"] = MaxCapacity - 1
      /\ state.bytes["payload"] = MaxCapacity - 1
      /\ ~state.capacityRejected
      /\ state.admissionOpen
      /\ state.pollOpen)

=============================================================================
