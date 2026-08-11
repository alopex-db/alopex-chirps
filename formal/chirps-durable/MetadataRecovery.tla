------------------------- MODULE MetadataRecovery -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Int;
    MaxSequence,
    \* @type: Str;
    UnsafeMode

Sequences == 1..MaxSequence
SnapshotImages == {"old", "new", "torn", "mixed"}
CompleteImages == {"old", "new"}
SnapshotStages == {
    "temp-write", "file-sync", "rename", "directory-sync",
    "install", "post-install-apply"
}
IdentityCrashStages == {
    "identity-prepare", "identity-wal-write", "identity-wal-sync",
    "identity-directory-sync", "identity-install", "identity-fence", "identity-mutate"
}
CrashStages == SnapshotStages \cup IdentityCrashStages
PipelineStages == {
    "idle", "preparing", "temp-written", "file-synced", "renamed",
    "directory-synced", "installed", "applied", "crashed", "failed"
}
RecoveryStates == {"idle", "recovering", "ready", "fail-stop"}
InstallSources == {"boot", "pipeline", "recovery"}
WalValidities == {
    "none", "valid", "partial", "truncated", "checksum-error", "unknown-version"
}
FailureReasons == {
    "none", "snapshot-write", "snapshot-sync", "snapshot-rename",
    "snapshot-directory-sync", "snapshot-install", "snapshot-apply",
    "wal-mismatched-duplicate", "wal-generation-mismatch-duplicate",
    "wal-gap", "wal-partial", "wal-truncated", "wal-checksum",
    "wal-version", "install-unknown"
}

SnapshotScenarios == {"snapshot-success", "snapshot-crash", "snapshot-stage-error"}
WalScenarios == {
    "wal-contiguous", "wal-exact-duplicate", "wal-mismatched-duplicate",
    "wal-generation-mismatch-duplicate", "wal-gap", "wal-partial",
    "wal-truncated", "wal-checksum", "wal-version"
}
IdentityScenarios == {
    "normal-restart", "destructive-mutation", "identity-mutation-crash",
    "retention-eviction"
}
ScenarioIds == SnapshotScenarios \cup WalScenarios \cup IdentityScenarios
    \cup {"install-unknown"}

UnsafeModes == {
    "none", "torn-snapshot-applied", "mixed-snapshot-applied",
    "omit-directory-sync", "infer-file-sync-success", "infer-directory-sync-success",
    "apply-before-install", "watermark-skip", "duplicate-applied",
    "wal-replayed-twice", "mismatched-duplicate-ignored",
    "generation-mismatch-duplicate-ignored", "gap-applied",
    "corrupt-wal-applied", "unsynced-wal-applied", "mixed-generation-applied",
    "identity-install-before-durable", "mutation-before-epoch-durable",
    "mutation-before-fence", "resource-identity-reused",
    "retention-changes-epoch", "restart-changes-identity", "install-unknown-continues"
}

Events == {
    "init", "snapshot-begin", "snapshot-temp-write", "snapshot-file-sync",
    "snapshot-rename", "snapshot-directory-sync", "snapshot-install",
    "snapshot-apply", "snapshot-crash", "snapshot-recover",
    "snapshot-write-failure", "snapshot-sync-failure", "snapshot-rename-failure",
    "snapshot-directory-sync-failure", "snapshot-install-failure",
    "snapshot-apply-failure", "snapshot-failure-recover",
    "wal-write", "wal-sync", "wal-recovery-start", "wal-replay",
    "wal-duplicate-ignore", "wal-duplicate-reject", "wal-gap-reject",
    "wal-corruption-reject", "wal-recovery-finish",
    "normal-restart", "epoch-mutation-begin", "epoch-wal-write",
    "epoch-wal-sync", "epoch-directory-sync", "identity-install", "old-session-fence",
    "destructive-mutation", "identity-crash", "identity-recover",
    "retention-eviction", "install-unknown", "unsafe"
}

ExpectedDigest(seq) == seq * 10 + 1

ImageState(image) ==
    CASE image = "old" -> 101
      [] image = "new" -> 202
      [] OTHER -> 0

EmptySequenceBool == [seq \in Sequences |-> FALSE]
EmptySequenceCount == [seq \in Sequences |-> 0]

IsSnapshotScenario(scenario) == scenario \in SnapshotScenarios
IsWalScenario(scenario) == scenario \in WalScenarios
IsIdentityMutationScenario(scenario) ==
    scenario \in {"destructive-mutation", "identity-mutation-crash"}

WalSequenceFor(scenario) ==
    IF scenario \in {"wal-exact-duplicate", "wal-mismatched-duplicate",
        "wal-generation-mismatch-duplicate"} THEN 1
    ELSE IF scenario = "wal-gap" THEN 3
    ELSE 2

WalDigestFor(scenario) ==
    IF scenario = "wal-mismatched-duplicate" THEN 99
    ELSE ExpectedDigest(WalSequenceFor(scenario))

WalGenerationFor(scenario) ==
    IF scenario = "wal-generation-mismatch-duplicate" THEN 2 ELSE 1

WalValidityFor(scenario) ==
    CASE scenario = "wal-partial" -> "partial"
      [] scenario = "wal-truncated" -> "truncated"
      [] scenario = "wal-checksum" -> "checksum-error"
      [] scenario = "wal-version" -> "unknown-version"
      [] OTHER -> "valid"

WalFailureFor(validity) ==
    CASE validity = "partial" -> "wal-partial"
      [] validity = "truncated" -> "wal-truncated"
      [] validity = "checksum-error" -> "wal-checksum"
      [] validity = "unknown-version" -> "wal-version"
      [] OTHER -> "none"

VARIABLES
    \* @type: {
    \* scenario: Str, selectedCrashStage: Str, selectedCrashOutcome: Str,
    \* step: Int, snapshotStage: Str,
    \* tempWritten: Bool, tempComplete: Bool, tempFileSynced: Bool,
    \* fileSyncFailed: Bool, finalVisible: Str, durableFinal: Str,
    \* directorySynced: Bool, directorySyncFailed: Bool,
    \* installedImage: Str, installSource: Str, baseState: Int, appliedState: Int,
    \* snapshotGeneration: Int, snapshotWatermark: Int,
    \* installUnknown: Bool, crashed: Bool, recoveredAfterCrash: Bool,
    \* recovery: Str, failureReason: Str,
    \* walPresent: Int -> Bool, walSequence: Int, walDigest: Int,
    \* walGeneration: Int, walValidity: Str, walSynced: Bool,
    \* walCommitMarker: Bool, nextReplay: Int, appliedCount: Int -> Int,
    \* duplicateIgnored: Bool, mismatchSeen: Bool, gapSeen: Bool,
    \* invalidWalSeen: Bool, invalidWalApplied: Bool,
    \* mixedGenerationApplied: Bool,
    \* initialResourceUuid: Int, initialResourceEpoch: Int,
    \* initialLifecycleGeneration: Int,
    \* resourceUuid: Int, resourceEpoch: Int, resourceLifecycleGeneration: Int,
    \* maxResourceEpoch: Int, pendingResourceUuid: Int,
    \* pendingResourceEpoch: Int, pendingLifecycleGeneration: Int,
    \* epochWalWritten: Bool, epochWalSynced: Bool, epochDirectorySynced: Bool,
    \* identityInstalled: Bool, oldSessionsFenced: Bool, destructiveApplied: Bool,
    \* retentionApplied: Bool, normalRestarted: Bool, lastEvent: Str};
    state

StopsAfter(stage) ==
    /\ state.scenario \in {"snapshot-crash", "snapshot-stage-error"}
    /\ state.selectedCrashStage = stage

IdentityStopsAfter(stage) ==
    /\ state.scenario = "identity-mutation-crash"
    /\ state.selectedCrashStage = stage

Init ==
    \E selectedScenario \in ScenarioIds:
    \E selectedStage \in
        IF selectedScenario \in {"snapshot-crash", "snapshot-stage-error"}
        THEN SnapshotStages
        ELSE IF selectedScenario = "identity-mutation-crash"
        THEN IdentityCrashStages
        ELSE {"temp-write"}:
    \E selectedOutcome \in
        IF selectedScenario = "snapshot-crash" /\ selectedStage = "rename"
        THEN {"old", "new"} ELSE {"old"}:
    state = [
        scenario |-> selectedScenario,
        selectedCrashStage |-> selectedStage,
        selectedCrashOutcome |-> selectedOutcome,
        step |-> 0,
        snapshotStage |-> "idle",
        tempWritten |-> FALSE,
        tempComplete |-> FALSE,
        tempFileSynced |-> FALSE,
        fileSyncFailed |-> FALSE,
        finalVisible |-> "old",
        durableFinal |-> "old",
        directorySynced |-> FALSE,
        directorySyncFailed |-> FALSE,
        installedImage |-> "old",
        installSource |-> "boot",
        baseState |-> 101,
        appliedState |-> 101,
        snapshotGeneration |-> 1,
        snapshotWatermark |-> 1,
        installUnknown |-> FALSE,
        crashed |-> FALSE,
        recoveredAfterCrash |-> FALSE,
        recovery |-> "idle",
        failureReason |-> "none",
        walPresent |-> EmptySequenceBool,
        walSequence |-> 0,
        walDigest |-> 0,
        walGeneration |-> 0,
        walValidity |-> "none",
        walSynced |-> FALSE,
        walCommitMarker |-> FALSE,
        nextReplay |-> 2,
        appliedCount |-> EmptySequenceCount,
        duplicateIgnored |-> FALSE,
        mismatchSeen |-> FALSE,
        gapSeen |-> FALSE,
        invalidWalSeen |-> FALSE,
        invalidWalApplied |-> FALSE,
        mixedGenerationApplied |-> FALSE,
        initialResourceUuid |-> 101,
        initialResourceEpoch |-> 1,
        initialLifecycleGeneration |-> 1,
        resourceUuid |-> 101,
        resourceEpoch |-> 1,
        resourceLifecycleGeneration |-> 1,
        maxResourceEpoch |-> 1,
        pendingResourceUuid |-> 0,
        pendingResourceEpoch |-> 0,
        pendingLifecycleGeneration |-> 0,
        epochWalWritten |-> FALSE,
        epochWalSynced |-> FALSE,
        epochDirectorySynced |-> FALSE,
        identityInstalled |-> FALSE,
        oldSessionsFenced |-> FALSE,
        destructiveApplied |-> FALSE,
        retentionApplied |-> FALSE,
        normalRestarted |-> FALSE,
        lastEvent |-> "init"
    ]

BeginSnapshot ==
    /\ IsSnapshotScenario(state.scenario)
    /\ state.step = 0
    /\ state' = [state EXCEPT
        !.step = 1,
        !.snapshotStage = "preparing",
        !.lastEvent = "snapshot-begin"]

WriteSnapshotTemp ==
    /\ IsSnapshotScenario(state.scenario)
    /\ state.step = 1
    /\ state.snapshotStage = "preparing"
    /\ ~(state.scenario = "snapshot-stage-error"
          /\ state.selectedCrashStage = "temp-write")
    /\ state' = [state EXCEPT
        !.step = 2,
        !.snapshotStage = "temp-written",
        !.tempWritten = TRUE,
        !.tempComplete = TRUE,
        !.lastEvent = "snapshot-temp-write"]

SyncSnapshotFile ==
    /\ IsSnapshotScenario(state.scenario)
    /\ state.step = 2
    /\ state.snapshotStage = "temp-written"
    /\ ~StopsAfter("temp-write")
    /\ ~(state.scenario = "snapshot-stage-error"
          /\ state.selectedCrashStage = "file-sync")
    /\ state' = [state EXCEPT
        !.step = 3,
        !.snapshotStage = "file-synced",
        !.tempFileSynced = TRUE,
        !.lastEvent = "snapshot-file-sync"]

RenameSnapshot ==
    /\ IsSnapshotScenario(state.scenario)
    /\ state.step = 3
    /\ state.snapshotStage = "file-synced"
    /\ ~StopsAfter("file-sync")
    /\ ~(state.scenario = "snapshot-stage-error"
          /\ state.selectedCrashStage = "rename")
    /\ state' = [state EXCEPT
        !.step = 4,
        !.snapshotStage = "renamed",
        !.finalVisible = "new",
        !.lastEvent = "snapshot-rename"]

SyncSnapshotDirectory ==
    /\ IsSnapshotScenario(state.scenario)
    /\ state.step = 4
    /\ state.snapshotStage = "renamed"
    /\ ~StopsAfter("rename")
    /\ ~(state.scenario = "snapshot-stage-error"
          /\ state.selectedCrashStage = "directory-sync")
    /\ state' = [state EXCEPT
        !.step = 5,
        !.snapshotStage = "directory-synced",
        !.directorySynced = TRUE,
        !.durableFinal = "new",
        !.lastEvent = "snapshot-directory-sync"]

InstallSnapshot ==
    /\ IsSnapshotScenario(state.scenario)
    /\ state.step = 5
    /\ state.snapshotStage = "directory-synced"
    /\ ~StopsAfter("directory-sync")
    /\ ~(state.scenario = "snapshot-stage-error"
          /\ state.selectedCrashStage = "install")
    /\ state' = [state EXCEPT
        !.step = 6,
        !.snapshotStage = "installed",
        !.installedImage = "new",
        !.installSource = "pipeline",
        !.snapshotGeneration = 2,
        !.snapshotWatermark = 2,
        !.lastEvent = "snapshot-install"]

ApplyInstalledSnapshot ==
    /\ IsSnapshotScenario(state.scenario)
    /\ state.step = 6
    /\ state.snapshotStage = "installed"
    /\ ~StopsAfter("install")
    /\ ~(state.scenario = "snapshot-stage-error"
          /\ state.selectedCrashStage = "post-install-apply")
    /\ state' = [state EXCEPT
        !.step = 7,
        !.snapshotStage = "applied",
        !.baseState = 202,
        !.appliedState = 202,
        !.recovery = "ready",
        !.lastEvent = "snapshot-apply"]

CrashAfterTempWrite ==
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "temp-write"
    /\ state.step = 2
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "crashed",
        !.crashed = TRUE,
        !.durableFinal = "old",
        !.lastEvent = "snapshot-crash"]

CrashAfterFileSync ==
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "file-sync"
    /\ state.step = 3
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "crashed",
        !.crashed = TRUE,
        !.durableFinal = "old",
        !.lastEvent = "snapshot-crash"]

CrashAfterRename ==
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "rename"
    /\ state.step = 4
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "crashed",
        !.crashed = TRUE,
        !.durableFinal = state.selectedCrashOutcome,
        !.lastEvent = "snapshot-crash"]

CrashAfterDirectorySync ==
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "directory-sync"
    /\ state.step = 5
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "crashed",
        !.crashed = TRUE,
        !.durableFinal = "new",
        !.lastEvent = "snapshot-crash"]

CrashAfterInstall ==
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "install"
    /\ state.step = 6
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "crashed",
        !.crashed = TRUE,
        !.durableFinal = "new",
        !.lastEvent = "snapshot-crash"]

CrashAfterPostInstallApply ==
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "post-install-apply"
    /\ state.step = 7
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "crashed",
        !.crashed = TRUE,
        !.durableFinal = "new",
        !.lastEvent = "snapshot-crash"]

RecoverSnapshotAfterCrash ==
    /\ state.scenario = "snapshot-crash"
    /\ state.step = 10
    /\ state.snapshotStage = "crashed"
    /\ state' = [state EXCEPT
        !.step = 11,
        !.crashed = FALSE,
        !.recoveredAfterCrash = TRUE,
        !.recovery = "ready",
        !.installedImage = state.durableFinal,
        !.installSource = "recovery",
        !.baseState = ImageState(state.durableFinal),
        !.appliedState = ImageState(state.durableFinal),
        !.snapshotGeneration = IF state.durableFinal = "new" THEN 2 ELSE 1,
        !.snapshotWatermark = IF state.durableFinal = "new" THEN 2 ELSE 1,
        !.lastEvent = "snapshot-recover"]

FailSnapshotTempWrite ==
    /\ state.scenario = "snapshot-stage-error"
    /\ state.selectedCrashStage = "temp-write"
    /\ state.step = 1
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "failed",
        !.failureReason = "snapshot-write",
        !.lastEvent = "snapshot-write-failure"]

FailSnapshotFileSync ==
    /\ state.scenario = "snapshot-stage-error"
    /\ state.selectedCrashStage = "file-sync"
    /\ state.step = 2
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "failed",
        !.fileSyncFailed = TRUE,
        !.tempFileSynced = FALSE,
        !.failureReason = "snapshot-sync",
        !.lastEvent = "snapshot-sync-failure"]

FailSnapshotRename ==
    /\ state.scenario = "snapshot-stage-error"
    /\ state.selectedCrashStage = "rename"
    /\ state.step = 3
    /\ state' = [state EXCEPT
        !.step = 10,
        !.snapshotStage = "failed",
        !.finalVisible = "old",
        !.failureReason = "snapshot-rename",
        !.lastEvent = "snapshot-rename-failure"]

FailSnapshotDirectorySync ==
    /\ state.scenario = "snapshot-stage-error"
    /\ state.selectedCrashStage = "directory-sync"
    /\ state.step = 4
    /\ state' = [state EXCEPT
        !.step = 11,
        !.snapshotStage = "failed",
        !.directorySyncFailed = TRUE,
        !.directorySynced = FALSE,
        !.installUnknown = TRUE,
        !.recovery = "fail-stop",
        !.failureReason = "snapshot-directory-sync",
        !.lastEvent = "snapshot-directory-sync-failure"]

FailSnapshotInstall ==
    /\ state.scenario = "snapshot-stage-error"
    /\ state.selectedCrashStage = "install"
    /\ state.step = 5
    /\ state' = [state EXCEPT
        !.step = 11,
        !.snapshotStage = "failed",
        !.installUnknown = TRUE,
        !.recovery = "fail-stop",
        !.failureReason = "snapshot-install",
        !.lastEvent = "snapshot-install-failure"]

FailSnapshotPostInstallApply ==
    /\ state.scenario = "snapshot-stage-error"
    /\ state.selectedCrashStage = "post-install-apply"
    /\ state.step = 6
    /\ state' = [state EXCEPT
        !.step = 11,
        !.snapshotStage = "failed",
        !.installUnknown = TRUE,
        !.recovery = "fail-stop",
        !.failureReason = "snapshot-apply",
        !.lastEvent = "snapshot-apply-failure"]

RecoverAfterSnapshotFailure ==
    /\ state.scenario = "snapshot-stage-error"
    /\ state.step = 10
    /\ state.snapshotStage = "failed"
    /\ state.failureReason \in {"snapshot-write", "snapshot-sync", "snapshot-rename"}
    /\ state' = [state EXCEPT
        !.step = 11,
        !.recovery = "ready",
        !.installedImage = "old",
        !.installSource = "recovery",
        !.baseState = 101,
        !.appliedState = 101,
        !.snapshotGeneration = 1,
        !.snapshotWatermark = 1,
        !.lastEvent = "snapshot-failure-recover"]

WriteWalFrame ==
    /\ IsWalScenario(state.scenario)
    /\ state.step = 0
    /\ LET seq == WalSequenceFor(state.scenario) IN
       state' = [state EXCEPT
        !.step = 1,
        !.walPresent = [@ EXCEPT ![seq] = TRUE],
        !.walSequence = seq,
        !.walDigest = WalDigestFor(state.scenario),
        !.walGeneration = WalGenerationFor(state.scenario),
        !.walValidity = WalValidityFor(state.scenario),
        !.lastEvent = "wal-write"]

SyncWalFrame ==
    /\ IsWalScenario(state.scenario)
    /\ state.step = 1
    /\ state' = [state EXCEPT
        !.step = 2,
        !.walSynced = state.walValidity \in {"valid", "checksum-error", "unknown-version"},
        !.walCommitMarker = state.walValidity \in {"valid", "checksum-error", "unknown-version"},
        !.lastEvent = "wal-sync"]

StartWalRecovery ==
    /\ IsWalScenario(state.scenario)
    /\ state.step = 2
    /\ state' = [state EXCEPT
        !.step = 3,
        !.recovery = "recovering",
        !.nextReplay = state.snapshotWatermark + 1,
        !.lastEvent = "wal-recovery-start"]

ReplayContiguousWal ==
    /\ state.scenario = "wal-contiguous"
    /\ state.step = 3
    /\ state.recovery = "recovering"
    /\ state.walSequence = state.nextReplay
    /\ state.walValidity = "valid"
    /\ state.walSynced
    /\ state.walCommitMarker
    /\ state.walGeneration = state.snapshotGeneration
    /\ state' = [state EXCEPT
        !.step = 4,
        !.appliedCount = [@ EXCEPT ![state.walSequence] = @ + 1],
        !.appliedState = 202,
        !.nextReplay = state.nextReplay + 1,
        !.lastEvent = "wal-replay"]

IgnoreExactWalDuplicate ==
    /\ state.scenario = "wal-exact-duplicate"
    /\ state.step = 3
    /\ state.recovery = "recovering"
    /\ state.walSequence <= state.snapshotWatermark
    /\ state.walDigest = ExpectedDigest(state.walSequence)
    /\ state.walGeneration = state.snapshotGeneration
    /\ state.walValidity = "valid"
    /\ state.walSynced
    /\ state.walCommitMarker
    /\ state' = [state EXCEPT
        !.step = 4,
        !.duplicateIgnored = TRUE,
        !.lastEvent = "wal-duplicate-ignore"]

RejectMismatchedWalDuplicate ==
    /\ state.scenario \in {"wal-mismatched-duplicate",
        "wal-generation-mismatch-duplicate"}
    /\ state.step = 3
    /\ state.recovery = "recovering"
    /\ state.walSequence <= state.snapshotWatermark
    /\ (state.walDigest # ExpectedDigest(state.walSequence)
        \/ state.walGeneration # state.snapshotGeneration)
    /\ state' = [state EXCEPT
        !.step = 4,
        !.recovery = "fail-stop",
        !.mismatchSeen = TRUE,
        !.failureReason =
            IF state.walGeneration # state.snapshotGeneration
            THEN "wal-generation-mismatch-duplicate"
            ELSE "wal-mismatched-duplicate",
        !.lastEvent = "wal-duplicate-reject"]

RejectWalGap ==
    /\ state.scenario = "wal-gap"
    /\ state.step = 3
    /\ state.recovery = "recovering"
    /\ state.walSequence > state.nextReplay
    /\ state' = [state EXCEPT
        !.step = 4,
        !.recovery = "fail-stop",
        !.gapSeen = TRUE,
        !.failureReason = "wal-gap",
        !.lastEvent = "wal-gap-reject"]

RejectInvalidWal ==
    /\ state.scenario \in {"wal-partial", "wal-truncated", "wal-checksum", "wal-version"}
    /\ state.step = 3
    /\ state.recovery = "recovering"
    /\ state.walValidity # "valid"
    /\ state' = [state EXCEPT
        !.step = 4,
        !.recovery = "fail-stop",
        !.invalidWalSeen = TRUE,
        !.failureReason = WalFailureFor(state.walValidity),
        !.lastEvent = "wal-corruption-reject"]

FinishWalRecovery ==
    /\ state.scenario \in {"wal-contiguous", "wal-exact-duplicate"}
    /\ state.step = 4
    /\ state.recovery = "recovering"
    /\ state' = [state EXCEPT
        !.step = 5,
        !.recovery = "ready",
        !.lastEvent = "wal-recovery-finish"]

NormalRestart ==
    /\ state.scenario = "normal-restart"
    /\ state.step = 0
    /\ state' = [state EXCEPT
        !.step = 1,
        !.normalRestarted = TRUE,
        !.recovery = "ready",
        !.lastEvent = "normal-restart"]

BeginEpochMutation ==
    /\ IsIdentityMutationScenario(state.scenario)
    /\ state.step = 0
    /\ state' = [state EXCEPT
        !.step = 1,
        !.pendingResourceUuid = 202,
        !.pendingResourceEpoch = 2,
        !.pendingLifecycleGeneration = 2,
        !.lastEvent = "epoch-mutation-begin"]

WriteEpochWal ==
    /\ IsIdentityMutationScenario(state.scenario)
    /\ state.step = 1
    /\ ~IdentityStopsAfter("identity-prepare")
    /\ state' = [state EXCEPT
        !.step = 2,
        !.epochWalWritten = TRUE,
        !.lastEvent = "epoch-wal-write"]

SyncEpochWal ==
    /\ IsIdentityMutationScenario(state.scenario)
    /\ state.step = 2
    /\ state.epochWalWritten
    /\ ~IdentityStopsAfter("identity-wal-write")
    /\ state' = [state EXCEPT
        !.step = 3,
        !.epochWalSynced = TRUE,
        !.lastEvent = "epoch-wal-sync"]

SyncEpochDirectory ==
    /\ IsIdentityMutationScenario(state.scenario)
    /\ state.step = 3
    /\ state.epochWalSynced
    /\ ~IdentityStopsAfter("identity-wal-sync")
    /\ state' = [state EXCEPT
        !.step = 4,
        !.epochDirectorySynced = TRUE,
        !.lastEvent = "epoch-directory-sync"]

InstallPendingIdentity ==
    /\ IsIdentityMutationScenario(state.scenario)
    /\ state.step = 4
    /\ state.epochDirectorySynced
    /\ ~IdentityStopsAfter("identity-directory-sync")
    /\ state' = [state EXCEPT
        !.step = 5,
        !.resourceUuid = state.pendingResourceUuid,
        !.resourceEpoch = state.pendingResourceEpoch,
        !.resourceLifecycleGeneration = state.pendingLifecycleGeneration,
        !.maxResourceEpoch = state.pendingResourceEpoch,
        !.identityInstalled = TRUE,
        !.lastEvent = "identity-install"]

FenceOldSessions ==
    /\ IsIdentityMutationScenario(state.scenario)
    /\ state.step = 5
    /\ state.identityInstalled
    /\ ~IdentityStopsAfter("identity-install")
    /\ state' = [state EXCEPT
        !.step = 6,
        !.oldSessionsFenced = TRUE,
        !.lastEvent = "old-session-fence"]

ApplyDestructiveMutation ==
    /\ IsIdentityMutationScenario(state.scenario)
    /\ state.step = 6
    /\ state.oldSessionsFenced
    /\ ~IdentityStopsAfter("identity-fence")
    /\ state' = [state EXCEPT
        !.step = 7,
        !.destructiveApplied = TRUE,
        !.lastEvent = "destructive-mutation"]

CrashIdentityMutationStage ==
    /\ state.scenario = "identity-mutation-crash"
    /\ \/ state.selectedCrashStage = "identity-prepare" /\ state.step = 1
       \/ state.selectedCrashStage = "identity-wal-write" /\ state.step = 2
       \/ state.selectedCrashStage = "identity-wal-sync" /\ state.step = 3
       \/ state.selectedCrashStage = "identity-directory-sync" /\ state.step = 4
       \/ state.selectedCrashStage = "identity-install" /\ state.step = 5
       \/ state.selectedCrashStage = "identity-fence" /\ state.step = 6
       \/ state.selectedCrashStage = "identity-mutate" /\ state.step = 7
    /\ state' = [state EXCEPT
        !.step = 10,
        !.crashed = TRUE,
        !.lastEvent = "identity-crash"]

RecoverIdentityMutation ==
    /\ state.scenario = "identity-mutation-crash"
    /\ state.step = 10
    /\ state.crashed
    /\ state' = [state EXCEPT
        !.step = 11,
        !.crashed = FALSE,
        !.recoveredAfterCrash = TRUE,
        !.installUnknown = state.epochWalSynced /\ ~state.epochDirectorySynced,
        !.recovery =
            IF state.epochWalSynced /\ ~state.epochDirectorySynced
            THEN "fail-stop" ELSE "ready",
        !.failureReason =
            IF state.epochWalSynced /\ ~state.epochDirectorySynced
            THEN "install-unknown" ELSE state.failureReason,
        !.resourceUuid =
            IF state.epochDirectorySynced THEN state.pendingResourceUuid
            ELSE state.initialResourceUuid,
        !.resourceEpoch =
            IF state.epochDirectorySynced THEN state.pendingResourceEpoch
            ELSE state.initialResourceEpoch,
        !.resourceLifecycleGeneration =
            IF state.epochDirectorySynced THEN state.pendingLifecycleGeneration
            ELSE state.initialLifecycleGeneration,
        !.maxResourceEpoch =
            IF state.epochDirectorySynced THEN state.pendingResourceEpoch
            ELSE state.initialResourceEpoch,
        !.identityInstalled = state.epochDirectorySynced,
        !.oldSessionsFenced =
            IF state.epochDirectorySynced THEN TRUE ELSE state.oldSessionsFenced,
        !.lastEvent = "identity-recover"]

ApplyRetentionEviction ==
    /\ state.scenario = "retention-eviction"
    /\ state.step = 0
    /\ state' = [state EXCEPT
        !.step = 1,
        !.retentionApplied = TRUE,
        !.lastEvent = "retention-eviction"]

FailStopInstallUnknown ==
    /\ state.scenario = "install-unknown"
    /\ state.step = 0
    /\ state' = [state EXCEPT
        !.step = 1,
        !.installUnknown = TRUE,
        !.recovery = "fail-stop",
        !.failureReason = "install-unknown",
        !.lastEvent = "install-unknown"]

TerminalState ==
    CASE state.scenario = "snapshot-success" -> state.step = 7
      [] state.scenario = "snapshot-crash" -> state.step = 11
      [] state.scenario = "snapshot-stage-error" -> state.step = 11
      [] state.scenario \in {"wal-contiguous", "wal-exact-duplicate"} -> state.step = 5
      [] state.scenario \in {
            "wal-mismatched-duplicate", "wal-generation-mismatch-duplicate",
            "wal-gap", "wal-partial", "wal-truncated", "wal-checksum", "wal-version"
         } -> state.step = 4
      [] state.scenario = "destructive-mutation" -> state.step = 7
      [] state.scenario = "identity-mutation-crash" -> state.step = 11
      [] OTHER -> state.step = 1

TerminalStutter ==
    /\ TerminalState
    /\ state' = state

UnsafeTransition ==
    /\ UnsafeMode # "none"
    /\ state.step = 0
    /\ CASE UnsafeMode = "torn-snapshot-applied" ->
            state' = [state EXCEPT
                !.installedImage = "torn", !.baseState = 150,
                !.appliedState = 150, !.recovery = "ready",
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "mixed-snapshot-applied" ->
            state' = [state EXCEPT
                !.installedImage = "mixed", !.baseState = 150,
                !.appliedState = 150, !.recovery = "ready",
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "omit-directory-sync" ->
            state' = [state EXCEPT
                !.tempWritten = TRUE, !.tempComplete = TRUE,
                !.tempFileSynced = TRUE, !.finalVisible = "new",
                !.directorySynced = FALSE, !.installedImage = "new",
                !.installSource = "pipeline", !.baseState = 202,
                !.appliedState = 202, !.snapshotGeneration = 2,
                !.snapshotWatermark = 2, !.recovery = "ready",
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "infer-file-sync-success" ->
            state' = [state EXCEPT
                !.fileSyncFailed = TRUE, !.tempFileSynced = TRUE,
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "infer-directory-sync-success" ->
            state' = [state EXCEPT
                !.directorySyncFailed = TRUE, !.directorySynced = TRUE,
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "apply-before-install" ->
            state' = [state EXCEPT
                !.snapshotStage = "applied", !.appliedState = 202,
                !.recovery = "ready", !.lastEvent = "unsafe"]
          [] UnsafeMode = "watermark-skip" ->
            state' = [state EXCEPT
                !.snapshotWatermark = 2, !.lastEvent = "unsafe"]
          [] UnsafeMode = "duplicate-applied" ->
            state' = [state EXCEPT
                !.appliedCount = [@ EXCEPT ![1] = 1],
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "wal-replayed-twice" ->
            state' = [state EXCEPT
                !.appliedCount = [@ EXCEPT ![2] = 2],
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "mismatched-duplicate-ignored" ->
            state' = [state EXCEPT
                !.mismatchSeen = TRUE, !.recovery = "ready",
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "generation-mismatch-duplicate-ignored" ->
            state' = [state EXCEPT
                !.walSequence = 1, !.walGeneration = 2,
                !.walDigest = ExpectedDigest(1), !.walValidity = "valid",
                !.walSynced = TRUE, !.walCommitMarker = TRUE,
                !.duplicateIgnored = TRUE, !.recovery = "ready",
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "gap-applied" ->
            state' = [state EXCEPT
                !.appliedCount = [@ EXCEPT ![3] = 1],
                !.appliedState = 202, !.gapSeen = TRUE,
                !.recovery = "ready", !.lastEvent = "unsafe"]
          [] UnsafeMode = "corrupt-wal-applied" ->
            state' = [state EXCEPT
                !.walSequence = 2, !.walValidity = "checksum-error",
                !.walSynced = TRUE, !.walCommitMarker = TRUE,
                !.appliedCount = [@ EXCEPT ![2] = 1],
                !.invalidWalSeen = TRUE, !.invalidWalApplied = TRUE,
                !.recovery = "ready", !.lastEvent = "unsafe"]
          [] UnsafeMode = "unsynced-wal-applied" ->
            state' = [state EXCEPT
                !.walSequence = 2, !.walValidity = "valid",
                !.walSynced = FALSE, !.walCommitMarker = FALSE,
                !.appliedCount = [@ EXCEPT ![2] = 1],
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "mixed-generation-applied" ->
            state' = [state EXCEPT
                !.walSequence = 2, !.walGeneration = 2,
                !.walValidity = "valid", !.walSynced = TRUE,
                !.walCommitMarker = TRUE,
                !.appliedCount = [@ EXCEPT ![2] = 1],
                !.mixedGenerationApplied = TRUE, !.lastEvent = "unsafe"]
          [] UnsafeMode = "identity-install-before-durable" ->
            state' = [state EXCEPT
                !.resourceUuid = 202, !.resourceEpoch = 2,
                !.resourceLifecycleGeneration = 2,
                !.maxResourceEpoch = 2, !.identityInstalled = TRUE,
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "mutation-before-epoch-durable" ->
            state' = [state EXCEPT
                !.resourceUuid = 202, !.resourceEpoch = 2,
                !.resourceLifecycleGeneration = 2,
                !.maxResourceEpoch = 2, !.destructiveApplied = TRUE,
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "mutation-before-fence" ->
            state' = [state EXCEPT
                !.resourceUuid = 202, !.resourceEpoch = 2,
                !.resourceLifecycleGeneration = 2,
                !.maxResourceEpoch = 2, !.epochWalWritten = TRUE,
                !.epochWalSynced = TRUE, !.epochDirectorySynced = TRUE,
                !.identityInstalled = TRUE,
                !.destructiveApplied = TRUE, !.lastEvent = "unsafe"]
          [] UnsafeMode = "resource-identity-reused" ->
            state' = [state EXCEPT
                !.resourceEpoch = 2, !.maxResourceEpoch = 2,
                !.resourceLifecycleGeneration = 2,
                !.epochWalWritten = TRUE, !.epochWalSynced = TRUE,
                !.epochDirectorySynced = TRUE, !.oldSessionsFenced = TRUE,
                !.identityInstalled = TRUE,
                !.destructiveApplied = TRUE, !.lastEvent = "unsafe"]
          [] UnsafeMode = "retention-changes-epoch" ->
            state' = [state EXCEPT
                !.resourceEpoch = 2, !.maxResourceEpoch = 2,
                !.resourceLifecycleGeneration = 2,
                !.retentionApplied = TRUE, !.lastEvent = "unsafe"]
          [] UnsafeMode = "restart-changes-identity" ->
            state' = [state EXCEPT
                !.resourceUuid = 202, !.normalRestarted = TRUE,
                !.lastEvent = "unsafe"]
          [] UnsafeMode = "install-unknown-continues" ->
            state' = [state EXCEPT
                !.installUnknown = TRUE, !.recovery = "ready",
                !.lastEvent = "unsafe"]
          [] OTHER -> /\ FALSE /\ state' = state

AllActions ==
    \/ BeginSnapshot
    \/ WriteSnapshotTemp
    \/ SyncSnapshotFile
    \/ RenameSnapshot
    \/ SyncSnapshotDirectory
    \/ InstallSnapshot
    \/ ApplyInstalledSnapshot
    \/ CrashAfterTempWrite
    \/ CrashAfterFileSync
    \/ CrashAfterRename
    \/ CrashAfterDirectorySync
    \/ CrashAfterInstall
    \/ CrashAfterPostInstallApply
    \/ RecoverSnapshotAfterCrash
    \/ FailSnapshotTempWrite
    \/ FailSnapshotFileSync
    \/ FailSnapshotRename
    \/ FailSnapshotDirectorySync
    \/ FailSnapshotInstall
    \/ FailSnapshotPostInstallApply
    \/ RecoverAfterSnapshotFailure
    \/ WriteWalFrame
    \/ SyncWalFrame
    \/ StartWalRecovery
    \/ ReplayContiguousWal
    \/ IgnoreExactWalDuplicate
    \/ RejectMismatchedWalDuplicate
    \/ RejectWalGap
    \/ RejectInvalidWal
    \/ FinishWalRecovery
    \/ NormalRestart
    \/ BeginEpochMutation
    \/ WriteEpochWal
    \/ SyncEpochWal
    \/ SyncEpochDirectory
    \/ InstallPendingIdentity
    \/ FenceOldSessions
    \/ ApplyDestructiveMutation
    \/ CrashIdentityMutationStage
    \/ RecoverIdentityMutation
    \/ ApplyRetentionEviction
    \/ FailStopInstallUnknown
    \/ TerminalStutter
    \/ UnsafeTransition

Next == AllActions

TypeOK ==
    /\ state.scenario \in ScenarioIds
    /\ state.selectedCrashStage \in CrashStages
    /\ state.selectedCrashOutcome \in CompleteImages
    /\ state.step \in 0..20
    /\ state.snapshotStage \in PipelineStages
    /\ state.tempWritten \in BOOLEAN
    /\ state.tempComplete \in BOOLEAN
    /\ state.tempFileSynced \in BOOLEAN
    /\ state.fileSyncFailed \in BOOLEAN
    /\ state.finalVisible \in SnapshotImages
    /\ state.durableFinal \in CompleteImages
    /\ state.directorySynced \in BOOLEAN
    /\ state.directorySyncFailed \in BOOLEAN
    /\ state.installedImage \in SnapshotImages
    /\ state.installSource \in InstallSources
    /\ state.baseState \in 0..300
    /\ state.appliedState \in 0..300
    /\ state.snapshotGeneration \in 1..2
    /\ state.snapshotWatermark \in 1..MaxSequence
    /\ state.installUnknown \in BOOLEAN
    /\ state.crashed \in BOOLEAN
    /\ state.recoveredAfterCrash \in BOOLEAN
    /\ state.recovery \in RecoveryStates
    /\ state.failureReason \in FailureReasons
    /\ state.walPresent \in [Sequences -> BOOLEAN]
    /\ state.walSequence \in 0..MaxSequence
    /\ state.walDigest \in 0..100
    /\ state.walGeneration \in 0..2
    /\ state.walValidity \in WalValidities
    /\ state.walSynced \in BOOLEAN
    /\ state.walCommitMarker \in BOOLEAN
    /\ state.nextReplay \in 1..(MaxSequence + 1)
    /\ state.appliedCount \in [Sequences -> 0..2]
    /\ state.duplicateIgnored \in BOOLEAN
    /\ state.mismatchSeen \in BOOLEAN
    /\ state.gapSeen \in BOOLEAN
    /\ state.invalidWalSeen \in BOOLEAN
    /\ state.invalidWalApplied \in BOOLEAN
    /\ state.mixedGenerationApplied \in BOOLEAN
    /\ state.initialResourceUuid \in 1..300
    /\ state.initialResourceEpoch \in 1..2
    /\ state.initialLifecycleGeneration \in 1..2
    /\ state.resourceUuid \in 1..300
    /\ state.resourceEpoch \in 1..2
    /\ state.resourceLifecycleGeneration \in 1..2
    /\ state.maxResourceEpoch \in 1..2
    /\ state.pendingResourceUuid \in 0..300
    /\ state.pendingResourceEpoch \in 0..2
    /\ state.pendingLifecycleGeneration \in 0..2
    /\ state.epochWalWritten \in BOOLEAN
    /\ state.epochWalSynced \in BOOLEAN
    /\ state.epochDirectorySynced \in BOOLEAN
    /\ state.identityInstalled \in BOOLEAN
    /\ state.oldSessionsFenced \in BOOLEAN
    /\ state.destructiveApplied \in BOOLEAN
    /\ state.retentionApplied \in BOOLEAN
    /\ state.normalRestarted \in BOOLEAN
    /\ state.lastEvent \in Events

SnapshotOldOrNewComplete ==
    /\ state.installedImage \in CompleteImages
    /\ state.recovery = "ready" \/ state.snapshotStage = "applied" =>
        state.baseState = ImageState(state.installedImage)

SnapshotInstallStagesOrdered ==
    /\ state.tempFileSynced => state.tempComplete
    /\ state.directorySynced =>
        state.tempFileSynced /\ state.finalVisible = "new"
    /\ state.installSource = "pipeline" /\ state.installedImage = "new" =>
        state.tempComplete /\ state.tempFileSynced
        /\ state.finalVisible = "new" /\ state.directorySynced

SnapshotSyncErrorsAreNotSuccess ==
    /\ state.fileSyncFailed => ~state.tempFileSynced
    /\ state.directorySyncFailed => ~state.directorySynced

SnapshotGenerationWatermarkCoherent ==
    /\ state.installedImage = "old" =>
        state.snapshotGeneration = 1 /\ state.snapshotWatermark = 1
    /\ state.installedImage = "new" =>
        state.snapshotGeneration = 2 /\ state.snapshotWatermark = 2

AppliedStateHasDurableProvenance ==
    state.recovery = "ready" \/ state.snapshotStage = "applied" =>
        state.appliedState = state.baseState
        \/ state.appliedCount[2] = 1

SnapshotWalApplyExactlyOnce ==
    \A seq \in Sequences:
        /\ state.appliedCount[seq] <= 1
        /\ seq <= state.snapshotWatermark => state.appliedCount[seq] = 0

RecoveryUsesContiguousValidPrefix ==
    state.appliedCount[3] > 0 => state.appliedCount[2] = 1

WalReplayRequiresDurableCommit ==
    state.walSequence = 0
    \/ state.appliedCount[state.walSequence] = 0
    \/ (state.walValidity = "valid"
        /\ state.walSynced
        /\ state.walCommitMarker)

SnapshotAndWalGenerationMatch ==
    /\ ~state.mixedGenerationApplied
    /\ (state.lastEvent = "wal-replay" =>
        state.walGeneration = state.snapshotGeneration)
    /\ (state.duplicateIgnored =>
        state.walGeneration = state.snapshotGeneration)

ExactDuplicateIsHarmless ==
    state.duplicateIgnored =>
        \A seq \in Sequences:
            seq <= state.snapshotWatermark => state.appliedCount[seq] = 0

MismatchedDuplicateFailsStop ==
    state.mismatchSeen => state.recovery = "fail-stop"

WalGapFailsStop ==
    state.gapSeen => state.recovery = "fail-stop"

InvalidWalFailsStop ==
    /\ ~state.invalidWalApplied
    /\ state.invalidWalSeen => state.recovery = "fail-stop"

InstallUnknownFailsStop ==
    state.installUnknown => state.recovery = "fail-stop"

EpochDurableBeforeMutation ==
    state.destructiveApplied =>
        state.epochWalWritten
        /\ state.epochWalSynced
        /\ state.epochDirectorySynced
        /\ state.identityInstalled
        /\ state.oldSessionsFenced

IdentityInstallRequiresDurableEpoch ==
    state.identityInstalled =>
        state.epochWalWritten
        /\ state.epochWalSynced
        /\ state.epochDirectorySynced

InstalledIdentityIsFresh ==
    state.identityInstalled =>
        /\ state.resourceEpoch > state.initialResourceEpoch
        /\ state.resourceEpoch = state.maxResourceEpoch
        /\ state.resourceUuid # state.initialResourceUuid
        /\ state.resourceLifecycleGeneration > state.initialLifecycleGeneration

ResourceEpochNeverReused ==
    state.destructiveApplied =>
        /\ state.resourceEpoch > state.initialResourceEpoch
        /\ state.resourceEpoch = state.maxResourceEpoch
        /\ state.resourceUuid # state.initialResourceUuid
        /\ state.resourceLifecycleGeneration > state.initialLifecycleGeneration

IdentityStableAcrossNormalRestart ==
    state.normalRestarted =>
        state.resourceUuid = state.initialResourceUuid
        /\ state.resourceEpoch = state.initialResourceEpoch
        /\ state.resourceLifecycleGeneration = state.initialLifecycleGeneration

RetentionPreservesIdentityEpoch ==
    state.retentionApplied =>
        state.resourceUuid = state.initialResourceUuid
        /\ state.resourceEpoch = state.initialResourceEpoch
        /\ state.resourceLifecycleGeneration = state.initialLifecycleGeneration

OldCrashWitnessInit ==
    /\ Init
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "temp-write"

OldCrashWitnessNext == Next

OldCrashWitnessAbsent ==
    ~(state.recoveredAfterCrash
      /\ state.recovery = "ready"
      /\ state.installedImage = "old"
      /\ state.baseState = 101)

NewCrashWitnessInit ==
    /\ Init
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "directory-sync"

NewCrashWitnessNext == Next

NewCrashWitnessAbsent ==
    ~(state.recoveredAfterCrash
      /\ state.recovery = "ready"
      /\ state.installedImage = "new"
      /\ state.baseState = 202)

RenameNewWitnessInit ==
    /\ Init
    /\ state.scenario = "snapshot-crash"
    /\ state.selectedCrashStage = "rename"
    /\ state.selectedCrashOutcome = "new"

RenameNewWitnessNext == Next

RenameNewWitnessAbsent ==
    ~(state.recoveredAfterCrash
      /\ state.installSource = "recovery"
      /\ state.installedImage = "new"
      /\ state.baseState = 202)

ContiguousWalWitnessInit == Init /\ state.scenario = "wal-contiguous"
ContiguousWalWitnessNext == Next
ContiguousWalWitnessAbsent ==
    ~(state.recovery = "ready"
      /\ state.appliedCount[2] = 1
      /\ state.nextReplay = 3)

ExactDuplicateWitnessInit == Init /\ state.scenario = "wal-exact-duplicate"
ExactDuplicateWitnessNext == Next
ExactDuplicateWitnessAbsent ==
    ~(state.recovery = "ready"
      /\ state.duplicateIgnored
      /\ state.appliedCount[1] = 0)

GenerationMismatchDuplicateWitnessInit ==
    Init /\ state.scenario = "wal-generation-mismatch-duplicate"

GenerationMismatchDuplicateWitnessNext == Next

GenerationMismatchDuplicateWitnessAbsent ==
    ~(state.recovery = "fail-stop"
      /\ state.mismatchSeen
      /\ state.failureReason = "wal-generation-mismatch-duplicate")

GapFailStopWitnessInit == Init /\ state.scenario = "wal-gap"
GapFailStopWitnessNext == Next
GapFailStopWitnessAbsent ==
    ~(state.recovery = "fail-stop"
      /\ state.gapSeen
      /\ state.failureReason = "wal-gap")

CorruptFailStopWitnessInit == Init /\ state.scenario = "wal-checksum"
CorruptFailStopWitnessNext == Next
CorruptFailStopWitnessAbsent ==
    ~(state.recovery = "fail-stop"
      /\ state.invalidWalSeen
      /\ state.failureReason = "wal-checksum")

DurableMutationWitnessInit == Init /\ state.scenario = "destructive-mutation"
DurableMutationWitnessNext == Next
DurableMutationWitnessAbsent ==
    ~(state.destructiveApplied
      /\ state.epochWalSynced
      /\ state.epochDirectorySynced
      /\ state.oldSessionsFenced
      /\ state.resourceUuid = 202
      /\ state.resourceEpoch = 2
      /\ state.resourceLifecycleGeneration = 2)

IdentityPrepareCrashWitnessInit ==
    /\ Init
    /\ state.scenario = "identity-mutation-crash"
    /\ state.selectedCrashStage = "identity-prepare"

IdentityPrepareCrashWitnessNext == Next

IdentityPrepareCrashWitnessAbsent ==
    ~(state.recoveredAfterCrash
      /\ state.recovery = "ready"
      /\ ~state.identityInstalled
      /\ state.resourceUuid = state.initialResourceUuid
      /\ state.resourceEpoch = state.initialResourceEpoch)

IdentitySyncCrashWitnessInit ==
    /\ Init
    /\ state.scenario = "identity-mutation-crash"
    /\ state.selectedCrashStage = "identity-wal-sync"

IdentitySyncCrashWitnessNext == Next

IdentitySyncCrashWitnessAbsent ==
    ~(state.recoveredAfterCrash
      /\ state.installUnknown
      /\ state.recovery = "fail-stop"
      /\ ~state.identityInstalled)

IdentityDirectoryCrashWitnessInit ==
    /\ Init
    /\ state.scenario = "identity-mutation-crash"
    /\ state.selectedCrashStage = "identity-directory-sync"

IdentityDirectoryCrashWitnessNext == Next

IdentityDirectoryCrashWitnessAbsent ==
    ~(state.recoveredAfterCrash
      /\ state.recovery = "ready"
      /\ state.identityInstalled
      /\ state.oldSessionsFenced
      /\ state.resourceUuid = 202
      /\ state.resourceEpoch = 2)

IdentityMutationCrashWitnessInit ==
    /\ Init
    /\ state.scenario = "identity-mutation-crash"
    /\ state.selectedCrashStage = "identity-mutate"

IdentityMutationCrashWitnessNext == Next

IdentityMutationCrashWitnessAbsent ==
    ~(state.recoveredAfterCrash
      /\ state.recovery = "ready"
      /\ state.identityInstalled
      /\ state.oldSessionsFenced
      /\ state.destructiveApplied
      /\ state.resourceUuid = 202
      /\ state.resourceEpoch = 2)

=============================================================================
