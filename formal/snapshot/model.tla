------------------------------- MODULE model --------------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Int;
    ChunkCount,
    \* @type: Int;
    RetryLimit,
    \* @type: Int;
    MaxConcurrent

Chunks == 1..ChunkCount
Phases == {"idle", "sending", "verifying", "checkpointing", "installed", "failed"}

VARIABLES \* @type: Str;
          phase,
          \* @type: Set(Int);
          inFlight,
          \* @type: Set(Int);
          verified,
          \* @type: Set(Int);
          faulted,
          \* @type: Int -> Int;
          attempts,
          \* @type: Int;
          progress,
          \* @type: Int;
          maxObservedConcurrency,
          \* @type: Bool;
          digestValid,
          \* @type: Bool;
          checkpointed,
          \* @type: Bool;
          visible

vars == << phase, inFlight, verified, faulted, attempts, progress,
           maxObservedConcurrency, digestValid, checkpointed, visible >>

Init ==
    /\ phase = "idle"
    /\ inFlight = {}
    /\ verified = {}
    /\ faulted = {}
    /\ attempts = [c \in Chunks |-> 0]
    /\ progress = 0
    /\ maxObservedConcurrency = 0
    /\ digestValid = FALSE
    /\ checkpointed = FALSE
    /\ visible = FALSE

Start ==
    /\ phase = "idle"
    /\ phase' = "sending"
    /\ UNCHANGED << inFlight, verified, faulted, attempts, progress,
                    maxObservedConcurrency, digestValid, checkpointed, visible >>

Schedule ==
    \E c \in Chunks:
        /\ phase = "sending"
        /\ c \notin verified
        /\ c \notin inFlight
        /\ attempts[c] <= RetryLimit
        /\ Cardinality(inFlight) < MaxConcurrent
        /\ inFlight' = inFlight \cup {c}
        /\ maxObservedConcurrency' =
              IF Cardinality(inFlight') > maxObservedConcurrency
              THEN Cardinality(inFlight')
              ELSE maxObservedConcurrency
        /\ UNCHANGED << phase, verified, faulted, attempts, progress,
                        digestValid, checkpointed, visible >>

DeliverClean ==
    \E c \in inFlight:
        /\ phase = "sending"
        /\ inFlight' = inFlight \ {c}
        /\ verified' = verified \cup {c}
        /\ progress' = Cardinality(verified')
        /\ UNCHANGED << phase, faulted, attempts, maxObservedConcurrency,
                        digestValid, checkpointed, visible >>

DeliverFault ==
    \E c \in inFlight:
        /\ phase = "sending"
        /\ attempts[c] <= RetryLimit
        /\ inFlight' = inFlight \ {c}
        /\ attempts' = [attempts EXCEPT ![c] = @ + 1]
        /\ faulted' = faulted \cup {c}
        /\ UNCHANGED << phase, verified, progress, maxObservedConcurrency,
                        digestValid, checkpointed, visible >>

BeginVerify ==
    /\ phase = "sending"
    /\ verified = Chunks
    /\ inFlight = {}
    /\ phase' = "verifying"
    /\ UNCHANGED << inFlight, verified, faulted, attempts, progress,
                    maxObservedConcurrency, digestValid, checkpointed, visible >>

VerifyDigest ==
    /\ phase = "verifying"
    /\ phase' = "checkpointing"
    /\ digestValid' = TRUE
    /\ UNCHANGED << inFlight, verified, faulted, attempts, progress,
                    maxObservedConcurrency, checkpointed, visible >>

CommitCheckpoint ==
    /\ phase = "checkpointing"
    /\ digestValid
    /\ verified = Chunks
    /\ phase' = "installed"
    /\ checkpointed' = TRUE
    /\ visible' = TRUE
    /\ UNCHANGED << inFlight, verified, faulted, attempts, progress,
                    maxObservedConcurrency, digestValid >>

Abort ==
    /\ phase = "sending"
    /\ \E c \in Chunks: attempts[c] > RetryLimit
    /\ phase' = "failed"
    /\ inFlight' = {}
    /\ UNCHANGED << verified, faulted, attempts, progress,
                    maxObservedConcurrency, digestValid, checkpointed, visible >>

Stutter == UNCHANGED vars

Next == Start \/ Schedule \/ DeliverClean \/ DeliverFault \/ BeginVerify \/
        VerifyDigest \/ CommitCheckpoint \/ Abort \/ Stutter

TypeOK ==
    /\ ChunkCount \in Nat \ {0}
    /\ RetryLimit \in Nat
    /\ MaxConcurrent \in Nat \ {0}
    /\ phase \in Phases
    /\ inFlight \subseteq Chunks
    /\ verified \subseteq Chunks
    /\ faulted \subseteq Chunks
    /\ attempts \in [Chunks -> 0..(RetryLimit + 1)]
    /\ progress \in 0..ChunkCount
    /\ maxObservedConcurrency \in 0..MaxConcurrent
    /\ digestValid \in BOOLEAN
    /\ checkpointed \in BOOLEAN
    /\ visible \in BOOLEAN

ConcurrencyIsBounded == Cardinality(inFlight) <= MaxConcurrent
ProgressMatchesVerifiedChunks == progress = Cardinality(verified)
RetriesBelongOnlyToFailedChunks == \A c \in Chunks: attempts[c] > 0 => c \in faulted
InstalledOnlyAfterWholeVerification ==
    visible => /\ phase = "installed"
               /\ verified = Chunks
               /\ progress = ChunkCount
               /\ digestValid
               /\ checkpointed
NoPartialInstall == phase # "installed" => ~visible
TerminalFailureIsInvisible == phase = "failed" => ~visible /\ ~checkpointed

=============================================================================
