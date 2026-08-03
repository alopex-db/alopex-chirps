--------------------------- MODULE FileTransfer ---------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Int;
    RetryLimit

Encodings == {"none", "zstd"}
Phases == {"new", "sending", "awaiting_retry", "verifying", "completed", "failed"}
HashStates == {"unknown", "valid", "invalid"}

VARIABLES \* @type: Str;
          phase,
          \* @type: Str;
          senderEncoding,
          \* @type: Str;
          wireEncoding,
          \* @type: Str;
          receiverEncoding,
          \* @type: Str;
          hashStatus,
          \* @type: Bool;
          installed,
          \* @type: Bool;
          complete,
          \* @type: Int;
          retries,
          \* @type: Bool;
          sawCorruption

vars == << phase,
           senderEncoding,
           wireEncoding,
           receiverEncoding,
           hashStatus,
           installed,
           complete,
           retries,
           sawCorruption >>

Init ==
    /\ phase = "new"
    /\ senderEncoding = "none"
    /\ wireEncoding = "none"
    /\ receiverEncoding = "unknown"
    /\ hashStatus = "unknown"
    /\ installed = FALSE
    /\ complete = FALSE
    /\ retries = 0
    /\ sawCorruption = FALSE

Start ==
    \E encoding \in Encodings:
        /\ phase = "new"
        /\ phase' = "sending"
        /\ senderEncoding' = encoding
        /\ wireEncoding' = encoding
        /\ UNCHANGED << receiverEncoding,
                       hashStatus,
                       installed,
                       complete,
                       retries,
                       sawCorruption >>

DeliverClean ==
    /\ phase = "sending"
    /\ phase' = "verifying"
    /\ receiverEncoding' = wireEncoding
    /\ hashStatus' = "valid"
    /\ UNCHANGED << senderEncoding,
                   wireEncoding,
                   installed,
                   complete,
                   retries,
                   sawCorruption >>

DeliverCorrupt ==
    /\ phase = "sending"
    /\ phase' = "awaiting_retry"
    /\ receiverEncoding' = "unknown"
    /\ hashStatus' = "invalid"
    /\ sawCorruption' = TRUE
    /\ UNCHANGED << senderEncoding,
                   wireEncoding,
                   installed,
                   complete,
                   retries >>

DropChunk ==
    /\ phase = "sending"
    /\ phase' = "awaiting_retry"
    /\ UNCHANGED << senderEncoding,
                   wireEncoding,
                   receiverEncoding,
                   hashStatus,
                   installed,
                   complete,
                   retries,
                   sawCorruption >>

Retry ==
    /\ phase = "awaiting_retry"
    /\ retries < RetryLimit
    /\ phase' = "sending"
    /\ retries' = retries + 1
    /\ UNCHANGED << senderEncoding,
                   wireEncoding,
                   receiverEncoding,
                   hashStatus,
                   installed,
                   complete,
                   sawCorruption >>

Abort ==
    /\ phase = "awaiting_retry"
    /\ retries = RetryLimit
    /\ phase' = "failed"
    /\ UNCHANGED << senderEncoding,
                   wireEncoding,
                   receiverEncoding,
                   hashStatus,
                   installed,
                   complete,
                   retries,
                   sawCorruption >>

Finalize ==
    /\ phase = "verifying"
    /\ hashStatus = "valid"
    /\ receiverEncoding = senderEncoding
    /\ phase' = "completed"
    /\ installed' = TRUE
    /\ complete' = TRUE
    /\ UNCHANGED << senderEncoding,
                   wireEncoding,
                   receiverEncoding,
                   hashStatus,
                   retries,
                   sawCorruption >>

Stutter == UNCHANGED vars

Next == Start \/ DeliverClean \/ DeliverCorrupt \/ DropChunk \/ Retry \/ Abort \/ Finalize \/ Stutter

TypeOK ==
    /\ RetryLimit \in Nat
    /\ phase \in Phases
    /\ senderEncoding \in Encodings
    /\ wireEncoding \in Encodings
    /\ receiverEncoding \in (Encodings \cup {"unknown"})
    /\ hashStatus \in HashStates
    /\ installed \in BOOLEAN
    /\ complete \in BOOLEAN
    /\ retries \in 0..RetryLimit
    /\ sawCorruption \in BOOLEAN

WireMetadataMatchesPayload ==
    phase # "new" => wireEncoding = senderEncoding

ReceiverDecodesTheAdvertisedEncoding ==
    receiverEncoding # "unknown" => receiverEncoding = senderEncoding

InstalledOnlyAfterVerifiedHash ==
    installed => hashStatus = "valid"

CompleteImpliesVerifiedInstall ==
    complete => /\ phase = "completed"
                /\ installed
                /\ hashStatus = "valid"
                /\ receiverEncoding = senderEncoding

CorruptionRequiresRetryBeforeSuccess ==
    sawCorruption /\ complete => retries > 0

TerminalStateIsConsistent ==
    /\ phase = "completed" => complete /\ installed
    /\ phase = "failed" => ~complete /\ ~installed

=============================================================================
