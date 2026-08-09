--------------------- MODULE model ---------------------
EXTENDS Naturals

MaxIndex == 3

VARIABLES \* @type: Int;
          pc,
          \* @type: Int;
          durable,
          \* @type: Int;
          volatile,
          \* @type: Int;
          recovered,
          \* @type: Bool;
          walCorrupt,
          \* @type: Int;
          snapshotDurable,
          \* @type: Int;
          snapshotTemp,
          \* @type: Bool;
          snapshotCorrupt,
          \* @type: Bool;
          recoveryRejected,
          \* @type: Bool;
          installRejected,
          \* @type: Int;
          installed

vars == <<pc, durable, volatile, recovered, walCorrupt,
          snapshotDurable, snapshotTemp, snapshotCorrupt,
          recoveryRejected, installRejected, installed>>

Init ==
    /\ pc = 0
    /\ durable = 0
    /\ volatile = 0
    /\ recovered = 0
    /\ walCorrupt = FALSE
    /\ snapshotDurable = 0
    /\ snapshotTemp = 0
    /\ snapshotCorrupt = FALSE
    /\ recoveryRejected = FALSE
    /\ installRejected = FALSE
    /\ installed = 0

Append ==
    /\ volatile < MaxIndex
    /\ volatile' = volatile + 1
    /\ UNCHANGED <<durable, recovered, walCorrupt, snapshotDurable,
                    snapshotTemp, snapshotCorrupt, recoveryRejected,
                    installRejected, installed>>

Fsync ==
    /\ durable' = volatile
    /\ UNCHANGED <<volatile, recovered, walCorrupt, snapshotDurable,
                    snapshotTemp, snapshotCorrupt, recoveryRejected,
                    installRejected, installed>>

Crash ==
    /\ volatile' = durable
    /\ snapshotTemp' = 0
    /\ UNCHANGED <<durable, recovered, walCorrupt, snapshotDurable,
                    snapshotCorrupt, recoveryRejected, installRejected, installed>>

CorruptWal ==
    /\ walCorrupt' = TRUE
    /\ recovered' = 0
    /\ UNCHANGED <<durable, volatile, snapshotDurable,
                    snapshotTemp, snapshotCorrupt, recoveryRejected,
                    installRejected, installed>>

Recover ==
    /\ recovered' = IF walCorrupt THEN recovered ELSE durable
    /\ recoveryRejected' = (recoveryRejected \/ walCorrupt)
    /\ UNCHANGED <<durable, volatile, walCorrupt, snapshotDurable,
                    snapshotTemp, snapshotCorrupt, installRejected, installed>>

BuildSnapshot ==
    /\ snapshotTemp' = durable
    /\ UNCHANGED <<durable, volatile, recovered, walCorrupt,
                    snapshotDurable, snapshotCorrupt, recoveryRejected,
                    installRejected, installed>>

PublishSnapshot ==
    /\ snapshotTemp > 0
    /\ snapshotDurable' = snapshotTemp
    /\ snapshotTemp' = 0
    /\ UNCHANGED <<durable, volatile, recovered, walCorrupt,
                    snapshotCorrupt, recoveryRejected, installRejected, installed>>

CorruptSnapshot ==
    /\ snapshotDurable > 0
    /\ snapshotCorrupt' = TRUE
    /\ installed' = 0
    /\ UNCHANGED <<durable, volatile, recovered, walCorrupt,
                    snapshotDurable, snapshotTemp, recoveryRejected,
                    installRejected>>

InstallSnapshot ==
    /\ snapshotDurable > 0
    /\ installed' = IF snapshotCorrupt THEN installed ELSE snapshotDurable
    /\ installRejected' = (installRejected \/ snapshotCorrupt)
    /\ UNCHANGED <<durable, volatile, recovered, walCorrupt,
                    snapshotDurable, snapshotTemp, snapshotCorrupt,
                    recoveryRejected>>

Step == Append \/ Fsync \/ Crash \/ CorruptWal \/ Recover \/
        BuildSnapshot \/ PublishSnapshot \/ CorruptSnapshot \/ InstallSnapshot

Next == /\ pc < 10 /\ pc' = pc + 1 /\ Step
Spec == Init /\ [][Next]_vars

TypeInvariant ==
    /\ pc \in 0..10
    /\ durable \in 0..MaxIndex
    /\ volatile \in 0..MaxIndex
    /\ recovered \in 0..MaxIndex
    /\ snapshotDurable \in 0..MaxIndex
    /\ snapshotTemp \in 0..MaxIndex
    /\ installed \in 0..MaxIndex
    /\ walCorrupt \in BOOLEAN
    /\ snapshotCorrupt \in BOOLEAN
    /\ recoveryRejected \in BOOLEAN
    /\ installRejected \in BOOLEAN

NoUnflushedRecovery == recovered <= durable
CorruptWalNeverAccepted == walCorrupt => recovered = 0
TempSnapshotNeverInstalled == snapshotTemp = 0 \/ installed <= snapshotDurable
CorruptSnapshotNeverInstalled == snapshotCorrupt => installed = 0
=======================================================
