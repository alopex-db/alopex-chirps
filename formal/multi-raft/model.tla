--------------------------- MODULE model ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANT
    \* @type: Int;
    MaxInFlight

Groups == {"g1", "g2"}
BootstrapGroup == "g1"
Replicas == {"n1", "n2", "n3"}
SeedReplica == "n1"
RemoteReplicas == Replicas \ {SeedReplica}

Phases == {"absent", "creating", "active", "draining", "stopped"}
StorageStates == {"none", "temporary", "committed"}
Namespaces == {"none", "groups/0000000000000001", "groups/0000000000000002"}
ReplicaRoles == {"absent", "uninitialized", "learner", "caught_up", "voter"}
TickStates == {"idle", "pending", "success", "failure", "cancelled"}
PendingStates == {"idle", "waiting", "delivered", "timed_out"}
Correlations == {"c1", "c2"}
CorrelationTargets == Correlations \cup {"none"}
CommitValues == {"none", "value_a", "value_b"}

Events == {
    "none", "create_started", "seed_prepared", "seed_published", "create_aborted",
    "duplicate_create", "invalid_namespace", "replica_published_uninitialized",
    "learner_added", "learner_caught_up", "learner_promoted", "bootstrap_ready",
    "leader_elected", "proposal_committed", "commit_replicated",
    "request_dispatched", "request_applied", "route_unknown", "route_unavailable",
    "rpc_sent", "response_delivered", "response_rejected", "rpc_timed_out",
    "tick_started", "tick_succeeded", "tick_failed", "remove_started",
    "shutdown", "removed", "repeated_remove", "replica_failed",
    "fail_replica_publish", "fail_learner_add", "fail_learner_catchup",
    "fail_learner_promote"
}
EventGroups == Groups \cup {"none"}
EventReplicas == Replicas \cup {"none"}
FailureEvents == {
    "rpc_timed_out", "replica_failed", "fail_replica_publish",
    "fail_learner_add", "fail_learner_catchup", "fail_learner_promote"
}
AtomicMembershipFailureEvents == {
    "fail_replica_publish", "fail_learner_add",
    "fail_learner_catchup", "fail_learner_promote"
}
RejectedRouteEvents == {"route_unknown", "route_unavailable", "response_rejected"}
RejectedLifecycleEvents == {"duplicate_create", "invalid_namespace", "repeated_remove"}

CanonicalNamespace(g) ==
    IF g = "g1"
    THEN "groups/0000000000000001"
    ELSE "groups/0000000000000002"

VARIABLES
    \* @type: Str -> Str;
    phase,
    \* @type: Str -> Str;
    storage,
    \* @type: Str -> Str;
    namespace,
    \* @type: Str -> Bool;
    seedReady,
    \* @type: Str -> Bool;
    transportRegistered,
    \* @type: Str -> Bool;
    shutdownComplete,

    \* @type: Str -> (Str -> Bool);
    replicaLocalReady,
    \* @type: Str -> (Str -> Bool);
    replicaPublished,
    \* @type: Str -> (Str -> Str);
    replicaRole,
    \* @type: Str -> (Str -> Bool);
    replicaAvailable,
    \* @type: Str -> (Str -> Bool);
    promotionCertified,

    \* @type: Str -> (Str -> Bool);
    leaderClaim,
    \* @type: Str -> Int;
    term,
    \* @type: Str -> Bool;
    bootstrapReady,
    \* @type: Str -> Int;
    commitIndex,
    \* @type: Str -> Str;
    committedValue,
    \* @type: Str -> (Str -> Int);
    replicaCommit,
    \* @type: Str -> (Str -> Str);
    replicaValue,
    \* @type: Str -> Bool;
    commitCertified,
    \* @type: Str -> Int;
    commitVoterCount,
    \* @type: Str -> Int;
    commitAckCount,

    \* @type: Str -> Int;
    routeInFlight,
    \* @type: Str -> Int;
    tickInFlight,
    \* @type: Str -> Bool;
    tickMember,
    \* @type: Str -> Str;
    tickStatus,
    \* @type: Str -> Int;
    groupVersion,

    \* @type: Str -> Str;
    pendingCorrelation,
    \* @type: Str -> Str;
    pendingTarget,
    \* @type: Str -> Str;
    pendingStatus,

    \* @type: Str;
    lastEvent,
    \* @type: Str;
    lastEventGroup,
    \* @type: Str;
    lastEventReplica,
    \* @type: Str;
    lastEventCorrelation,
    \* @type: Str -> Int;
    versionSnapshot,
    \* @type: Str -> Str;
    pendingCorrelationSnapshot,
    \* @type: Str -> Str;
    pendingTargetSnapshot,
    \* @type: Str;
    phaseSnapshot,
    \* @type: Str;
    storageSnapshot,
    \* @type: Str;
    namespaceSnapshot,
    \* @type: Str;
    roleSnapshot,
    \* @type: Bool;
    publishedSnapshot,
    \* @type: Int;
    commitIndexSnapshot,
    \* @type: Str;
    committedValueSnapshot,
    \* @type: Int;
    replicaCommitSnapshot,
    \* @type: Str;
    replicaValueSnapshot

LifecycleCore == << phase, storage, namespace, seedReady,
                    transportRegistered, shutdownComplete >>
ReplicaCore == << replicaLocalReady, replicaPublished, replicaRole,
                 replicaAvailable, promotionCertified >>
ConsensusCore == << leaderClaim, term, bootstrapReady, commitIndex,
                   committedValue, replicaCommit, replicaValue,
                   commitCertified, commitVoterCount, commitAckCount >>
WorkCore == << routeInFlight, tickInFlight, tickMember, tickStatus,
              groupVersion >>
\* @type: <<Str -> Str, Str -> Str, Str -> Str>>;
PendingCore == << pendingCorrelation, pendingTarget, pendingStatus >>
ObservationCore == << lastEvent, lastEventGroup, lastEventReplica,
                     lastEventCorrelation, versionSnapshot,
                     pendingCorrelationSnapshot, pendingTargetSnapshot,
                     phaseSnapshot, storageSnapshot, namespaceSnapshot,
                     roleSnapshot, publishedSnapshot, commitIndexSnapshot,
                     committedValueSnapshot, replicaCommitSnapshot,
                     replicaValueSnapshot >>

vars == << phase, storage, namespace, seedReady, transportRegistered,
           shutdownComplete, replicaLocalReady, replicaPublished, replicaRole,
           replicaAvailable, promotionCertified, leaderClaim, term,
           bootstrapReady, commitIndex, committedValue, replicaCommit,
           replicaValue, commitCertified, commitVoterCount, commitAckCount,
           routeInFlight, tickInFlight,
           tickMember, tickStatus, groupVersion, pendingCorrelation,
           pendingTarget, pendingStatus, lastEvent, lastEventGroup,
           lastEventReplica, lastEventCorrelation, versionSnapshot,
           pendingCorrelationSnapshot, pendingTargetSnapshot, phaseSnapshot,
           storageSnapshot, namespaceSnapshot, roleSnapshot,
           publishedSnapshot, commitIndexSnapshot, committedValueSnapshot,
           replicaCommitSnapshot, replicaValueSnapshot >>

Voters(g) == {r \in Replicas: replicaRole[g][r] = "voter"}
Leaders(g) == {r \in Replicas: leaderClaim[g][r]}
AvailableVoters(g) == {r \in Voters(g): replicaAvailable[g][r]}

Init ==
    /\ phase = [g \in Groups |-> "absent"]
    /\ storage = [g \in Groups |-> "none"]
    /\ namespace = [g \in Groups |-> "none"]
    /\ seedReady = [g \in Groups |-> FALSE]
    /\ transportRegistered = [g \in Groups |-> FALSE]
    /\ shutdownComplete = [g \in Groups |-> FALSE]
    /\ replicaLocalReady = [g \in Groups |-> [r \in Replicas |-> FALSE]]
    /\ replicaPublished = [g \in Groups |-> [r \in Replicas |-> FALSE]]
    /\ replicaRole = [g \in Groups |-> [r \in Replicas |-> "absent"]]
    /\ replicaAvailable = [g \in Groups |-> [r \in Replicas |-> FALSE]]
    /\ promotionCertified = [g \in Groups |-> [r \in Replicas |-> FALSE]]
    /\ leaderClaim = [g \in Groups |-> [r \in Replicas |-> FALSE]]
    /\ term = [g \in Groups |-> 0]
    /\ bootstrapReady = [g \in Groups |-> FALSE]
    /\ commitIndex = [g \in Groups |-> 0]
    /\ committedValue = [g \in Groups |-> "none"]
    /\ replicaCommit = [g \in Groups |-> [r \in Replicas |-> 0]]
    /\ replicaValue = [g \in Groups |-> [r \in Replicas |-> "none"]]
    /\ commitCertified = [g \in Groups |-> FALSE]
    /\ commitVoterCount = [g \in Groups |-> 0]
    /\ commitAckCount = [g \in Groups |-> 0]
    /\ routeInFlight = [g \in Groups |-> 0]
    /\ tickInFlight = [g \in Groups |-> 0]
    /\ tickMember = [g \in Groups |-> FALSE]
    /\ tickStatus = [g \in Groups |-> "idle"]
    /\ groupVersion = [g \in Groups |-> 0]
    /\ pendingCorrelation = [g \in Groups |-> "none"]
    /\ pendingTarget = [g \in Groups |-> "none"]
    /\ pendingStatus = [g \in Groups |-> "idle"]
    /\ lastEvent = "none"
    /\ lastEventGroup = "none"
    /\ lastEventReplica = "none"
    /\ lastEventCorrelation = "none"
    /\ versionSnapshot = groupVersion
    /\ pendingCorrelationSnapshot = pendingCorrelation
    /\ pendingTargetSnapshot = pendingTarget
    /\ phaseSnapshot = "absent"
    /\ storageSnapshot = "none"
    /\ namespaceSnapshot = "none"
    /\ roleSnapshot = "absent"
    /\ publishedSnapshot = FALSE
    /\ commitIndexSnapshot = 0
    /\ committedValueSnapshot = "none"
    /\ replicaCommitSnapshot = 0
    /\ replicaValueSnapshot = "none"

Observe(event, g, r, correlation) ==
    /\ lastEvent' = event
    /\ lastEventGroup' = g
    /\ lastEventReplica' = r
    /\ lastEventCorrelation' = correlation
    /\ versionSnapshot' = groupVersion
    /\ pendingCorrelationSnapshot' = pendingCorrelation
    /\ pendingTargetSnapshot' = pendingTarget
    /\ phaseSnapshot' = IF g \in Groups THEN phase[g] ELSE "absent"
    /\ storageSnapshot' = IF g \in Groups THEN storage[g] ELSE "none"
    /\ namespaceSnapshot' = IF g \in Groups THEN namespace[g] ELSE "none"
    /\ roleSnapshot' =
        IF g \in Groups /\ r \in Replicas THEN replicaRole[g][r] ELSE "absent"
    /\ publishedSnapshot' =
        IF g \in Groups /\ r \in Replicas THEN replicaPublished[g][r] ELSE FALSE
    /\ commitIndexSnapshot' = IF g \in Groups THEN commitIndex[g] ELSE 0
    /\ committedValueSnapshot' =
        IF g \in Groups THEN committedValue[g] ELSE "none"
    /\ replicaCommitSnapshot' =
        IF g \in Groups /\ r \in Replicas THEN replicaCommit[g][r] ELSE 0
    /\ replicaValueSnapshot' =
        IF g \in Groups /\ r \in Replicas THEN replicaValue[g][r] ELSE "none"

BeginCreate(g) ==
    /\ phase[g] = "absent"
    /\ phase' = [phase EXCEPT ![g] = "creating"]
    /\ storage' = [storage EXCEPT ![g] = "temporary"]
    /\ namespace' = [namespace EXCEPT ![g] = CanonicalNamespace(g)]
    /\ shutdownComplete' = [shutdownComplete EXCEPT ![g] = FALSE]
    /\ UNCHANGED << seedReady, transportRegistered, ReplicaCore,
                    ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("create_started", g, "none", "none")

PrepareSeed(g) ==
    /\ phase[g] = "creating"
    /\ storage[g] = "temporary"
    /\ ~seedReady[g]
    /\ seedReady' = [seedReady EXCEPT ![g] = TRUE]
    /\ transportRegistered' = [transportRegistered EXCEPT ![g] = TRUE]
    /\ replicaLocalReady' = [replicaLocalReady EXCEPT ![g][SeedReplica] = TRUE]
    /\ replicaAvailable' = [replicaAvailable EXCEPT ![g][SeedReplica] = TRUE]
    /\ UNCHANGED << phase, storage, namespace, shutdownComplete,
                    replicaPublished, replicaRole, promotionCertified,
                    ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("seed_prepared", g, SeedReplica, "none")

PublishSeed(g) ==
    /\ phase[g] = "creating"
    /\ storage[g] = "temporary"
    /\ namespace[g] = CanonicalNamespace(g)
    /\ seedReady[g]
    /\ transportRegistered[g]
    /\ replicaLocalReady[g][SeedReplica]
    /\ phase' = [phase EXCEPT ![g] = "active"]
    /\ storage' = [storage EXCEPT ![g] = "committed"]
    /\ replicaPublished' = [replicaPublished EXCEPT ![g][SeedReplica] = TRUE]
    /\ replicaRole' = [replicaRole EXCEPT ![g][SeedReplica] = "voter"]
    /\ promotionCertified' = [promotionCertified EXCEPT ![g][SeedReplica] = TRUE]
    /\ leaderClaim' = [leaderClaim EXCEPT ![g] = [r \in Replicas |-> r = SeedReplica]]
    /\ term' = [term EXCEPT ![g] = 1]
    /\ UNCHANGED << namespace, seedReady, transportRegistered,
                    shutdownComplete, replicaLocalReady, replicaAvailable,
                    bootstrapReady, commitIndex, committedValue,
                    replicaCommit, replicaValue, commitCertified,
                    commitVoterCount, commitAckCount,
                    WorkCore, PendingCore >>
    /\ Observe("seed_published", g, SeedReplica, "none")

AbortCreate(g) ==
    /\ phase[g] = "creating"
    /\ phase' = [phase EXCEPT ![g] = "absent"]
    /\ storage' = [storage EXCEPT ![g] = "none"]
    /\ namespace' = [namespace EXCEPT ![g] = "none"]
    /\ seedReady' = [seedReady EXCEPT ![g] = FALSE]
    /\ transportRegistered' = [transportRegistered EXCEPT ![g] = FALSE]
    /\ replicaLocalReady' = [replicaLocalReady EXCEPT ![g] = [r \in Replicas |-> FALSE]]
    /\ replicaAvailable' = [replicaAvailable EXCEPT ![g] = [r \in Replicas |-> FALSE]]
    /\ UNCHANGED << shutdownComplete, replicaPublished, replicaRole,
                    promotionCertified, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("create_aborted", g, "none", "none")

RejectDuplicateCreate(g) ==
    /\ phase[g] # "absent"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("duplicate_create", g, "none", "none")

RejectInvalidNamespace(g) ==
    /\ phase[g] = "absent"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("invalid_namespace", g, "none", "none")

PublishUninitializedReplica(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ \/ r = "n2"
       \/ /\ r = "n3"
          /\ replicaRole[g]["n2"] = "voter"
    /\ replicaRole[g][r] = "absent"
    /\ ~replicaPublished[g][r]
    /\ replicaLocalReady' = [replicaLocalReady EXCEPT ![g][r] = TRUE]
    /\ replicaPublished' = [replicaPublished EXCEPT ![g][r] = TRUE]
    /\ replicaRole' = [replicaRole EXCEPT ![g][r] = "uninitialized"]
    /\ replicaAvailable' = [replicaAvailable EXCEPT ![g][r] = TRUE]
    /\ UNCHANGED << LifecycleCore, promotionCertified, ConsensusCore,
                    WorkCore, PendingCore >>
    /\ Observe("replica_published_uninitialized", g, r, "none")

FailReplicaPublication(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ replicaRole[g][r] = "absent"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("fail_replica_publish", g, r, "none")

AddLearner(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ replicaPublished[g][r]
    /\ replicaAvailable[g][r]
    /\ replicaRole[g][r] = "uninitialized"
    /\ replicaRole' = [replicaRole EXCEPT ![g][r] = "learner"]
    /\ UNCHANGED << LifecycleCore, replicaLocalReady, replicaPublished,
                    replicaAvailable, promotionCertified, ConsensusCore,
                    WorkCore, PendingCore >>
    /\ Observe("learner_added", g, r, "none")

FailLearnerAdd(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ replicaRole[g][r] = "uninitialized"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("fail_learner_add", g, r, "none")

CatchUpLearner(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ replicaRole[g][r] = "learner"
    /\ replicaAvailable[g][r]
    /\ Cardinality(Leaders(g)) = 1
    /\ replicaRole' = [replicaRole EXCEPT ![g][r] = "caught_up"]
    /\ replicaCommit' = [replicaCommit EXCEPT ![g][r] = commitIndex[g]]
    /\ replicaValue' = [replicaValue EXCEPT ![g][r] = committedValue[g]]
    /\ UNCHANGED << LifecycleCore, replicaLocalReady, replicaPublished,
                    replicaAvailable, promotionCertified, leaderClaim, term,
                    bootstrapReady, commitIndex, committedValue,
                    commitCertified, commitVoterCount, commitAckCount,
                    WorkCore, PendingCore >>
    /\ Observe("learner_caught_up", g, r, "none")

FailLearnerCatchUp(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ replicaRole[g][r] = "learner"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("fail_learner_catchup", g, r, "none")

PromoteLearner(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ replicaRole[g][r] = "caught_up"
    /\ replicaCommit[g][r] = commitIndex[g]
    /\ replicaValue[g][r] = committedValue[g]
    /\ replicaRole' = [replicaRole EXCEPT ![g][r] = "voter"]
    /\ promotionCertified' = [promotionCertified EXCEPT ![g][r] = TRUE]
    /\ UNCHANGED << LifecycleCore, replicaLocalReady, replicaPublished,
                    replicaAvailable, leaderClaim, term, bootstrapReady,
                    commitIndex, committedValue, replicaCommit, replicaValue,
                    commitCertified, commitVoterCount, commitAckCount,
                    WorkCore, PendingCore >>
    /\ Observe("learner_promoted", g, r, "none")

FailLearnerPromote(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r \in RemoteReplicas
    /\ replicaRole[g][r] = "caught_up"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("fail_learner_promote", g, r, "none")

MarkBootstrapReady(g) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ Voters(g) = Replicas
    /\ ~bootstrapReady[g]
    /\ bootstrapReady' = [bootstrapReady EXCEPT ![g] = TRUE]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, leaderClaim, term,
                    commitIndex, committedValue, replicaCommit, replicaValue,
                    commitCertified, commitVoterCount, commitAckCount,
                    WorkCore, PendingCore >>
    /\ Observe("bootstrap_ready", g, "none", "none")

FailReplica(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ replicaPublished[g][r]
    /\ replicaAvailable[g][r]
    /\ replicaAvailable' = [replicaAvailable EXCEPT ![g][r] = FALSE]
    /\ leaderClaim' = [leaderClaim EXCEPT ![g][r] = FALSE]
    /\ UNCHANGED << LifecycleCore, replicaLocalReady, replicaPublished,
                    replicaRole, promotionCertified, term, bootstrapReady,
                    commitIndex, committedValue, replicaCommit, replicaValue,
                    commitCertified, commitVoterCount, commitAckCount,
                    WorkCore, PendingCore >>
    /\ Observe("replica_failed", g, r, "none")

ElectLeader(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ replicaRole[g][r] = "voter"
    /\ replicaPublished[g][r]
    /\ replicaAvailable[g][r]
    /\ term[g] < 2
    /\ leaderClaim' = [leaderClaim EXCEPT ![g] = [candidate \in Replicas |-> candidate = r]]
    /\ term' = [term EXCEPT ![g] = @ + 1]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, bootstrapReady,
                    commitIndex, committedValue, replicaCommit, replicaValue,
                    commitCertified, commitVoterCount, commitAckCount,
                    WorkCore, PendingCore >>
    /\ Observe("leader_elected", g, r, "none")

CommitProposal(g, r, value) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ leaderClaim[g][r]
    /\ value = "value_a"
    /\ commitIndex[g] = 0
    /\ groupVersion[g] < 2
    /\ 2 * Cardinality(AvailableVoters(g)) > Cardinality(Voters(g))
    /\ commitIndex' = [commitIndex EXCEPT ![g] = 1]
    /\ committedValue' = [committedValue EXCEPT ![g] = value]
    /\ replicaCommit' = [replicaCommit EXCEPT ![g][r] = 1]
    /\ replicaValue' = [replicaValue EXCEPT ![g][r] = value]
    /\ commitCertified' = [commitCertified EXCEPT ![g] = TRUE]
    /\ commitVoterCount' = [commitVoterCount EXCEPT ![g] = Cardinality(Voters(g))]
    /\ commitAckCount' = [commitAckCount EXCEPT ![g] = Cardinality(AvailableVoters(g))]
    /\ groupVersion' = [groupVersion EXCEPT ![g] = @ + 1]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, leaderClaim, term,
                    bootstrapReady, routeInFlight, tickInFlight,
                    tickMember, tickStatus, PendingCore >>
    /\ Observe("proposal_committed", g, r, "none")

ReplicateCommit(g, r) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ commitIndex[g] = 1
    /\ replicaPublished[g][r]
    /\ replicaAvailable[g][r]
    /\ replicaRole[g][r] \in {"learner", "caught_up", "voter"}
    /\ replicaCommit[g][r] = 0
    /\ replicaCommit' = [replicaCommit EXCEPT ![g][r] = 1]
    /\ replicaValue' = [replicaValue EXCEPT ![g][r] = committedValue[g]]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, leaderClaim, term,
                    bootstrapReady, commitIndex, committedValue,
                    commitCertified, commitVoterCount, commitAckCount,
                    WorkCore, PendingCore >>
    /\ Observe("commit_replicated", g, r, "none")

DispatchRequest(g) ==
    /\ phase[g] = "active"
    /\ routeInFlight[g] < MaxInFlight
    /\ routeInFlight' = [routeInFlight EXCEPT ![g] = @ + 1]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore,
                    tickInFlight, tickMember, tickStatus, groupVersion,
                    PendingCore >>
    /\ Observe("request_dispatched", g, "none", "none")

FinishRequest(g) ==
    /\ routeInFlight[g] > 0
    /\ phase[g] \in {"active", "draining"}
    /\ groupVersion[g] < 2
    /\ routeInFlight' = [routeInFlight EXCEPT ![g] = @ - 1]
    /\ groupVersion' = [groupVersion EXCEPT ![g] = @ + 1]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore,
                    tickInFlight, tickMember, tickStatus, PendingCore >>
    /\ Observe("request_applied", g, "none", "none")

RejectUnknownRoute(g) ==
    /\ phase[g] = "absent"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("route_unknown", g, "none", "none")

RejectUnavailableRoute(g) ==
    /\ phase[g] \in {"creating", "draining", "stopped"}
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("route_unavailable", g, "none", "none")

SendCorrelatedRequest(g, r, correlation) ==
    /\ g = BootstrapGroup
    /\ phase[g] = "active"
    /\ r = "n2"
    /\ replicaPublished[g][r]
    /\ replicaAvailable[g][r]
    /\ correlation = "c1"
    /\ pendingCorrelation[g] = "none"
    /\ pendingCorrelation' = [pendingCorrelation EXCEPT ![g] = correlation]
    /\ pendingTarget' = [pendingTarget EXCEPT ![g] = r]
    /\ pendingStatus' = [pendingStatus EXCEPT ![g] = "waiting"]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore >>
    /\ Observe("rpc_sent", g, r, correlation)

DispatchMatchedResponse(g, r, correlation) ==
    /\ g = BootstrapGroup
    /\ phase[g] \in {"active", "draining"}
    /\ r = "n2"
    /\ correlation = "c1"
    /\ pendingCorrelation[g] = correlation
    /\ pendingTarget[g] = r
    /\ pendingStatus[g] = "waiting"
    /\ pendingCorrelation' = [pendingCorrelation EXCEPT ![g] = "none"]
    /\ pendingTarget' = [pendingTarget EXCEPT ![g] = "none"]
    /\ pendingStatus' = [pendingStatus EXCEPT ![g] = "delivered"]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore >>
    /\ Observe("response_delivered", g, r, correlation)

RejectMismatchedResponse(g, r, correlation) ==
    /\ phase[BootstrapGroup] \in {"active", "draining"}
    /\ r \in Replicas
    /\ correlation \in Correlations
    /\ pendingStatus[BootstrapGroup] = "waiting"
    /\ \/ g # BootstrapGroup
       \/ pendingCorrelation[BootstrapGroup] # correlation
       \/ pendingTarget[BootstrapGroup] # r
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("response_rejected", g, r, correlation)

TimeoutPendingRpc(g) ==
    /\ pendingStatus[g] = "waiting"
    /\ pendingCorrelation[g] \in Correlations
    /\ pendingCorrelation' = [pendingCorrelation EXCEPT ![g] = "none"]
    /\ pendingTarget' = [pendingTarget EXCEPT ![g] = "none"]
    /\ pendingStatus' = [pendingStatus EXCEPT ![g] = "timed_out"]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore >>
    /\ Observe("rpc_timed_out", g, "none", "none")

StartTickRound ==
    /\ \A g \in Groups: tickInFlight[g] = 0
    /\ \E g \in Groups: phase[g] = "active"
    /\ tickMember' = [g \in Groups |-> phase[g] = "active"]
    /\ tickStatus' = [g \in Groups |-> IF phase[g] = "active" THEN "pending" ELSE "idle"]
    /\ tickInFlight' = [g \in Groups |-> IF phase[g] = "active" THEN 1 ELSE 0]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore,
                    routeInFlight, groupVersion, PendingCore >>
    /\ Observe("tick_started", "none", "none", "none")

FinishTickSuccess(g) ==
    /\ tickInFlight[g] = 1
    /\ tickStatus[g] = "pending"
    /\ groupVersion[g] < 2
    /\ tickInFlight' = [tickInFlight EXCEPT ![g] = 0]
    /\ tickStatus' = [tickStatus EXCEPT ![g] = "success"]
    /\ groupVersion' = [groupVersion EXCEPT ![g] = @ + 1]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore,
                    routeInFlight, tickMember, PendingCore >>
    /\ Observe("tick_succeeded", g, "none", "none")

FinishTickFailure(g) ==
    /\ tickInFlight[g] = 1
    /\ tickStatus[g] = "pending"
    /\ tickInFlight' = [tickInFlight EXCEPT ![g] = 0]
    /\ tickStatus' = [tickStatus EXCEPT ![g] = "failure"]
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore,
                    routeInFlight, tickMember, groupVersion, PendingCore >>
    /\ Observe("tick_failed", g, "none", "none")

BeginRemove(g) ==
    /\ phase[g] = "active"
    /\ phase' = [phase EXCEPT ![g] = "draining"]
    /\ UNCHANGED << storage, namespace, seedReady, transportRegistered,
                    shutdownComplete, ReplicaCore, ConsensusCore,
                    WorkCore, PendingCore >>
    /\ Observe("remove_started", g, "none", "none")

Shutdown(g) ==
    /\ phase[g] = "draining"
    /\ routeInFlight[g] = 0
    /\ tickInFlight[g] = 0
    /\ pendingCorrelation[g] = "none"
    /\ phase' = [phase EXCEPT ![g] = "stopped"]
    /\ transportRegistered' = [transportRegistered EXCEPT ![g] = FALSE]
    /\ shutdownComplete' = [shutdownComplete EXCEPT ![g] = TRUE]
    /\ UNCHANGED << storage, namespace, seedReady, ReplicaCore,
                    ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("shutdown", g, "none", "none")

FinishRemove(g) ==
    /\ phase[g] = "stopped"
    /\ shutdownComplete[g]
    /\ phase' = [phase EXCEPT ![g] = "absent"]
    /\ storage' = [storage EXCEPT ![g] = "none"]
    /\ namespace' = [namespace EXCEPT ![g] = "none"]
    /\ seedReady' = [seedReady EXCEPT ![g] = FALSE]
    /\ shutdownComplete' = [shutdownComplete EXCEPT ![g] = FALSE]
    /\ replicaLocalReady' = [replicaLocalReady EXCEPT ![g] = [r \in Replicas |-> FALSE]]
    /\ replicaPublished' = [replicaPublished EXCEPT ![g] = [r \in Replicas |-> FALSE]]
    /\ replicaRole' = [replicaRole EXCEPT ![g] = [r \in Replicas |-> "absent"]]
    /\ replicaAvailable' = [replicaAvailable EXCEPT ![g] = [r \in Replicas |-> FALSE]]
    /\ promotionCertified' = [promotionCertified EXCEPT ![g] = [r \in Replicas |-> FALSE]]
    /\ leaderClaim' = [leaderClaim EXCEPT ![g] = [r \in Replicas |-> FALSE]]
    /\ term' = [term EXCEPT ![g] = 0]
    /\ bootstrapReady' = [bootstrapReady EXCEPT ![g] = FALSE]
    /\ commitIndex' = [commitIndex EXCEPT ![g] = 0]
    /\ committedValue' = [committedValue EXCEPT ![g] = "none"]
    /\ replicaCommit' = [replicaCommit EXCEPT ![g] = [r \in Replicas |-> 0]]
    /\ replicaValue' = [replicaValue EXCEPT ![g] = [r \in Replicas |-> "none"]]
    /\ commitCertified' = [commitCertified EXCEPT ![g] = FALSE]
    /\ commitVoterCount' = [commitVoterCount EXCEPT ![g] = 0]
    /\ commitAckCount' = [commitAckCount EXCEPT ![g] = 0]
    /\ routeInFlight' = [routeInFlight EXCEPT ![g] = 0]
    /\ tickInFlight' = [tickInFlight EXCEPT ![g] = 0]
    /\ tickMember' = [tickMember EXCEPT ![g] = FALSE]
    /\ tickStatus' = [tickStatus EXCEPT ![g] = "idle"]
    /\ groupVersion' = [groupVersion EXCEPT ![g] = 0]
    /\ pendingStatus' = [pendingStatus EXCEPT ![g] = "idle"]
    /\ UNCHANGED << transportRegistered, pendingCorrelation, pendingTarget >>
    /\ Observe("removed", g, "none", "none")

RejectRepeatedRemove(g) ==
    /\ phase[g] # "active"
    /\ UNCHANGED << LifecycleCore, ReplicaCore, ConsensusCore, WorkCore, PendingCore >>
    /\ Observe("repeated_remove", g, "none", "none")

Stutter == UNCHANGED vars

AssumeReadyBootstrap ==
    /\ phase[BootstrapGroup] = "absent"
    /\ phase' = [phase EXCEPT ![BootstrapGroup] = "active"]
    /\ storage' = [storage EXCEPT ![BootstrapGroup] = "committed"]
    /\ namespace' = [namespace EXCEPT
        ![BootstrapGroup] = CanonicalNamespace(BootstrapGroup)]
    /\ seedReady' = [seedReady EXCEPT ![BootstrapGroup] = TRUE]
    /\ transportRegistered' =
        [transportRegistered EXCEPT ![BootstrapGroup] = TRUE]
    /\ replicaLocalReady' = [replicaLocalReady EXCEPT
        ![BootstrapGroup] = [r \in Replicas |-> TRUE]]
    /\ replicaPublished' = [replicaPublished EXCEPT
        ![BootstrapGroup] = [r \in Replicas |-> TRUE]]
    /\ replicaRole' = [replicaRole EXCEPT
        ![BootstrapGroup] = [r \in Replicas |-> "voter"]]
    /\ replicaAvailable' = [replicaAvailable EXCEPT
        ![BootstrapGroup] = [r \in Replicas |-> TRUE]]
    /\ promotionCertified' = [promotionCertified EXCEPT
        ![BootstrapGroup] = [r \in Replicas |-> TRUE]]
    /\ leaderClaim' = [leaderClaim EXCEPT
        ![BootstrapGroup] = [r \in Replicas |-> r = SeedReplica]]
    /\ term' = [term EXCEPT ![BootstrapGroup] = 1]
    /\ bootstrapReady' = [bootstrapReady EXCEPT ![BootstrapGroup] = TRUE]
    /\ UNCHANGED << shutdownComplete, commitIndex, committedValue,
                    replicaCommit, replicaValue, commitCertified,
                    commitVoterCount, commitAckCount, WorkCore, PendingCore >>
    /\ Observe("bootstrap_ready", BootstrapGroup, "none", "none")

BootstrapNext ==
    BeginCreate(BootstrapGroup) \/ PrepareSeed(BootstrapGroup) \/
    PublishSeed(BootstrapGroup) \/
    MarkBootstrapReady(BootstrapGroup) \/
    (\E r \in Replicas:
        PublishUninitializedReplica(BootstrapGroup, r) \/
        AddLearner(BootstrapGroup, r) \/
        CatchUpLearner(BootstrapGroup, r) \/
        PromoteLearner(BootstrapGroup, r))

MembershipFailureNext ==
    BeginCreate(BootstrapGroup) \/ PrepareSeed(BootstrapGroup) \/
    PublishSeed(BootstrapGroup) \/ AbortCreate(BootstrapGroup) \/
    RejectDuplicateCreate(BootstrapGroup) \/
    RejectInvalidNamespace(BootstrapGroup) \/
    (\E r \in Replicas:
        PublishUninitializedReplica(BootstrapGroup, r) \/
        FailReplicaPublication(BootstrapGroup, r) \/
        AddLearner(BootstrapGroup, r) \/ FailLearnerAdd(BootstrapGroup, r) \/
        CatchUpLearner(BootstrapGroup, r) \/
        FailLearnerCatchUp(BootstrapGroup, r) \/
        PromoteLearner(BootstrapGroup, r) \/
        FailLearnerPromote(BootstrapGroup, r))

ConsensusNext ==
    AssumeReadyBootstrap \/
    (\E r \in Replicas:
        FailReplica(BootstrapGroup, r) \/
        ElectLeader(BootstrapGroup, r) \/
        ReplicateCommit(BootstrapGroup, r)) \/
    (\E r \in Replicas, value \in CommitValues:
        CommitProposal(BootstrapGroup, r, value)) \/
    Stutter

CorrelationNext ==
    AssumeReadyBootstrap \/
    SendCorrelatedRequest(BootstrapGroup, "n2", "c1") \/
    RejectMismatchedResponse("g2", "n2", "c1") \/
    RejectMismatchedResponse(BootstrapGroup, "n1", "c1") \/
    RejectMismatchedResponse(BootstrapGroup, "n2", "c2") \/
    DispatchMatchedResponse(BootstrapGroup, "n2", "c1") \/
    TimeoutPendingRpc(BootstrapGroup)

DrainNext ==
    /\ lastEvent = "none"
       /\ BeginCreate(BootstrapGroup)
    \/ /\ lastEvent = "create_started"
          /\ PrepareSeed(BootstrapGroup)
    \/ /\ lastEvent = "seed_prepared"
          /\ PublishSeed(BootstrapGroup)
    \/ /\ lastEvent = "seed_published"
          /\ DispatchRequest(BootstrapGroup)
    \/ /\ lastEvent = "request_dispatched"
          /\ StartTickRound
    \/ /\ lastEvent = "tick_started"
          /\ BeginRemove(BootstrapGroup)
    \/ /\ lastEvent = "remove_started"
          /\ FinishRequest(BootstrapGroup)
    \/ /\ lastEvent = "request_applied"
          /\ FinishTickSuccess(BootstrapGroup)
    \/ /\ lastEvent = "tick_succeeded"
          /\ Shutdown(BootstrapGroup)
    \/ /\ lastEvent = "shutdown"
          /\ FinishRemove(BootstrapGroup)

RoutingNext ==
    BeginCreate(BootstrapGroup) \/ PrepareSeed(BootstrapGroup) \/
    PublishSeed(BootstrapGroup) \/ AbortCreate(BootstrapGroup) \/
    RejectDuplicateCreate(BootstrapGroup) \/
    RejectInvalidNamespace(BootstrapGroup) \/
    DispatchRequest(BootstrapGroup) \/ FinishRequest(BootstrapGroup) \/
    RejectUnknownRoute(BootstrapGroup) \/ RejectUnknownRoute("g2") \/
    RejectUnavailableRoute(BootstrapGroup) \/
    TimeoutPendingRpc(BootstrapGroup) \/
    FinishTickSuccess(BootstrapGroup) \/ FinishTickFailure(BootstrapGroup) \/
    BeginRemove(BootstrapGroup) \/ Shutdown(BootstrapGroup) \/
    FinishRemove(BootstrapGroup) \/ RejectRepeatedRemove(BootstrapGroup) \/
    (\E r \in Replicas, correlation \in Correlations:
        SendCorrelatedRequest(BootstrapGroup, r, correlation) \/
        DispatchMatchedResponse(BootstrapGroup, r, correlation) \/
        RejectMismatchedResponse(BootstrapGroup, r, correlation)) \/
    PublishUninitializedReplica(BootstrapGroup, "n2") \/
    FailReplicaPublication(BootstrapGroup, "n2") \/
    StartTickRound \/
    Stutter

IsolationNext ==
    BeginCreate("g1") \/
    PrepareSeed("g1") \/
    PublishSeed("g1") \/
    /\ phase["g1"] = "active"
       /\ (BeginCreate("g2") \/ PrepareSeed("g2") \/ PublishSeed("g2"))
    \/ /\ phase["g1"] = "active"
          /\ phase["g2"] = "active"
          /\ StartTickRound
    \/ /\ tickStatus["g1"] = "pending"
          /\ FinishTickFailure("g1")
    \/ /\ tickStatus["g1"] = "failure"
          /\ FinishTickSuccess("g2")

Next ==
    (\E g \in Groups:
        BeginCreate(g) \/ PrepareSeed(g) \/ PublishSeed(g) \/ AbortCreate(g) \/
        RejectDuplicateCreate(g) \/ RejectInvalidNamespace(g) \/
        DispatchRequest(g) \/ FinishRequest(g) \/ RejectUnknownRoute(g) \/
        RejectUnavailableRoute(g) \/ TimeoutPendingRpc(g) \/
        FinishTickSuccess(g) \/ FinishTickFailure(g) \/ BeginRemove(g) \/
        Shutdown(g) \/ FinishRemove(g) \/ RejectRepeatedRemove(g) \/
        MarkBootstrapReady(g) \/
        (\E r \in Replicas:
            PublishUninitializedReplica(g, r) \/ FailReplicaPublication(g, r) \/
            AddLearner(g, r) \/ FailLearnerAdd(g, r) \/
            CatchUpLearner(g, r) \/ FailLearnerCatchUp(g, r) \/
            PromoteLearner(g, r) \/ FailLearnerPromote(g, r) \/
            FailReplica(g, r) \/ ElectLeader(g, r) \/ ReplicateCommit(g, r) \/
            (\E correlation \in Correlations:
                SendCorrelatedRequest(g, r, correlation) \/
                DispatchMatchedResponse(g, r, correlation) \/
                RejectMismatchedResponse(g, r, correlation))) \/
        (\E r \in Replicas, value \in CommitValues:
            CommitProposal(g, r, value)))
    \/ StartTickRound
    \/ Stutter

TypeOK ==
    /\ MaxInFlight = 1
    /\ phase \in [Groups -> Phases]
    /\ storage \in [Groups -> StorageStates]
    /\ namespace \in [Groups -> Namespaces]
    /\ seedReady \in [Groups -> BOOLEAN]
    /\ transportRegistered \in [Groups -> BOOLEAN]
    /\ shutdownComplete \in [Groups -> BOOLEAN]
    /\ replicaLocalReady \in [Groups -> [Replicas -> BOOLEAN]]
    /\ replicaPublished \in [Groups -> [Replicas -> BOOLEAN]]
    /\ replicaRole \in [Groups -> [Replicas -> ReplicaRoles]]
    /\ replicaAvailable \in [Groups -> [Replicas -> BOOLEAN]]
    /\ promotionCertified \in [Groups -> [Replicas -> BOOLEAN]]
    /\ leaderClaim \in [Groups -> [Replicas -> BOOLEAN]]
    /\ term \in [Groups -> 0..2]
    /\ bootstrapReady \in [Groups -> BOOLEAN]
    /\ commitIndex \in [Groups -> 0..1]
    /\ committedValue \in [Groups -> CommitValues]
    /\ replicaCommit \in [Groups -> [Replicas -> 0..1]]
    /\ replicaValue \in [Groups -> [Replicas -> CommitValues]]
    /\ commitCertified \in [Groups -> BOOLEAN]
    /\ commitVoterCount \in [Groups -> 0..3]
    /\ commitAckCount \in [Groups -> 0..3]
    /\ routeInFlight \in [Groups -> 0..MaxInFlight]
    /\ tickInFlight \in [Groups -> 0..MaxInFlight]
    /\ tickMember \in [Groups -> BOOLEAN]
    /\ tickStatus \in [Groups -> TickStates]
    /\ groupVersion \in [Groups -> 0..2]
    /\ pendingCorrelation \in [Groups -> CorrelationTargets]
    /\ pendingTarget \in [Groups -> EventReplicas]
    /\ pendingStatus \in [Groups -> PendingStates]
    /\ lastEvent \in Events
    /\ lastEventGroup \in EventGroups
    /\ lastEventReplica \in EventReplicas
    /\ lastEventCorrelation \in CorrelationTargets
    /\ versionSnapshot \in [Groups -> 0..2]
    /\ pendingCorrelationSnapshot \in [Groups -> CorrelationTargets]
    /\ pendingTargetSnapshot \in [Groups -> EventReplicas]
    /\ phaseSnapshot \in Phases
    /\ storageSnapshot \in StorageStates
    /\ namespaceSnapshot \in Namespaces
    /\ roleSnapshot \in ReplicaRoles
    /\ publishedSnapshot \in BOOLEAN
    /\ commitIndexSnapshot \in 0..1
    /\ committedValueSnapshot \in CommitValues
    /\ replicaCommitSnapshot \in 0..1
    /\ replicaValueSnapshot \in CommitValues

PublishedOnlyAfterCommit ==
    \A g \in Groups:
        phase[g] = "active" =>
            /\ storage[g] = "committed"
            /\ namespace[g] = CanonicalNamespace(g)
            /\ seedReady[g]
            /\ transportRegistered[g]
            /\ replicaPublished[g][SeedReplica]
            /\ replicaRole[g][SeedReplica] = "voter"

StorageIsolation ==
    /\ \A g \in Groups:
        storage[g] # "none" => namespace[g] = CanonicalNamespace(g)
    /\ \A g1, g2 \in Groups:
        g1 # g2 /\ namespace[g1] # "none" /\ namespace[g2] # "none"
            => namespace[g1] # namespace[g2]

ReplicaPublicationSafe ==
    \A g \in Groups, r \in Replicas:
        /\ replicaPublished[g][r] =>
            /\ phase[g] \in {"active", "draining", "stopped"}
            /\ storage[g] = "committed"
            /\ replicaLocalReady[g][r]
        /\ replicaRole[g][r] # "absent" => replicaPublished[g][r]
        /\ replicaRole[g][r] = "uninitialized" =>
            /\ r \in RemoteReplicas
            /\ ~promotionCertified[g][r]
            /\ ~leaderClaim[g][r]

MembershipSafety ==
    \A g \in Groups:
        /\ \A r \in Replicas:
            /\ replicaRole[g][r] = "voter" => promotionCertified[g][r]
            /\ promotionCertified[g][r] => replicaRole[g][r] = "voter"
            /\ leaderClaim[g][r] =>
                /\ replicaRole[g][r] = "voter"
                /\ replicaPublished[g][r]
                /\ replicaAvailable[g][r]
        /\ bootstrapReady[g] =>
            /\ g = BootstrapGroup
            /\ phase[g] = "active"
            /\ Voters(g) = Replicas

SingleLeaderPerGroup ==
    \A g \in Groups: Cardinality(Leaders(g)) <= 1

CommitConsistency ==
    \A g \in Groups:
        /\ (commitIndex[g] = 0) = (committedValue[g] = "none")
        /\ commitIndex[g] = 1 => commitCertified[g]
        /\ \A r \in Replicas:
            /\ replicaCommit[g][r] <= commitIndex[g]
            /\ replicaCommit[g][r] = 0 => replicaValue[g][r] = "none"
            /\ replicaCommit[g][r] = 1 =>
                /\ replicaValue[g][r] = committedValue[g]
                /\ committedValue[g] # "none"

QuorumCommitOnly ==
    \A g \in Groups:
        commitCertified[g] =>
            /\ commitIndex[g] = 1
            /\ commitVoterCount[g] > 0
            /\ commitAckCount[g] <= commitVoterCount[g]
            /\ 2 * commitAckCount[g] > commitVoterCount[g]

LearnerPromotionSafe ==
    lastEvent = "learner_promoted" =>
        /\ lastEventGroup = BootstrapGroup
        /\ lastEventReplica \in RemoteReplicas
        /\ roleSnapshot = "caught_up"
        /\ replicaCommitSnapshot = commitIndexSnapshot
        /\ replicaValueSnapshot = committedValueSnapshot
        /\ replicaRole[lastEventGroup][lastEventReplica] = "voter"
        /\ promotionCertified[lastEventGroup][lastEventReplica]

ResponseCorrelationSafe ==
    /\ lastEvent = "response_delivered" =>
        /\ lastEventGroup \in Groups
        /\ lastEventReplica \in Replicas
        /\ lastEventCorrelation \in Correlations
        /\ pendingCorrelationSnapshot[lastEventGroup] = lastEventCorrelation
        /\ pendingTargetSnapshot[lastEventGroup] = lastEventReplica
        /\ pendingCorrelation[lastEventGroup] = "none"
        /\ pendingTarget[lastEventGroup] = "none"
        /\ pendingStatus[lastEventGroup] = "delivered"
        /\ versionSnapshot = groupVersion
    /\ lastEvent = "response_rejected" =>
        /\ pendingCorrelationSnapshot = pendingCorrelation
        /\ pendingTargetSnapshot = pendingTarget
        /\ versionSnapshot = groupVersion

RejectedRouteHasNoMutation ==
    lastEvent \in RejectedRouteEvents =>
        /\ versionSnapshot = groupVersion
        /\ pendingCorrelationSnapshot = pendingCorrelation
        /\ pendingTargetSnapshot = pendingTarget

FailureLeavesMembershipAtomic ==
    lastEvent \in AtomicMembershipFailureEvents =>
        /\ lastEventGroup = BootstrapGroup
        /\ lastEventReplica \in RemoteReplicas
        /\ phaseSnapshot = phase[lastEventGroup]
        /\ storageSnapshot = storage[lastEventGroup]
        /\ namespaceSnapshot = namespace[lastEventGroup]
        /\ roleSnapshot = replicaRole[lastEventGroup][lastEventReplica]
        /\ publishedSnapshot = replicaPublished[lastEventGroup][lastEventReplica]
        /\ commitIndexSnapshot = commitIndex[lastEventGroup]
        /\ committedValueSnapshot = committedValue[lastEventGroup]
        /\ replicaCommitSnapshot = replicaCommit[lastEventGroup][lastEventReplica]
        /\ replicaValueSnapshot = replicaValue[lastEventGroup][lastEventReplica]
        /\ versionSnapshot[lastEventGroup] = groupVersion[lastEventGroup]

RemovalDrainsBeforeShutdown ==
    \A g \in Groups:
        shutdownComplete[g] =>
            /\ routeInFlight[g] = 0
            /\ tickInFlight[g] = 0
            /\ pendingCorrelation[g] = "none"

InactiveGroupsRejectNewWork ==
    \A g \in Groups:
        /\ routeInFlight[g] > 0 => phase[g] \in {"active", "draining"}
        /\ tickInFlight[g] > 0 => phase[g] \in {"active", "draining"}
        /\ pendingCorrelation[g] # "none" => phase[g] \in {"active", "draining"}

RollbackLeavesNoPartialGroup ==
    \A g \in Groups:
        phase[g] = "absent" =>
            /\ storage[g] = "none"
            /\ namespace[g] = "none"
            /\ ~seedReady[g]
            /\ ~transportRegistered[g]
            /\ ~shutdownComplete[g]
            /\ \A r \in Replicas:
                /\ ~replicaLocalReady[g][r]
                /\ ~replicaPublished[g][r]
                /\ replicaRole[g][r] = "absent"
                /\ ~replicaAvailable[g][r]
                /\ ~promotionCertified[g][r]
                /\ ~leaderClaim[g][r]
                /\ replicaCommit[g][r] = 0
                /\ replicaValue[g][r] = "none"
            /\ routeInFlight[g] = 0
            /\ tickInFlight[g] = 0
            /\ pendingCorrelation[g] = "none"

RejectedLifecycleIsIdempotent ==
    lastEvent \in RejectedLifecycleEvents =>
        /\ lastEventGroup \in Groups
        /\ phaseSnapshot = phase[lastEventGroup]
        /\ storageSnapshot = storage[lastEventGroup]
        /\ namespaceSnapshot = namespace[lastEventGroup]
        /\ commitIndexSnapshot = commitIndex[lastEventGroup]
        /\ versionSnapshot[lastEventGroup] = groupVersion[lastEventGroup]

TickFailureDoesNotCancelPeers ==
    \A failed \in Groups:
        tickStatus[failed] = "failure" =>
            \A peer \in Groups:
                tickMember[peer] /\ peer # failed => tickStatus[peer] # "cancelled"

=============================================================================
