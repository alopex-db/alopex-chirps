--------------------------- MODULE model ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANT
    \* @type: Int;
    MaxInFlight

Groups == {"g1", "g2"}
Phases == {"absent", "creating", "active", "draining", "stopped"}
StorageStates == {"none", "temporary", "committed"}
Namespaces == {"none", "groups/0000000000000001", "groups/0000000000000002"}
RouteKinds == {"none", "accepted", "applied", "unknown", "unavailable"}
RouteTargets == Groups \cup {"none"}
TickStates == {"idle", "pending", "success", "failure", "cancelled"}
LifecycleResults == {
    "none", "started", "created", "aborted", "duplicate",
    "invalid_namespace", "remove_started", "removed", "not_found",
    "already_removing", "unavailable"
}
RejectedLifecycleResults == {
    "duplicate", "invalid_namespace", "not_found", "already_removing", "unavailable"
}

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
    nodeReady,
    \* @type: Str -> Bool;
    transportRegistered,
    \* @type: Str -> Int;
    routeInFlight,
    \* @type: Str -> Int;
    tickInFlight,
    \* @type: Str -> Int;
    groupVersion,
    \* @type: Str -> Bool;
    tickMember,
    \* @type: Str -> Str;
    tickStatus,
    \* @type: Str -> Bool;
    shutdownComplete,
    \* @type: Str;
    lastRouteKind,
    \* @type: Str;
    lastRouteTarget,
    \* @type: Str -> Int;
    routeVersionSnapshot,
    \* @type: Str;
    lastLifecycleResult,
    \* @type: Str -> Str;
    lifecyclePhaseSnapshot,
    \* @type: Str -> Str;
    lifecycleStorageSnapshot,
    \* @type: Str -> Str;
    lifecycleNamespaceSnapshot,
    \* @type: Str -> Int;
    lifecycleVersionSnapshot

vars == << phase,
           storage,
           namespace,
           nodeReady,
           transportRegistered,
           routeInFlight,
           tickInFlight,
           groupVersion,
           tickMember,
           tickStatus,
           shutdownComplete,
           lastRouteKind,
           lastRouteTarget,
           routeVersionSnapshot,
           lastLifecycleResult,
           lifecyclePhaseSnapshot,
           lifecycleStorageSnapshot,
           lifecycleNamespaceSnapshot,
           lifecycleVersionSnapshot >>

Init ==
    /\ phase = [g \in Groups |-> "absent"]
    /\ storage = [g \in Groups |-> "none"]
    /\ namespace = [g \in Groups |-> "none"]
    /\ nodeReady = [g \in Groups |-> FALSE]
    /\ transportRegistered = [g \in Groups |-> FALSE]
    /\ routeInFlight = [g \in Groups |-> 0]
    /\ tickInFlight = [g \in Groups |-> 0]
    /\ groupVersion = [g \in Groups |-> 0]
    /\ tickMember = [g \in Groups |-> FALSE]
    /\ tickStatus = [g \in Groups |-> "idle"]
    /\ shutdownComplete = [g \in Groups |-> FALSE]
    /\ lastRouteKind = "none"
    /\ lastRouteTarget = "none"
    /\ routeVersionSnapshot = [g \in Groups |-> 0]
    /\ lastLifecycleResult = "none"
    /\ lifecyclePhaseSnapshot = phase
    /\ lifecycleStorageSnapshot = storage
    /\ lifecycleNamespaceSnapshot = namespace
    /\ lifecycleVersionSnapshot = groupVersion

RememberLifecycle(result) ==
    /\ lastLifecycleResult' = result
    /\ lifecyclePhaseSnapshot' = phase'
    /\ lifecycleStorageSnapshot' = storage'
    /\ lifecycleNamespaceSnapshot' = namespace'
    /\ lifecycleVersionSnapshot' = groupVersion'

BeginCreate(g) ==
    /\ phase[g] = "absent"
    /\ phase' = [phase EXCEPT ![g] = "creating"]
    /\ storage' = [storage EXCEPT ![g] = "temporary"]
    /\ namespace' = [namespace EXCEPT ![g] = CanonicalNamespace(g)]
    /\ shutdownComplete' = [shutdownComplete EXCEPT ![g] = FALSE]
    /\ UNCHANGED << nodeReady, transportRegistered, routeInFlight,
                    tickInFlight, groupVersion, tickMember, tickStatus,
                    lastRouteKind, lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("started")

PrepareGroup(g) ==
    /\ phase[g] = "creating"
    /\ storage[g] = "temporary"
    /\ ~nodeReady[g]
    /\ nodeReady' = [nodeReady EXCEPT ![g] = TRUE]
    /\ transportRegistered' = [transportRegistered EXCEPT ![g] = TRUE]
    /\ UNCHANGED << phase, storage, namespace, routeInFlight,
                    tickInFlight, groupVersion, tickMember, tickStatus,
                    shutdownComplete, lastRouteKind, lastRouteTarget,
                    routeVersionSnapshot >>
    /\ RememberLifecycle("started")

PublishGroup(g) ==
    /\ phase[g] = "creating"
    /\ storage[g] = "temporary"
    /\ namespace[g] = CanonicalNamespace(g)
    /\ nodeReady[g]
    /\ transportRegistered[g]
    /\ phase' = [phase EXCEPT ![g] = "active"]
    /\ storage' = [storage EXCEPT ![g] = "committed"]
    /\ UNCHANGED << namespace, nodeReady, transportRegistered,
                    routeInFlight, tickInFlight, groupVersion, tickMember,
                    tickStatus, shutdownComplete, lastRouteKind,
                    lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("created")

AbortCreate(g) ==
    /\ phase[g] = "creating"
    /\ phase' = [phase EXCEPT ![g] = "absent"]
    /\ storage' = [storage EXCEPT ![g] = "none"]
    /\ namespace' = [namespace EXCEPT ![g] = "none"]
    /\ nodeReady' = [nodeReady EXCEPT ![g] = FALSE]
    /\ transportRegistered' = [transportRegistered EXCEPT ![g] = FALSE]
    /\ UNCHANGED << routeInFlight, tickInFlight, groupVersion,
                    tickMember, tickStatus, shutdownComplete, lastRouteKind,
                    lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("aborted")

RejectDuplicateCreate(g) ==
    /\ phase[g] # "absent"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, tickInFlight,
                    groupVersion, tickMember, tickStatus, shutdownComplete,
                    lastRouteKind, lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("duplicate")

RejectInvalidNamespace(g) ==
    /\ phase[g] = "absent"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, tickInFlight,
                    groupVersion, tickMember, tickStatus, shutdownComplete,
                    lastRouteKind, lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("invalid_namespace")

BeginRoute(g) ==
    /\ phase[g] = "active"
    /\ routeInFlight[g] < MaxInFlight
    /\ routeInFlight' = [routeInFlight EXCEPT ![g] = @ + 1]
    /\ lastRouteKind' = "accepted"
    /\ lastRouteTarget' = g
    /\ lastLifecycleResult' = "none"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, tickInFlight, groupVersion,
                    tickMember, tickStatus, shutdownComplete,
                    routeVersionSnapshot,
                    lifecyclePhaseSnapshot, lifecycleStorageSnapshot,
                    lifecycleNamespaceSnapshot, lifecycleVersionSnapshot >>

FinishRoute(g) ==
    /\ routeInFlight[g] > 0
    /\ phase[g] \in {"active", "draining"}
    /\ groupVersion[g] < 2
    /\ routeInFlight' = [routeInFlight EXCEPT ![g] = @ - 1]
    /\ groupVersion' = [groupVersion EXCEPT ![g] = @ + 1]
    /\ lastRouteKind' = "applied"
    /\ lastRouteTarget' = g
    /\ lastLifecycleResult' = "none"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, tickInFlight, tickMember, tickStatus,
                    shutdownComplete, routeVersionSnapshot,
                    lifecyclePhaseSnapshot,
                    lifecycleStorageSnapshot, lifecycleNamespaceSnapshot,
                    lifecycleVersionSnapshot >>

RejectUnknownRoute(g) ==
    /\ phase[g] = "absent"
    /\ lastRouteKind' = "unknown"
    /\ lastRouteTarget' = "none"
    /\ routeVersionSnapshot' = groupVersion
    /\ lastLifecycleResult' = "none"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, tickInFlight,
                    groupVersion, tickMember, tickStatus, shutdownComplete,
                    lifecyclePhaseSnapshot,
                    lifecycleStorageSnapshot, lifecycleNamespaceSnapshot,
                    lifecycleVersionSnapshot >>

RejectUnavailableRoute(g) ==
    /\ phase[g] \in {"creating", "draining", "stopped"}
    /\ lastRouteKind' = "unavailable"
    /\ lastRouteTarget' = g
    /\ routeVersionSnapshot' = groupVersion
    /\ lastLifecycleResult' = "none"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, tickInFlight,
                    groupVersion, tickMember, tickStatus, shutdownComplete,
                    lifecyclePhaseSnapshot,
                    lifecycleStorageSnapshot, lifecycleNamespaceSnapshot,
                    lifecycleVersionSnapshot >>

StartTickRound ==
    /\ \A g \in Groups: tickInFlight[g] = 0
    /\ \E g \in Groups: phase[g] = "active"
    /\ tickMember' = [g \in Groups |-> phase[g] = "active"]
    /\ tickStatus' = [g \in Groups |-> IF phase[g] = "active" THEN "pending" ELSE "idle"]
    /\ tickInFlight' = [g \in Groups |-> IF phase[g] = "active" THEN 1 ELSE 0]
    /\ lastRouteKind' = "none"
    /\ lastRouteTarget' = "none"
    /\ lastLifecycleResult' = "none"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, groupVersion,
                    shutdownComplete, routeVersionSnapshot,
                    lifecyclePhaseSnapshot,
                    lifecycleStorageSnapshot, lifecycleNamespaceSnapshot,
                    lifecycleVersionSnapshot >>

FinishTickSuccess(g) ==
    /\ tickInFlight[g] = 1
    /\ tickStatus[g] = "pending"
    /\ groupVersion[g] < 2
    /\ tickInFlight' = [tickInFlight EXCEPT ![g] = 0]
    /\ tickStatus' = [tickStatus EXCEPT ![g] = "success"]
    /\ groupVersion' = [groupVersion EXCEPT ![g] = @ + 1]
    /\ lastRouteKind' = "none"
    /\ lastRouteTarget' = "none"
    /\ lastLifecycleResult' = "none"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, tickMember,
                    shutdownComplete, routeVersionSnapshot,
                    lifecyclePhaseSnapshot, lifecycleStorageSnapshot,
                    lifecycleNamespaceSnapshot, lifecycleVersionSnapshot >>

FinishTickFailure(g) ==
    /\ tickInFlight[g] = 1
    /\ tickStatus[g] = "pending"
    /\ tickInFlight' = [tickInFlight EXCEPT ![g] = 0]
    /\ tickStatus' = [tickStatus EXCEPT ![g] = "failure"]
    /\ lastRouteKind' = "none"
    /\ lastRouteTarget' = "none"
    /\ lastLifecycleResult' = "none"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, groupVersion,
                    tickMember, shutdownComplete, routeVersionSnapshot,
                    lifecyclePhaseSnapshot,
                    lifecycleStorageSnapshot, lifecycleNamespaceSnapshot,
                    lifecycleVersionSnapshot >>

BeginRemove(g) ==
    /\ phase[g] = "active"
    /\ phase' = [phase EXCEPT ![g] = "draining"]
    /\ UNCHANGED << storage, namespace, nodeReady, transportRegistered,
                    routeInFlight, tickInFlight, groupVersion, tickMember,
                    tickStatus, shutdownComplete, lastRouteKind,
                    lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("remove_started")

Shutdown(g) ==
    /\ phase[g] = "draining"
    /\ routeInFlight[g] = 0
    /\ tickInFlight[g] = 0
    /\ phase' = [phase EXCEPT ![g] = "stopped"]
    /\ transportRegistered' = [transportRegistered EXCEPT ![g] = FALSE]
    /\ shutdownComplete' = [shutdownComplete EXCEPT ![g] = TRUE]
    /\ UNCHANGED << storage, namespace, nodeReady, routeInFlight,
                    tickInFlight, groupVersion, tickMember, tickStatus,
                    lastRouteKind, lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("remove_started")

FinishRemove(g) ==
    /\ phase[g] = "stopped"
    /\ shutdownComplete[g]
    /\ phase' = [phase EXCEPT ![g] = "absent"]
    /\ storage' = [storage EXCEPT ![g] = "none"]
    /\ namespace' = [namespace EXCEPT ![g] = "none"]
    /\ nodeReady' = [nodeReady EXCEPT ![g] = FALSE]
    /\ shutdownComplete' = [shutdownComplete EXCEPT ![g] = FALSE]
    /\ UNCHANGED << transportRegistered, routeInFlight, tickInFlight,
                    groupVersion, tickMember, tickStatus, lastRouteKind,
                    lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle("removed")

RejectRepeatedRemove(g) ==
    /\ phase[g] # "active"
    /\ UNCHANGED << phase, storage, namespace, nodeReady,
                    transportRegistered, routeInFlight, tickInFlight,
                    groupVersion, tickMember, tickStatus, shutdownComplete,
                    lastRouteKind, lastRouteTarget, routeVersionSnapshot >>
    /\ RememberLifecycle(
        IF phase[g] = "absent" THEN "not_found"
        ELSE IF phase[g] \in {"draining", "stopped"} THEN "already_removing"
        ELSE "unavailable")

Stutter == UNCHANGED vars

Next ==
    (\E g \in Groups:
        BeginCreate(g) \/ PrepareGroup(g) \/ PublishGroup(g) \/ AbortCreate(g) \/
        RejectDuplicateCreate(g) \/ RejectInvalidNamespace(g) \/ BeginRoute(g) \/
        FinishRoute(g) \/ RejectUnknownRoute(g) \/ RejectUnavailableRoute(g) \/
        FinishTickSuccess(g) \/ FinishTickFailure(g) \/ BeginRemove(g) \/
        Shutdown(g) \/ FinishRemove(g) \/ RejectRepeatedRemove(g))
    \/ StartTickRound
    \/ Stutter

TypeOK ==
    /\ MaxInFlight = 1
    /\ phase \in [Groups -> Phases]
    /\ storage \in [Groups -> StorageStates]
    /\ namespace \in [Groups -> Namespaces]
    /\ nodeReady \in [Groups -> BOOLEAN]
    /\ transportRegistered \in [Groups -> BOOLEAN]
    /\ routeInFlight \in [Groups -> 0..MaxInFlight]
    /\ tickInFlight \in [Groups -> 0..MaxInFlight]
    /\ groupVersion \in [Groups -> 0..2]
    /\ tickMember \in [Groups -> BOOLEAN]
    /\ tickStatus \in [Groups -> TickStates]
    /\ shutdownComplete \in [Groups -> BOOLEAN]
    /\ lastRouteKind \in RouteKinds
    /\ lastRouteTarget \in RouteTargets
    /\ routeVersionSnapshot \in [Groups -> 0..2]
    /\ lastLifecycleResult \in LifecycleResults
    /\ lifecyclePhaseSnapshot \in [Groups -> Phases]
    /\ lifecycleStorageSnapshot \in [Groups -> StorageStates]
    /\ lifecycleNamespaceSnapshot \in [Groups -> Namespaces]
    /\ lifecycleVersionSnapshot \in [Groups -> 0..2]

PublishedOnlyAfterCommit ==
    \A g \in Groups:
        phase[g] = "active" =>
            /\ storage[g] = "committed"
            /\ namespace[g] = CanonicalNamespace(g)
            /\ nodeReady[g]
            /\ transportRegistered[g]

StorageIsolation ==
    /\ \A g \in Groups:
        storage[g] # "none" => namespace[g] = CanonicalNamespace(g)
    /\ \A g1, g2 \in Groups:
        g1 # g2 /\ namespace[g1] # "none" /\ namespace[g2] # "none"
            => namespace[g1] # namespace[g2]

RejectedRouteHasNoMutation ==
    /\ lastRouteKind \in {"unknown", "unavailable"} => routeVersionSnapshot = groupVersion
    /\ lastRouteKind = "unknown" => lastRouteTarget = "none"

RemovalDrainsBeforeShutdown ==
    \A g \in Groups:
        shutdownComplete[g] => /\ routeInFlight[g] = 0
                               /\ tickInFlight[g] = 0

InactiveGroupsRejectNewWork ==
    \A g \in Groups:
        routeInFlight[g] > 0 => phase[g] \in {"active", "draining"}

RollbackLeavesNoPartialGroup ==
    \A g \in Groups:
        phase[g] = "absent" =>
            /\ storage[g] = "none"
            /\ namespace[g] = "none"
            /\ ~nodeReady[g]
            /\ ~transportRegistered[g]
            /\ routeInFlight[g] = 0
            /\ tickInFlight[g] = 0

RejectedLifecycleIsIdempotent ==
    lastLifecycleResult \in RejectedLifecycleResults =>
        /\ lifecyclePhaseSnapshot = phase
        /\ lifecycleStorageSnapshot = storage
        /\ lifecycleNamespaceSnapshot = namespace
        /\ lifecycleVersionSnapshot = groupVersion

TickFailureDoesNotCancelPeers ==
    \A failed \in Groups:
        tickStatus[failed] = "failure" =>
            \A peer \in Groups:
                tickMember[peer] /\ peer # failed => tickStatus[peer] # "cancelled"

=============================================================================
