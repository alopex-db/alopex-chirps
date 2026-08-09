----------------------------- MODULE model -----------------------------
EXTENDS Naturals

CONSTANTS
    \* @type: Int;
    BatchLimit,
    \* @type: Int;
    LeaseLength

Nodes == {"n1", "n2", "n3"}
Followers == Nodes \ {"n1"}
Actors == Nodes \cup {"client", "none"}
Phases == {"serving", "handoff_wait"}
Events == {
    "none", "range_reserved", "server_issued", "not_leader",
    "physical_advanced", "physical_rollback", "handoff_started",
    "early_handoff_rejected", "time_advanced", "handoff_completed",
    "client_retry", "client_batch", "client_issued"
}
Statuses == {"idle", "ok", "not_leader", "retryable", "lease_wait"}
MaxTimestamp == 20

Max(a, b) == IF a >= b THEN a ELSE b
PhysicalFloor(p) == p * 4

VARIABLES
    \* @type: Str;
    raftGroup,
    \* @type: Bool;
    raftReady,
    \* @type: Str;
    leader,
    \* @type: Str;
    leaseOwner,
    \* @type: Int;
    leaseExpiry,
    \* @type: Int;
    handoffNotBefore,
    \* @type: Str;
    phase,
    \* @type: Int;
    now,
    \* @type: Int;
    physicalNow,
    \* @type: Int;
    committedEnd,
    \* @type: Str -> Int;
    rangeStart,
    \* @type: Str -> Int;
    rangeEnd,
    \* @type: Str -> Int;
    rangeNext,
    \* @type: Int;
    clientStart,
    \* @type: Int;
    clientEnd,
    \* @type: Int;
    clientNext,
    \* @type: Str;
    clientHint,
    \* @type: Int;
    retryCount,
    \* @type: Int;
    issuedHigh,
    \* @type: Int;
    issuedCount,
    \* @type: Str;
    lastEvent,
    \* @type: Str;
    lastActor,
    \* @type: Str;
    lastStatus,
    \* @type: Int;
    issuedSnapshot,
    \* @type: Int;
    committedSnapshot,
    \* @type: Int;
    physicalSnapshot,
    \* @type: Str;
    leaderSnapshot,
    \* @type: Str;
    leaseOwnerSnapshot,
    \* @type: Int;
    leaseExpirySnapshot,
    \* @type: Int;
    nowSnapshot,
    \* @type: Int;
    clientNextSnapshot,
    \* @type: Int;
    clientEndSnapshot

Core == << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
           handoffNotBefore, phase, now, physicalNow, committedEnd,
           rangeStart, rangeEnd, rangeNext, clientStart, clientEnd,
           clientNext, clientHint, retryCount, issuedHigh, issuedCount >>
Observation == << lastEvent, lastActor, lastStatus, issuedSnapshot,
                 committedSnapshot, physicalSnapshot, leaderSnapshot,
                 leaseOwnerSnapshot, leaseExpirySnapshot, nowSnapshot,
                 clientNextSnapshot, clientEndSnapshot >>
vars == << Core, Observation >>

ServerRangeAvailable(n) ==
    rangeStart[n] > 0 /\ rangeNext[n] <= rangeEnd[n]

AllServerRangesExhausted ==
    \A n \in Nodes: ~ServerRangeAvailable(n)

ClientRangeAvailable ==
    clientStart > 0 /\ clientNext <= clientEnd

LeaseValid(n) ==
    phase = "serving" /\ leader = n /\ leaseOwner = n /\ now < leaseExpiry

Init ==
    /\ BatchLimit = 2
    /\ LeaseLength = 2
    /\ raftGroup = "tso"
    /\ raftReady = TRUE
    /\ leader = "n1"
    /\ leaseOwner = "n1"
    /\ leaseExpiry = 2
    /\ handoffNotBefore = 0
    /\ phase = "serving"
    /\ now = 0
    /\ physicalNow = 2
    /\ committedEnd = 0
    /\ rangeStart = [n \in Nodes |-> 0]
    /\ rangeEnd = [n \in Nodes |-> 0]
    /\ rangeNext = [n \in Nodes |-> 0]
    /\ clientStart = 0
    /\ clientEnd = 0
    /\ clientNext = 0
    /\ clientHint = "n2"
    /\ retryCount = 0
    /\ issuedHigh = 0
    /\ issuedCount = 0
    /\ lastEvent = "none"
    /\ lastActor = "none"
    /\ lastStatus = "idle"
    /\ issuedSnapshot = 0
    /\ committedSnapshot = 0
    /\ physicalSnapshot = physicalNow
    /\ leaderSnapshot = leader
    /\ leaseOwnerSnapshot = leaseOwner
    /\ leaseExpirySnapshot = leaseExpiry
    /\ nowSnapshot = now
    /\ clientNextSnapshot = 0
    /\ clientEndSnapshot = 0

Observe(event, actor, status) ==
    /\ lastEvent' = event
    /\ lastActor' = actor
    /\ lastStatus' = status
    /\ issuedSnapshot' = issuedHigh
    /\ committedSnapshot' = committedEnd
    /\ physicalSnapshot' = physicalNow
    /\ leaderSnapshot' = leader
    /\ leaseOwnerSnapshot' = leaseOwner
    /\ leaseExpirySnapshot' = leaseExpiry
    /\ nowSnapshot' = now
    /\ clientNextSnapshot' = clientNext
    /\ clientEndSnapshot' = clientEnd

ReserveServerRange(n, count) ==
    LET base == Max(committedEnd, PhysicalFloor(physicalNow)) IN
    /\ n \in Nodes
    /\ count \in 1..BatchLimit
    /\ LeaseValid(n)
    /\ ~ServerRangeAvailable(n)
    /\ ~ClientRangeAvailable
    /\ base + count <= MaxTimestamp
    /\ committedEnd' = base + count
    /\ rangeStart' = [rangeStart EXCEPT ![n] = base + 1]
    /\ rangeEnd' = [rangeEnd EXCEPT ![n] = base + count]
    /\ rangeNext' = [rangeNext EXCEPT ![n] = base + 1]
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, physicalNow,
                    clientStart, clientEnd, clientNext, clientHint,
                    retryCount, issuedHigh, issuedCount >>
    /\ Observe("range_reserved", n, "ok")

IssueServerTimestamp(n) ==
    /\ n \in Nodes
    /\ LeaseValid(n)
    /\ ServerRangeAvailable(n)
    /\ rangeNext[n] > issuedHigh
    /\ issuedHigh' = rangeNext[n]
    /\ issuedCount' = issuedCount + 1
    /\ rangeNext' = [rangeNext EXCEPT ![n] = @ + 1]
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, physicalNow, committedEnd,
                    rangeStart, rangeEnd, clientStart, clientEnd, clientNext,
                    clientHint, retryCount >>
    /\ Observe("server_issued", n, "ok")

RejectFollower(n) ==
    /\ n \in Followers
    /\ n # leader
    /\ UNCHANGED Core
    /\ Observe("not_leader", n, "not_leader")

AdvancePhysical ==
    /\ physicalNow < 3
    /\ physicalNow' = physicalNow + 1
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, committedEnd, rangeStart,
                    rangeEnd, rangeNext, clientStart, clientEnd, clientNext,
                    clientHint, retryCount, issuedHigh, issuedCount >>
    /\ Observe("physical_advanced", "none", "ok")

RollbackPhysical ==
    /\ physicalNow > 0
    /\ physicalNow' = physicalNow - 1
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, committedEnd, rangeStart,
                    rangeEnd, rangeNext, clientStart, clientEnd, clientNext,
                    clientHint, retryCount, issuedHigh, issuedCount >>
    /\ Observe("physical_rollback", "none", "ok")

BeginHandoff(newLeader) ==
    /\ newLeader \in Nodes
    /\ newLeader # leader
    /\ phase = "serving"
    /\ leader' = newLeader
    /\ phase' = "handoff_wait"
    /\ handoffNotBefore' = leaseExpiry
    /\ rangeStart' = [n \in Nodes |-> 0]
    /\ rangeEnd' = [n \in Nodes |-> 0]
    /\ rangeNext' = [n \in Nodes |-> 0]
    /\ clientStart' = 0
    /\ clientEnd' = 0
    /\ clientNext' = 0
    /\ UNCHANGED << raftGroup, raftReady, leaseOwner, leaseExpiry, now,
                    physicalNow, committedEnd, clientHint, retryCount,
                    issuedHigh, issuedCount >>
    /\ Observe("handoff_started", newLeader, "lease_wait")

RejectEarlyHandoff ==
    /\ phase = "handoff_wait"
    /\ now < handoffNotBefore
    /\ UNCHANGED Core
    /\ Observe("early_handoff_rejected", leader, "lease_wait")

AdvanceTime ==
    /\ now < 4
    /\ now' = now + 1
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, physicalNow, committedEnd,
                    rangeStart, rangeEnd, rangeNext, clientStart, clientEnd,
                    clientNext, clientHint, retryCount, issuedHigh, issuedCount >>
    /\ Observe("time_advanced", "none", "ok")

CompleteHandoff ==
    /\ phase = "handoff_wait"
    /\ now >= handoffNotBefore
    /\ now + LeaseLength <= 6
    /\ leaseOwner' = leader
    /\ leaseExpiry' = now + LeaseLength
    /\ handoffNotBefore' = 0
    /\ phase' = "serving"
    /\ clientHint' = leader
    /\ UNCHANGED << raftGroup, raftReady, leader, now, physicalNow,
                    committedEnd, rangeStart, rangeEnd, rangeNext,
                    clientStart, clientEnd, clientNext, retryCount,
                    issuedHigh, issuedCount >>
    /\ Observe("handoff_completed", leader, "ok")

RefreshClientLeader ==
    /\ clientHint # leader
    /\ clientHint' = leader
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, physicalNow, committedEnd,
                    rangeStart, rangeEnd, rangeNext, clientStart, clientEnd,
                    clientNext, retryCount, issuedHigh, issuedCount >>
    /\ Observe("not_leader", "client", "not_leader")

ClientNetworkFailure ==
    /\ retryCount < 2
    /\ retryCount' = retryCount + 1
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, physicalNow, committedEnd,
                    rangeStart, rangeEnd, rangeNext, clientStart, clientEnd,
                    clientNext, clientHint, issuedHigh, issuedCount >>
    /\ Observe("client_retry", "client", "retryable")

ClientFetchBatch(count) ==
    LET base == Max(committedEnd, PhysicalFloor(physicalNow)) IN
    /\ count \in 1..BatchLimit
    /\ clientHint = leader
    /\ LeaseValid(leader)
    /\ ~ClientRangeAvailable
    /\ AllServerRangesExhausted
    /\ base + count <= MaxTimestamp
    /\ committedEnd' = base + count
    /\ clientStart' = base + 1
    /\ clientEnd' = base + count
    /\ clientNext' = base + 1
    /\ retryCount' = 0
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, physicalNow, rangeStart,
                    rangeEnd, rangeNext, clientHint, issuedHigh, issuedCount >>
    /\ Observe("client_batch", "client", "ok")

ClientIssue ==
    /\ ClientRangeAvailable
    /\ clientHint = leader
    /\ LeaseValid(leader)
    /\ clientNext > issuedHigh
    /\ issuedHigh' = clientNext
    /\ issuedCount' = issuedCount + 1
    /\ clientNext' = clientNext + 1
    /\ UNCHANGED << raftGroup, raftReady, leader, leaseOwner, leaseExpiry,
                    handoffNotBefore, phase, now, physicalNow, committedEnd,
                    rangeStart, rangeEnd, rangeNext, clientStart, clientEnd,
                    clientHint, retryCount >>
    /\ Observe("client_issued", "client", "ok")

Stutter == UNCHANGED vars

AllocationNext ==
    /\ lastEvent = "none"
       /\ ReserveServerRange("n1", 2)
    \/ /\ lastEvent = "range_reserved"
          /\ IssueServerTimestamp(leader)
    \/ /\ lastEvent = "server_issued"
          /\ issuedCount = 1
          /\ RollbackPhysical
    \/ /\ lastEvent = "physical_rollback"
          /\ IssueServerTimestamp(leader)
    \/ /\ lastEvent = "server_issued"
          /\ issuedCount = 2
          /\ ReserveServerRange(leader, 1)
    \/ /\ lastEvent = "range_reserved"
          /\ issuedCount = 2
          /\ IssueServerTimestamp(leader)

FollowerNext ==
    /\ lastEvent = "none"
       /\ RejectFollower("n2")
    \/ /\ lastEvent = "not_leader"
          /\ ReserveServerRange("n1", 1)
    \/ /\ lastEvent = "range_reserved"
          /\ IssueServerTimestamp("n1")

HandoffNext ==
    /\ lastEvent = "none"
       /\ ReserveServerRange("n1", 2)
    \/ /\ lastEvent = "range_reserved"
          /\ IssueServerTimestamp(leader)
    \/ /\ lastEvent = "server_issued"
          /\ BeginHandoff("n2")
    \/ /\ lastEvent = "handoff_started"
          /\ RejectEarlyHandoff
    \/ /\ lastEvent \in {"early_handoff_rejected", "time_advanced"}
          /\ now < handoffNotBefore
          /\ AdvanceTime
    \/ /\ lastEvent = "time_advanced"
          /\ now >= handoffNotBefore
          /\ CompleteHandoff
    \/ /\ lastEvent = "handoff_completed"
          /\ ReserveServerRange("n2", 1)
    \/ /\ lastEvent = "range_reserved"
          /\ leader = "n2"
          /\ IssueServerTimestamp("n2")

ClientNext ==
    /\ lastEvent = "none"
       /\ RefreshClientLeader
    \/ /\ lastEvent = "not_leader"
          /\ ClientNetworkFailure
    \/ /\ lastEvent = "client_retry"
          /\ ClientFetchBatch(2)
    \/ /\ lastEvent = "client_batch"
          /\ ClientIssue
    \/ /\ lastEvent = "client_issued"
          /\ issuedCount = 1
          /\ ClientIssue

Next ==
    (\E n \in Nodes, count \in 1..BatchLimit:
        ReserveServerRange(n, count))
    \/ (\E n \in Nodes: IssueServerTimestamp(n) \/ RejectFollower(n))
    \/ AdvancePhysical \/ RollbackPhysical
    \/ (\E n \in Nodes: BeginHandoff(n))
    \/ RejectEarlyHandoff \/ AdvanceTime \/ CompleteHandoff
    \/ RefreshClientLeader \/ ClientNetworkFailure
    \/ (\E count \in 1..BatchLimit: ClientFetchBatch(count))
    \/ ClientIssue \/ Stutter

TypeOK ==
    /\ BatchLimit = 2
    /\ LeaseLength = 2
    /\ raftGroup = "tso"
    /\ raftReady \in BOOLEAN
    /\ leader \in Nodes
    /\ leaseOwner \in Nodes
    /\ leaseExpiry \in 0..6
    /\ handoffNotBefore \in 0..6
    /\ phase \in Phases
    /\ now \in 0..4
    /\ physicalNow \in 0..3
    /\ committedEnd \in 0..MaxTimestamp
    /\ rangeStart \in [Nodes -> 0..MaxTimestamp]
    /\ rangeEnd \in [Nodes -> 0..MaxTimestamp]
    /\ rangeNext \in [Nodes -> 0..(MaxTimestamp + 1)]
    /\ clientStart \in 0..MaxTimestamp
    /\ clientEnd \in 0..MaxTimestamp
    /\ clientNext \in 0..(MaxTimestamp + 1)
    /\ clientHint \in Nodes
    /\ retryCount \in 0..2
    /\ issuedHigh \in 0..MaxTimestamp
    /\ issuedCount \in 0..6
    /\ lastEvent \in Events
    /\ lastActor \in Actors
    /\ lastStatus \in Statuses
    /\ issuedSnapshot \in 0..MaxTimestamp
    /\ committedSnapshot \in 0..MaxTimestamp
    /\ physicalSnapshot \in 0..3
    /\ leaderSnapshot \in Nodes
    /\ leaseOwnerSnapshot \in Nodes
    /\ leaseExpirySnapshot \in 0..6
    /\ nowSnapshot \in 0..4
    /\ clientNextSnapshot \in 0..(MaxTimestamp + 1)
    /\ clientEndSnapshot \in 0..MaxTimestamp

DedicatedRaftGroup ==
    raftReady /\ raftGroup = "tso"

RangesAreCommitted ==
    /\ \A n \in Nodes:
        rangeStart[n] = 0
        \/ /\ rangeStart[n] <= rangeEnd[n]
           /\ rangeEnd[n] <= committedEnd
           /\ rangeNext[n] \in rangeStart[n]..(rangeEnd[n] + 1)
    /\ clientStart = 0
       \/ /\ clientStart <= clientEnd
          /\ clientEnd <= committedEnd
          /\ clientNext \in clientStart..(clientEnd + 1)

LeaderOnlyIssue ==
    /\ lastEvent = "server_issued" =>
        /\ lastActor = leaderSnapshot
        /\ lastActor = leaseOwnerSnapshot
        /\ nowSnapshot < leaseExpirySnapshot
    /\ lastEvent = "client_issued" =>
        /\ clientHint = leaderSnapshot
        /\ leaderSnapshot = leaseOwnerSnapshot
        /\ nowSnapshot < leaseExpirySnapshot
    /\ lastEvent = "not_leader" /\ lastActor \in Nodes =>
        lastActor # leader

IssuedTimestampsMonotonic ==
    lastEvent \in {"server_issued", "client_issued"} =>
        /\ issuedHigh > issuedSnapshot
        /\ issuedHigh <= committedEnd
        /\ issuedCount > 0

ReservedRangesDoNotOverlap ==
    lastEvent \in {"range_reserved", "client_batch"} =>
        /\ committedEnd > committedSnapshot
        /\ committedEnd - committedSnapshot <=
            BatchLimit + PhysicalFloor(physicalNow)

LeaseHandoffSafe ==
    /\ phase = "serving" => leader = leaseOwner
    /\ phase = "handoff_wait" =>
        /\ leader # leaseOwner
        /\ handoffNotBefore = leaseExpiry
    /\ lastEvent = "early_handoff_rejected" =>
        /\ nowSnapshot < handoffNotBefore
        /\ issuedHigh = issuedSnapshot
        /\ committedEnd = committedSnapshot

PhysicalRollbackPreservesFloor ==
    lastEvent = "physical_rollback" =>
        /\ physicalNow < physicalSnapshot
        /\ committedEnd = committedSnapshot
        /\ issuedHigh = issuedSnapshot

FollowerRejectHasNoAllocation ==
    lastEvent = "not_leader" /\ lastActor \in Nodes =>
        /\ committedEnd = committedSnapshot
        /\ issuedHigh = issuedSnapshot

ClientCacheSafe ==
    /\ ClientRangeAvailable => clientNext <= clientEnd
    /\ lastEvent = "client_issued" =>
        /\ clientNextSnapshot <= clientEndSnapshot
        /\ issuedHigh = clientNextSnapshot
    /\ retryCount <= 2

=============================================================================

