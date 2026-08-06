----------------------------- MODULE model -----------------------------
EXTENDS Naturals

CONSTANT
    \* @type: Int;
    MaxClockSkew

Nodes == {"n1", "n2"}
Messages == {"m1", "m2"}
MessageKinds == {"swim", "gossip"}
MessageStates == {"absent", "inflight", "delivered", "rejected"}
Events == {
    "none", "local_tick", "wall_advanced", "wall_rollback",
    "swim_sent", "gossip_sent", "swim_delivered", "gossip_delivered",
    "duplicate_delivered", "skew_injected", "skew_rejected"
}
EventNodes == Nodes \cup {"none"}
EventMessages == Messages \cup {"none"}

Sender(m) == "n1"
Target(m) == "n2"
Kind(m) == IF m = "m1" THEN "swim" ELSE "gossip"

Max(a, b) == IF a >= b THEN a ELSE b
Max3(a, b, c) == Max(Max(a, b), c)

TsGreater(p1, l1, p0, l0) ==
    p1 > p0 \/ (p1 = p0 /\ l1 > l0)

TickP(localP, wallP) == Max(localP, wallP)
TickL(localP, localL, wallP) ==
    IF wallP > localP THEN 0 ELSE localL + 1

MergeP(localP, wallP, remoteP) ==
    Max3(localP, wallP, remoteP)

MergeL(localP, localL, wallP, remoteP, remoteL) ==
    LET mergedP == MergeP(localP, wallP, remoteP) IN
    IF mergedP = localP /\ mergedP = remoteP
    THEN Max(localL, remoteL) + 1
    ELSE IF mergedP = localP
         THEN localL + 1
         ELSE IF mergedP = remoteP
              THEN remoteL + 1
              ELSE 0

VARIABLES
    \* @type: Str -> Int;
    wall,
    \* @type: Str -> Int;
    clockP,
    \* @type: Str -> Int;
    clockL,
    \* @type: Str -> Str;
    messageState,
    \* @type: Str -> Int;
    messageP,
    \* @type: Str -> Int;
    messageL,
    \* @type: Str -> Int;
    receiveCount,
    \* @type: Str -> Bool;
    swimApplied,
    \* @type: Str -> Bool;
    gossipApplied,
    \* @type: Str;
    lastEvent,
    \* @type: Str;
    lastNode,
    \* @type: Str;
    lastMessage,
    \* @type: Int;
    eventP,
    \* @type: Int;
    eventL,
    \* @type: Str -> Int;
    wallSnapshot,
    \* @type: Str -> Int;
    clockPSnapshot,
    \* @type: Str -> Int;
    clockLSnapshot,
    \* @type: Str -> Str;
    messageStateSnapshot,
    \* @type: Str -> Bool;
    swimSnapshot,
    \* @type: Str -> Bool;
    gossipSnapshot

Core == << wall, clockP, clockL, messageState, messageP, messageL,
           receiveCount, swimApplied, gossipApplied >>
Observation == << lastEvent, lastNode, lastMessage, eventP, eventL,
                 wallSnapshot, clockPSnapshot, clockLSnapshot,
                 messageStateSnapshot, swimSnapshot, gossipSnapshot >>
vars == << Core, Observation >>

Init ==
    /\ MaxClockSkew = 1
    /\ wall = [n \in Nodes |-> 0]
    /\ clockP = [n \in Nodes |-> 0]
    /\ clockL = [n \in Nodes |-> 0]
    /\ messageState = [m \in Messages |-> "absent"]
    /\ messageP = [m \in Messages |-> 0]
    /\ messageL = [m \in Messages |-> 0]
    /\ receiveCount = [m \in Messages |-> 0]
    /\ swimApplied = [n \in Nodes |-> FALSE]
    /\ gossipApplied = [n \in Nodes |-> FALSE]
    /\ lastEvent = "none"
    /\ lastNode = "none"
    /\ lastMessage = "none"
    /\ eventP = 0
    /\ eventL = 0
    /\ wallSnapshot = wall
    /\ clockPSnapshot = clockP
    /\ clockLSnapshot = clockL
    /\ messageStateSnapshot = messageState
    /\ swimSnapshot = swimApplied
    /\ gossipSnapshot = gossipApplied

Observe(event, node, message, p, l) ==
    /\ lastEvent' = event
    /\ lastNode' = node
    /\ lastMessage' = message
    /\ eventP' = p
    /\ eventL' = l
    /\ wallSnapshot' = wall
    /\ clockPSnapshot' = clockP
    /\ clockLSnapshot' = clockL
    /\ messageStateSnapshot' = messageState
    /\ swimSnapshot' = swimApplied
    /\ gossipSnapshot' = gossipApplied

LocalTick(n) ==
    LET p == TickP(clockP[n], wall[n]) IN
    LET l == TickL(clockP[n], clockL[n], wall[n]) IN
    /\ n \in Nodes
    /\ l <= 6
    /\ clockP' = [clockP EXCEPT ![n] = p]
    /\ clockL' = [clockL EXCEPT ![n] = l]
    /\ UNCHANGED << wall, messageState, messageP, messageL, receiveCount,
                    swimApplied, gossipApplied >>
    /\ Observe("local_tick", n, "none", p, l)

AdvanceWall(n) ==
    /\ n \in Nodes
    /\ wall[n] < 3
    /\ wall' = [wall EXCEPT ![n] = @ + 1]
    /\ UNCHANGED << clockP, clockL, messageState, messageP, messageL,
                    receiveCount, swimApplied, gossipApplied >>
    /\ Observe("wall_advanced", n, "none", clockP[n], clockL[n])

RollbackWall(n) ==
    /\ n \in Nodes
    /\ wall[n] > 0
    /\ wall' = [wall EXCEPT ![n] = @ - 1]
    /\ UNCHANGED << clockP, clockL, messageState, messageP, messageL,
                    receiveCount, swimApplied, gossipApplied >>
    /\ Observe("wall_rollback", n, "none", clockP[n], clockL[n])

SendMessage(m) ==
    LET sender == Sender(m) IN
    LET p == TickP(clockP[sender], wall[sender]) IN
    LET l == TickL(clockP[sender], clockL[sender], wall[sender]) IN
    /\ m \in Messages
    /\ messageState[m] = "absent"
    /\ l <= 6
    /\ clockP' = [clockP EXCEPT ![sender] = p]
    /\ clockL' = [clockL EXCEPT ![sender] = l]
    /\ messageState' = [messageState EXCEPT ![m] = "inflight"]
    /\ messageP' = [messageP EXCEPT ![m] = p]
    /\ messageL' = [messageL EXCEPT ![m] = l]
    /\ UNCHANGED << wall, receiveCount, swimApplied, gossipApplied >>
    /\ Observe(
        IF Kind(m) = "swim" THEN "swim_sent" ELSE "gossip_sent",
        sender, m, p, l)

DeliverMessage(m) ==
    LET target == Target(m) IN
    LET p == MergeP(
        clockP[target], wall[target], messageP[m]) IN
    LET l == MergeL(
        clockP[target], clockL[target], wall[target],
        messageP[m], messageL[m]) IN
    /\ m \in Messages
    /\ messageState[m] = "inflight"
    /\ messageP[m] <= wall[target] + MaxClockSkew
    /\ l <= 6
    /\ clockP' = [clockP EXCEPT ![target] = p]
    /\ clockL' = [clockL EXCEPT ![target] = l]
    /\ messageState' = [messageState EXCEPT ![m] = "delivered"]
    /\ receiveCount' = [receiveCount EXCEPT ![m] = @ + 1]
    /\ swimApplied' = [swimApplied EXCEPT
        ![target] = IF Kind(m) = "swim" THEN TRUE ELSE @]
    /\ gossipApplied' = [gossipApplied EXCEPT
        ![target] = IF Kind(m) = "gossip" THEN TRUE ELSE @]
    /\ UNCHANGED << wall, messageP, messageL >>
    /\ Observe(
        IF Kind(m) = "swim" THEN "swim_delivered" ELSE "gossip_delivered",
        target, m, p, l)

DeliverDuplicate(m) ==
    LET target == Target(m) IN
    LET p == MergeP(
        clockP[target], wall[target], messageP[m]) IN
    LET l == MergeL(
        clockP[target], clockL[target], wall[target],
        messageP[m], messageL[m]) IN
    /\ m \in Messages
    /\ messageState[m] = "delivered"
    /\ receiveCount[m] = 1
    /\ messageP[m] <= wall[target] + MaxClockSkew
    /\ l <= 6
    /\ clockP' = [clockP EXCEPT ![target] = p]
    /\ clockL' = [clockL EXCEPT ![target] = l]
    /\ receiveCount' = [receiveCount EXCEPT ![m] = 2]
    /\ UNCHANGED << wall, messageState, messageP, messageL,
                    swimApplied, gossipApplied >>
    /\ Observe("duplicate_delivered", target, m, p, l)

InjectSkewed(m) ==
    LET target == Target(m) IN
    /\ m \in Messages
    /\ messageState[m] = "absent"
    /\ wall[target] + MaxClockSkew + 1 <= 3
    /\ messageState' = [messageState EXCEPT ![m] = "inflight"]
    /\ messageP' =
        [messageP EXCEPT ![m] = wall[target] + MaxClockSkew + 1]
    /\ messageL' = [messageL EXCEPT ![m] = 0]
    /\ UNCHANGED << wall, clockP, clockL, receiveCount,
                    swimApplied, gossipApplied >>
    /\ Observe("skew_injected", Sender(m), m, clockP[Sender(m)], clockL[Sender(m)])

RejectSkewed(m) ==
    /\ m \in Messages
    /\ messageState[m] = "inflight"
    /\ messageP[m] > wall[Target(m)] + MaxClockSkew
    /\ messageState' = [messageState EXCEPT ![m] = "rejected"]
    /\ UNCHANGED << wall, clockP, clockL, messageP, messageL,
                    receiveCount, swimApplied, gossipApplied >>
    /\ Observe(
        "skew_rejected", Target(m), m,
        clockP[Target(m)], clockL[Target(m)])

Stutter == UNCHANGED vars

TickNext ==
    /\ lastEvent = "none"
       /\ LocalTick("n1")
    \/ /\ lastEvent = "local_tick"
          /\ eventP = 0
          /\ AdvanceWall("n1")
    \/ /\ lastEvent = "wall_advanced"
          /\ LocalTick("n1")
    \/ /\ lastEvent = "local_tick"
          /\ eventP = 1
          /\ eventL = 0
          /\ RollbackWall("n1")
    \/ /\ lastEvent = "wall_rollback"
          /\ LocalTick("n1")

ReorderNext ==
    /\ lastEvent = "none"
       /\ SendMessage("m1")
    \/ /\ lastEvent = "swim_sent"
          /\ AdvanceWall("n1")
    \/ /\ lastEvent = "wall_advanced"
          /\ SendMessage("m2")
    \/ /\ lastEvent = "gossip_sent"
          /\ DeliverMessage("m2")
    \/ /\ lastEvent = "gossip_delivered"
          /\ DeliverMessage("m1")
    \/ /\ lastEvent = "swim_delivered"
          /\ DeliverDuplicate("m2")

SkewNext ==
    /\ lastEvent = "none"
       /\ InjectSkewed("m1")
    \/ /\ lastEvent = "skew_injected"
          /\ RejectSkewed("m1")

Next ==
    (\E n \in Nodes: LocalTick(n) \/ AdvanceWall(n) \/ RollbackWall(n))
    \/ (\E m \in Messages:
        SendMessage(m) \/ DeliverMessage(m) \/ DeliverDuplicate(m)
        \/ InjectSkewed(m) \/ RejectSkewed(m))
    \/ Stutter

TypeOK ==
    /\ MaxClockSkew = 1
    /\ wall \in [Nodes -> 0..3]
    /\ clockP \in [Nodes -> 0..3]
    /\ clockL \in [Nodes -> 0..6]
    /\ messageState \in [Messages -> MessageStates]
    /\ messageP \in [Messages -> 0..3]
    /\ messageL \in [Messages -> 0..6]
    /\ receiveCount \in [Messages -> 0..2]
    /\ swimApplied \in [Nodes -> BOOLEAN]
    /\ gossipApplied \in [Nodes -> BOOLEAN]
    /\ lastEvent \in Events
    /\ lastNode \in EventNodes
    /\ lastMessage \in EventMessages
    /\ eventP \in 0..3
    /\ eventL \in 0..6
    /\ wallSnapshot \in [Nodes -> 0..3]
    /\ clockPSnapshot \in [Nodes -> 0..3]
    /\ clockLSnapshot \in [Nodes -> 0..6]
    /\ messageStateSnapshot \in [Messages -> MessageStates]
    /\ swimSnapshot \in [Nodes -> BOOLEAN]
    /\ gossipSnapshot \in [Nodes -> BOOLEAN]

LocalEventsAdvanceClock ==
    lastEvent \in {
        "local_tick", "swim_sent", "gossip_sent",
        "swim_delivered", "gossip_delivered", "duplicate_delivered"
    } =>
        /\ lastNode \in Nodes
        /\ TsGreater(
            eventP, eventL,
            clockPSnapshot[lastNode], clockLSnapshot[lastNode])
        /\ clockP[lastNode] = eventP
        /\ clockL[lastNode] = eventL

TickRuleCorrect ==
    lastEvent = "local_tick" =>
        /\ eventP = TickP(
            clockPSnapshot[lastNode], wallSnapshot[lastNode])
        /\ eventL = TickL(
            clockPSnapshot[lastNode], clockLSnapshot[lastNode],
            wallSnapshot[lastNode])

SendTimestampIncluded ==
    lastEvent \in {"swim_sent", "gossip_sent"} =>
        /\ lastMessage \in Messages
        /\ messageState[lastMessage] = "inflight"
        /\ messageP[lastMessage] = eventP
        /\ messageL[lastMessage] = eventL
        /\ lastEvent =
            IF Kind(lastMessage) = "swim" THEN "swim_sent" ELSE "gossip_sent"

ReceiveRuleCorrect ==
    lastEvent \in {
        "swim_delivered", "gossip_delivered", "duplicate_delivered"
    } =>
        /\ lastMessage \in Messages
        /\ lastNode = Target(lastMessage)
        /\ eventP = MergeP(
            clockPSnapshot[lastNode], wallSnapshot[lastNode],
            messageP[lastMessage])
        /\ eventL = MergeL(
            clockPSnapshot[lastNode], clockLSnapshot[lastNode],
            wallSnapshot[lastNode], messageP[lastMessage],
            messageL[lastMessage])
        /\ TsGreater(
            eventP, eventL,
            messageP[lastMessage], messageL[lastMessage])

SkewRejectedWithoutMutation ==
    lastEvent = "skew_rejected" =>
        /\ lastMessage \in Messages
        /\ messageState[lastMessage] = "rejected"
        /\ messageP[lastMessage] >
            wallSnapshot[lastNode] + MaxClockSkew
        /\ clockP = clockPSnapshot
        /\ clockL = clockLSnapshot
        /\ swimApplied = swimSnapshot
        /\ gossipApplied = gossipSnapshot

DuplicateApplicationIsIdempotent ==
    lastEvent = "duplicate_delivered" =>
        /\ messageStateSnapshot[lastMessage] = "delivered"
        /\ receiveCount[lastMessage] = 2
        /\ swimApplied = swimSnapshot
        /\ gossipApplied = gossipSnapshot

SwimAndGossipEventsAreStamped ==
    /\ lastEvent = "swim_delivered" =>
        /\ Kind(lastMessage) = "swim"
        /\ swimApplied[lastNode]
    /\ lastEvent = "gossip_delivered" =>
        /\ Kind(lastMessage) = "gossip"
        /\ gossipApplied[lastNode]

ReorderedDeliveryPreservesLocalOrder ==
    lastEvent = "swim_delivered"
        /\ lastMessage = "m1"
        /\ messageState["m2"] = "delivered"
    =>
        TsGreater(
            eventP, eventL,
            messageP["m2"], messageL["m2"])

WallRollbackDoesNotRegressHlc ==
    lastEvent = "wall_rollback" =>
        /\ wall[lastNode] < wallSnapshot[lastNode]
        /\ clockP = clockPSnapshot
        /\ clockL = clockLSnapshot

=============================================================================

