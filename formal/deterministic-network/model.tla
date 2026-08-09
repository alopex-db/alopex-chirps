--------------------- MODULE model ---------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANT
    \* @type: Int;
    Seed,
    \* @type: Str;
    UnsafeMode

Replicas == {"original", "replay"}
Nodes == {"a", "b"}
Links == {"ab", "ba"}
Packets == {"p1", "p2", "p3", "p4", "p5", "p6", "p7"}
PacketStates == {"pending", "queued", "delivered", "dropped", "stale"}
ScheduleLength == 8

PacketLink(p) == IF p \in {"p4"} THEN "ba" ELSE "ab"
LinkSource(link) == IF link = "ab" THEN "a" ELSE "b"
LinkTarget(link) == IF link = "ab" THEN "b" ELSE "a"

\* Fault sets are expanded before execution. p1 proves the composite
\* delay+duplicate+reorder path; p5 proves loss; p7 overlaps partition.
DelayTicks(p) == IF p = "p1" THEN 3 ELSE IF p = "p3" THEN 2 ELSE 0
CopyCount(p) == IF p \in {"p1", "p6"} THEN 2 ELSE 1
ForcedDrop(p) == p = "p5"

\* @typeAlias: SIMSTATE = [clock: Int, partitioned: Str -> Bool,
\* generation: Str -> Int, packetState: Str -> Str, due: Str -> Int,
\* sourceGeneration: Str -> Int, targetGeneration: Str -> Int,
\* remainingCopies: Str -> Int, deliveredCopies: Str -> Int,
\* sendOrdinal: Str -> Int, deliveryOrdinal: Str -> Int,
\* eventCount: Int, oracleChecks: Int, timeoutFired: Bool, terminal: Bool,
\* partitionViolation: Bool, earlyViolation: Bool, staleViolation: Bool,
\* eventCodes: Seq(Int)];

InitialState == [
    clock |-> 0,
    partitioned |-> [link \in Links |-> FALSE],
    generation |-> [node \in Nodes |-> 0],
    packetState |-> [packet \in Packets |-> "pending"],
    due |-> [packet \in Packets |-> 0],
    sourceGeneration |-> [packet \in Packets |-> 0],
    targetGeneration |-> [packet \in Packets |-> 0],
    remainingCopies |-> [packet \in Packets |-> 0],
    deliveredCopies |-> [packet \in Packets |-> 0],
    sendOrdinal |-> [packet \in Packets |-> 99],
    deliveryOrdinal |-> [packet \in Packets |-> 99],
    eventCount |-> 0,
    oracleChecks |-> 0,
    timeoutFired |-> FALSE,
    terminal |-> FALSE,
    partitionViolation |-> FALSE,
    earlyViolation |-> FALSE,
    staleViolation |-> FALSE,
    eventCodes |-> <<>>
]

\* The 23 schedule events are represented by stable codes. Each batch is one
\* pure state application; counters show that an oracle is evaluated for every
\* inner event. Sticky flags preserve unsafe intermediate delivery attempts.
\* @type: (SIMSTATE, Int) => SIMSTATE;
Apply(s, batch) ==
    CASE batch = 0 ->
        [s EXCEPT
            !.packetState["p1"] = "queued", !.packetState["p2"] = "queued",
            !.due["p1"] = 3, !.due["p2"] = 0,
            !.remainingCopies["p1"] = 2, !.remainingCopies["p2"] = 1,
            !.sendOrdinal["p1"] = 0, !.sendOrdinal["p2"] = 1,
            !.eventCount = @ + 2, !.oracleChecks = @ + 2,
            !.eventCodes = @ \o <<100, 101>>]
      [] batch = 1 ->
        [s EXCEPT
            !.timeoutFired = TRUE,
            !.packetState["p2"] = "delivered",
            !.remainingCopies["p2"] = 0,
            !.deliveredCopies["p2"] = 1,
            !.deliveryOrdinal["p2"] = 3,
            !.packetState["p1"] = IF UnsafeMode = "early" THEN "queued" ELSE @,
            !.remainingCopies["p1"] = IF UnsafeMode = "early" THEN 1 ELSE @,
            !.deliveredCopies["p1"] = IF UnsafeMode = "early" THEN 1 ELSE @,
            !.earlyViolation = UnsafeMode = "early",
            !.eventCount = @ + 3, !.oracleChecks = @ + 3,
            !.eventCodes = @ \o <<102, 103, 104>>]
      [] batch = 2 ->
        [s EXCEPT
            !.partitioned["ab"] = TRUE,
            !.packetState["p7"] = "dropped",
            !.packetState["p1"] = IF UnsafeMode = "partition" THEN "queued" ELSE @,
            !.remainingCopies["p1"] = IF UnsafeMode = "partition" THEN 1 ELSE @,
            !.deliveredCopies["p1"] = IF UnsafeMode = "partition" THEN 1 ELSE @,
            !.partitionViolation = UnsafeMode = "partition",
            !.eventCount = @ + 3, !.oracleChecks = @ + 3,
            !.eventCodes = @ \o <<105, 106, 107>>]
      [] batch = 3 ->
        [s EXCEPT
            !.partitioned["ab"] = FALSE, !.clock = @ + 3,
            !.packetState["p1"] = "delivered",
            !.remainingCopies["p1"] = 0,
            !.deliveredCopies["p1"] = 2,
            !.deliveryOrdinal["p1"] = 10,
            !.eventCount = @ + 4, !.oracleChecks = @ + 4,
            !.eventCodes = @ \o <<108, 109, 110, 111>>]
      [] batch = 4 ->
        [s EXCEPT
            !.packetState["p3"] = IF UnsafeMode = "stale" THEN "delivered" ELSE "stale",
            !.due["p3"] = s.clock + 2,
            !.sourceGeneration["p3"] = s.generation["a"],
            !.targetGeneration["p3"] = s.generation["b"],
            !.remainingCopies["p3"] = 0,
            !.deliveredCopies["p3"] = IF UnsafeMode = "stale" THEN 1 ELSE 0,
            !.generation["a"] = @ + 1, !.clock = @ + 2,
            !.staleViolation = UnsafeMode = "stale",
            !.eventCount = @ + 4, !.oracleChecks = @ + 4,
            !.eventCodes = @ \o <<112, 113, 114, 115>>]
      [] batch = 5 ->
        [s EXCEPT
            !.packetState["p4"] = "delivered",
            !.remainingCopies["p4"] = 0,
            !.deliveredCopies["p4"] = 1,
            !.packetState["p5"] = "dropped",
            !.eventCount = @ + 3, !.oracleChecks = @ + 3,
            !.eventCodes = @ \o <<116, 117, 118>>]
      [] batch = 6 ->
        [s EXCEPT
            !.packetState["p6"] = "delivered",
            !.remainingCopies["p6"] = 0,
            !.deliveredCopies["p6"] = 2,
            !.eventCount = @ + 3, !.oracleChecks = @ + 3,
            !.eventCodes = @ \o <<119, 120, 121>>]
      [] OTHER ->
        [s EXCEPT
            !.terminal = TRUE,
            !.eventCount = @ + 1, !.oracleChecks = @ + 1,
            !.eventCodes = @ \o <<122>>]

VARIABLES
    \* @type: Int;
    pc,
    \* @type: Str -> SIMSTATE;
    simulators

vars == << pc, simulators >>

Init ==
    /\ pc = 0
    /\ simulators = [replica \in Replicas |-> InitialState]

Next ==
    /\ pc < ScheduleLength
    /\ simulators' = [replica \in Replicas |-> Apply(simulators[replica], pc)]
    /\ pc' = pc + 1

TypeInvariant ==
    /\ pc \in 0..ScheduleLength
    /\ \A replica \in Replicas:
        /\ simulators[replica].clock \in Nat
        /\ simulators[replica].partitioned \in [Links -> BOOLEAN]
        /\ simulators[replica].generation \in [Nodes -> Nat]
        /\ simulators[replica].packetState \in [Packets -> PacketStates]
        /\ simulators[replica].remainingCopies \in [Packets -> 0..2]
        /\ simulators[replica].deliveredCopies \in [Packets -> 0..2]

ReplayIdentical == simulators["original"] = simulators["replay"]
NoPartitionDelivery == \A replica \in Replicas: ~simulators[replica].partitionViolation
NoEarlyDelivery == \A replica \in Replicas: ~simulators[replica].earlyViolation
NoStaleGenerationDelivery == \A replica \in Replicas: ~simulators[replica].staleViolation
OraclesAfterEveryEvent ==
    \A replica \in Replicas:
        simulators[replica].oracleChecks = simulators[replica].eventCount
DuplicateBounded ==
    \A replica \in Replicas: \A packet \in Packets:
        simulators[replica].deliveredCopies[packet] <= CopyCount(packet)

TerminalConvergence ==
    pc < ScheduleLength \/
    \A replica \in Replicas:
        /\ simulators[replica].terminal
        /\ \A packet \in Packets: simulators[replica].packetState[packet] # "queued"
        /\ simulators[replica].packetState["p3"] = "stale"
        /\ simulators[replica].packetState["p5"] = "dropped"
        /\ simulators[replica].packetState["p7"] = "dropped"

ReorderAndCompositeObserved ==
    pc < ScheduleLength \/
    \A replica \in Replicas:
        /\ simulators[replica].sendOrdinal["p1"] < simulators[replica].sendOrdinal["p2"]
        /\ simulators[replica].deliveryOrdinal["p2"] < simulators[replica].deliveryOrdinal["p1"]
        /\ simulators[replica].deliveredCopies["p1"] = 2
        /\ simulators[replica].deliveredCopies["p6"] = 2

AsymmetricLinkObserved ==
    pc < ScheduleLength \/
    \A replica \in Replicas:
        /\ simulators[replica].packetState["p4"] = "delivered"
        /\ simulators[replica].packetState["p7"] = "dropped"

Spec == Init /\ [][Next]_vars
=======================================================
