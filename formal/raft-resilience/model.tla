--------------------- MODULE model ---------------------
EXTENDS Naturals, FiniteSets

Nodes == {1, 2, 3}
Quorum(reachable) == Cardinality(reachable) >= 2

VARIABLES \* @type: Int;
          pc,
          \* @type: Int;
          leader,
          \* @type: Set(Int);
          alive,
          \* @type: Set(Int);
          reachable,
          \* @type: Int;
          committed,
          \* @type: Int -> Int;
          applied,
          \* @type: Bool;
          staleCommit,
          \* @type: Int;
          membershipEpoch

vars == <<pc, leader, alive, reachable, committed, applied,
          staleCommit, membershipEpoch>>

Init ==
    /\ pc = 0
    /\ leader = 1
    /\ alive = Nodes
    /\ reachable = Nodes
    /\ committed = 0
    /\ applied = [n \in Nodes |-> 0]
    /\ staleCommit = FALSE
    /\ membershipEpoch = 0

Commit ==
    /\ leader \in alive
    /\ leader \in reachable
    /\ Quorum(reachable \cap alive)
    /\ committed < 3
    /\ committed' = committed + 1
    /\ applied' = [applied EXCEPT ![leader] = committed + 1]
    /\ UNCHANGED <<leader, alive, reachable, staleCommit, membershipEpoch>>

CrashLeader ==
    /\ leader \in alive
    /\ alive' = alive \ {leader}
    /\ UNCHANGED <<leader, reachable, committed, applied, staleCommit, membershipEpoch>>

Elect ==
    /\ Cardinality(alive \cap reachable) >= 2
    /\ leader' \in alive \cap reachable
    /\ UNCHANGED <<alive, reachable, committed, applied, staleCommit, membershipEpoch>>

PartitionOne ==
    /\ \E n \in Nodes: reachable' = Nodes \ {n}
    /\ UNCHANGED <<leader, alive, committed, applied, staleCommit, membershipEpoch>>

StaleLeaderAttempt ==
    /\ leader \notin reachable \/ ~Quorum(reachable \cap alive)
    /\ staleCommit' = staleCommit
    /\ UNCHANGED <<leader, alive, reachable, committed, applied, membershipEpoch>>

Heal ==
    /\ reachable' = Nodes
    /\ alive' = Nodes
    /\ UNCHANGED <<leader, committed, applied, staleCommit, membershipEpoch>>

CatchUp ==
    /\ \E n \in alive: applied' = [applied EXCEPT ![n] = committed]
    /\ UNCHANGED <<leader, alive, reachable, committed, staleCommit, membershipEpoch>>

ChangeMembership ==
    /\ Quorum(reachable \cap alive)
    /\ membershipEpoch' = membershipEpoch + 1
    /\ UNCHANGED <<leader, alive, reachable, committed, applied, staleCommit>>

Step == Commit \/ CrashLeader \/ Elect \/ PartitionOne \/ StaleLeaderAttempt \/
        Heal \/ CatchUp \/ ChangeMembership
Next == /\ pc < 10 /\ pc' = pc + 1 /\ Step
Spec == Init /\ [][Next]_vars

TypeInvariant ==
    /\ pc \in 0..10
    /\ leader \in Nodes
    /\ alive \subseteq Nodes
    /\ reachable \subseteq Nodes
    /\ committed \in 0..3
    /\ applied \in [Nodes -> 0..3]
    /\ membershipEpoch \in Nat

AppliedNeverExceedsCommit == \A n \in Nodes: applied[n] <= committed
MinorityNeverCommits == ~staleCommit
SingleCommittedHistory == \A n \in Nodes: applied[n] <= committed
=======================================================
