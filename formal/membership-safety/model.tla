--------------------- MODULE model ---------------------
EXTENDS Naturals, FiniteSets

Nodes == {1, 2, 3}
Statuses == {"alive", "suspect", "dead"}

VARIABLES \* @type: Int;
          pc,
          \* @type: Int -> Str;
          status,
          \* @type: Int -> Int;
          incarnation,
          \* @type: Set(Int);
          voters,
          \* @type: Bool;
          removalAuthorized,
          \* @type: Int;
          removalCount
vars == <<pc, status, incarnation, voters, removalAuthorized, removalCount>>

Init ==
    /\ pc = 0
    /\ status = [n \in Nodes |-> "alive"]
    /\ incarnation = [n \in Nodes |-> 0]
    /\ voters = Nodes
    /\ removalAuthorized = FALSE
    /\ removalCount = 0

Suspect ==
    /\ \E n \in Nodes: status' = [status EXCEPT ![n] = "suspect"]
    /\ UNCHANGED <<incarnation, voters, removalAuthorized, removalCount>>

Dead ==
    /\ \E n \in Nodes: status' = [status EXCEPT ![n] = "dead"]
    /\ UNCHANGED <<incarnation, voters, removalAuthorized, removalCount>>

Rejoin ==
    /\ \E n \in Nodes:
        /\ status' = [status EXCEPT ![n] = "alive"]
        /\ incarnation' = [incarnation EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<voters, removalAuthorized, removalCount>>

OldAliveIgnored ==
    /\ UNCHANGED <<status, incarnation, voters, removalAuthorized, removalCount>>

AuthorizeRemoval ==
    /\ removalAuthorized' = TRUE
    /\ UNCHANGED <<status, incarnation, voters, removalCount>>

RemoveVoter ==
    /\ removalAuthorized
    /\ Cardinality(voters) = 3
    /\ \E n \in voters: voters' = voters \ {n}
    /\ removalAuthorized' = FALSE
    /\ removalCount' = removalCount + 1
    /\ UNCHANGED <<status, incarnation>>

Step == Suspect \/ Dead \/ Rejoin \/ OldAliveIgnored \/ AuthorizeRemoval \/ RemoveVoter
Next == /\ pc < 8 /\ pc' = pc + 1 /\ Step
Spec == Init /\ [][Next]_vars

TypeInvariant ==
    /\ pc \in 0..8
    /\ status \in [Nodes -> Statuses]
    /\ incarnation \in [Nodes -> Nat]
    /\ voters \subseteq Nodes
    /\ removalAuthorized \in BOOLEAN
    /\ removalCount \in 0..1

SuspicionCannotRemoveVoter == Cardinality(voters) = 3 - removalCount
QuorumPreserved == Cardinality(voters) >= 2
IncarnationNonNegative == \A n \in Nodes: incarnation[n] >= 0
=======================================================
