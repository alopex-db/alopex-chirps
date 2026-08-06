--------------------- MODULE model ---------------------
EXTENDS Naturals

HighLimit == 4
NormalLimit == 4
LowLimit == 4

VARIABLES \* @type: Int;
          pc,
          \* @type: Int;
          high,
          \* @type: Int;
          normal,
          \* @type: Int;
          low,
          \* @type: Int;
          turn,
          \* @type: Int;
          highWait,
          \* @type: Int;
          servedHigh,
          \* @type: Int;
          servedNormal,
          \* @type: Int;
          servedLow,
          \* @type: Int;
          dropped
\* @type: <<Int, Int, Int, Int, Int, Int, Int, Int, Int, Int>>;
vars == <<pc, high, normal, low, turn, highWait,
          servedHigh, servedNormal, servedLow, dropped>>

Init ==
    /\ pc = 0
    /\ high = 0
    /\ normal = 0
    /\ low = 0
    /\ turn = 0
    /\ highWait = 0
    /\ servedHigh = 0
    /\ servedNormal = 0
    /\ servedLow = 0
    /\ dropped = 0

EnqueueHigh ==
    /\ high' = IF high < HighLimit THEN high + 1 ELSE high
    /\ dropped' = IF high < HighLimit THEN dropped ELSE dropped + 1
    /\ UNCHANGED <<normal, low, turn, highWait, servedHigh, servedNormal, servedLow>>

EnqueueNormal ==
    /\ normal' = IF normal < NormalLimit THEN normal + 1 ELSE normal
    /\ dropped' = IF normal < NormalLimit THEN dropped ELSE dropped + 1
    /\ UNCHANGED <<high, low, turn, highWait, servedHigh, servedNormal, servedLow>>

EnqueueLow ==
    /\ low' = IF low < LowLimit THEN low + 1 ELSE low
    /\ dropped' = IF low < LowLimit THEN dropped ELSE dropped + 1
    /\ UNCHANGED <<high, normal, turn, highWait, servedHigh, servedNormal, servedLow>>

Serve ==
    /\ high + normal + low > 0
    /\ IF high > 0 /\ turn < 4
          THEN /\ high' = high - 1 /\ servedHigh' = servedHigh + 1
               /\ normal' = normal /\ low' = low /\ turn' = turn + 1
               /\ servedNormal' = servedNormal /\ servedLow' = servedLow
          ELSE IF normal > 0 /\ turn < 6
          THEN /\ normal' = normal - 1 /\ servedNormal' = servedNormal + 1
               /\ high' = high /\ low' = low /\ turn' = turn + 1
               /\ servedHigh' = servedHigh /\ servedLow' = servedLow
          ELSE /\ low' = IF low > 0 THEN low - 1 ELSE low
               /\ servedLow' = IF low > 0 THEN servedLow + 1 ELSE servedLow
               /\ high' = high /\ normal' = normal /\ turn' = 0
               /\ servedHigh' = servedHigh /\ servedNormal' = servedNormal
    /\ highWait' = IF high' > 0 /\ servedHigh' = servedHigh THEN highWait + 1 ELSE 0
    /\ UNCHANGED dropped

Step == EnqueueHigh \/ EnqueueNormal \/ EnqueueLow \/ Serve
Next == /\ pc < 12 /\ pc' = pc + 1 /\ Step
Spec == Init /\ [][Next]_vars

TypeInvariant ==
    /\ pc \in 0..12
    /\ high \in 0..HighLimit
    /\ normal \in 0..NormalLimit
    /\ low \in 0..LowLimit
    /\ turn \in 0..6
    /\ highWait \in Nat

QueuesBounded == high <= HighLimit /\ normal <= NormalLimit /\ low <= LowLimit
ControlWaitBounded == highWait <= 3
CountersConsistent == servedHigh + servedNormal + servedLow <= pc
=======================================================
