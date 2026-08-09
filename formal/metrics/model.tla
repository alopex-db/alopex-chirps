---------------------- MODULE model ----------------------
EXTENDS Naturals

CONSTANT
    \* @type: Bool;
    AuthRequired

Groups == {"g0", "g1"}
States == {"follower", "candidate", "leader"}
AuthValues == {"none", "invalid", "valid"}
Responses == {"idle", "ok", "unauthorized", "error"}
MaxVersion == 3

VARIABLES
    \* @type: Int;
    sourceVersion,
    \* @type: Int;
    cachedVersion,
    \* @type: Str -> Str;
    sourceState,
    \* @type: Str -> Str;
    cachedState,
    \* @type: Bool;
    cacheValid,
    \* @type: Bool;
    encodingFailed,
    \* @type: Str;
    requestAuth,
    \* @type: Str;
    response,
    \* @type: Bool;
    bodyPresent,
    \* @type: Int;
    servedVersion,
    \* @type: Str -> Str;
    servedState

vars == << sourceVersion, cachedVersion, sourceState, cachedState, cacheValid,
           encodingFailed, requestAuth, response, bodyPresent, servedVersion,
           servedState >>

Init ==
    /\ sourceVersion = 0
    /\ cachedVersion = 0
    /\ sourceState = [g \in Groups |-> "follower"]
    /\ cachedState = [g \in Groups |-> "follower"]
    /\ cacheValid = FALSE
    /\ encodingFailed = FALSE
    /\ requestAuth = "none"
    /\ response = "idle"
    /\ bodyPresent = FALSE
    /\ servedVersion = 0
    /\ servedState = [g \in Groups |-> "follower"]

ChangeGroup(g, state) ==
    /\ sourceVersion < MaxVersion
    /\ sourceVersion' = sourceVersion + 1
    /\ sourceState' = [sourceState EXCEPT ![g] = state]
    /\ requestAuth' = "none"
    /\ response' = "idle"
    /\ bodyPresent' = FALSE
    /\ UNCHANGED << cachedVersion, cachedState, cacheValid, encodingFailed,
                    servedVersion, servedState >>

RefreshSuccess ==
    /\ cachedVersion' = sourceVersion
    /\ cachedState' = sourceState
    /\ cacheValid' = TRUE
    /\ encodingFailed' = FALSE
    /\ requestAuth' = "none"
    /\ response' = "idle"
    /\ bodyPresent' = FALSE
    /\ UNCHANGED << sourceVersion, sourceState, servedVersion, servedState >>

RefreshFailure ==
    /\ encodingFailed' = TRUE
    /\ requestAuth' = "none"
    /\ response' = "idle"
    /\ bodyPresent' = FALSE
    /\ UNCHANGED << sourceVersion, cachedVersion, sourceState, cachedState,
                    cacheValid, servedVersion, servedState >>

Request(auth) ==
    /\ requestAuth' = auth
    /\ encodingFailed' = FALSE
    /\ IF AuthRequired /\ auth # "valid"
       THEN /\ response' = "unauthorized"
            /\ bodyPresent' = FALSE
            /\ UNCHANGED << servedVersion, servedState >>
       ELSE IF cacheValid
            THEN /\ response' = "ok"
                 /\ bodyPresent' = TRUE
                 /\ servedVersion' = cachedVersion
                 /\ servedState' = cachedState
            ELSE /\ response' = "error"
                 /\ bodyPresent' = FALSE
                 /\ UNCHANGED << servedVersion, servedState >>
    /\ UNCHANGED << sourceVersion, cachedVersion, sourceState, cachedState,
                    cacheValid >>

Stutter == UNCHANGED vars

Next ==
    (\E g \in Groups, state \in States: ChangeGroup(g, state)) \/
    RefreshSuccess \/ RefreshFailure \/
    (\E auth \in AuthValues: Request(auth)) \/ Stutter

TypeOK ==
    /\ AuthRequired \in BOOLEAN
    /\ sourceVersion \in 0..MaxVersion
    /\ cachedVersion \in 0..MaxVersion
    /\ sourceState \in [Groups -> States]
    /\ cachedState \in [Groups -> States]
    /\ cacheValid \in BOOLEAN
    /\ encodingFailed \in BOOLEAN
    /\ requestAuth \in AuthValues
    /\ response \in Responses
    /\ bodyPresent \in BOOLEAN
    /\ servedVersion \in 0..MaxVersion
    /\ servedState \in [Groups -> States]

UnauthorizedNeverExposesMetrics ==
    response = "unauthorized" => ~bodyPresent

SuccessfulResponseUsesCachedSnapshot ==
    response = "ok" =>
        /\ bodyPresent
        /\ cacheValid
        /\ servedVersion = cachedVersion
        /\ servedState = cachedState

ServedMetricsNeverComeFromTheFuture ==
    bodyPresent => servedVersion <= sourceVersion

EmptyCacheNeverSucceeds ==
    ~cacheValid => response # "ok"

AuthPolicyIsEnforced ==
    AuthRequired /\ requestAuth # "valid" /\ response # "idle" =>
        response = "unauthorized"

==========================================================
