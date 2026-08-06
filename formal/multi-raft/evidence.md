# Multi-Raft model evidence

Status: PASS for the six bounded profiles below (2026-08-06 Asia/Tokyo)

The model was re-typechecked and all six profiles were rerun after narrowing
the membership-failure property name and evidence scope. The chained command
completed with exit status 0; every profile again reported `NoError` at the
documented bound. Generated `_apalache-out` data was deleted after recording
the verdict.

## Finite bounds

- checker: Apalache 0.58.3, pinned by digest in compose.yml
- groups: 2 (g1, g2)
- replicas: 3 (n1, n2, n3); detailed bootstrap applies to g1
- voter target: {n1, n2, n3}
- correlations: 2 (c1, c2); at most one pending RPC per group
- consensus payload: one committed log position and a finite value domain
- in-flight work: at most one route and one tick per group (MaxInFlight = 1)
- profile bounds: bootstrap 12, membership failure 7, consensus 5,
  correlation 4, drain 10, isolation 9

## Completed results

    docker compose run --rm apalache typecheck model.tla
    Type checker [OK]
    Total time: 3.971 sec
    EXITCODE: OK

    docker compose run --rm apalache check --config=Bootstrap.cfg --length=12 model.tla
    The outcome is: NoError
    Checker reports no error up to computation length 12
    Total time: 11.188 sec
    EXITCODE: OK

    docker compose run --rm apalache check --config=MembershipFailure.cfg --length=7 model.tla
    The outcome is: NoError
    Checker reports no error up to computation length 7
    Total time: 51.112 sec
    EXITCODE: OK

    docker compose run --rm apalache check --config=Consensus.cfg --length=5 model.tla
    The outcome is: NoError
    Checker reports no error up to computation length 5
    Total time: 20.110 sec
    EXITCODE: OK

    docker compose run --rm apalache check --config=Correlation.cfg --length=4 model.tla
    The outcome is: NoError
    Checker reports no error up to computation length 4
    Total time: 9.263 sec
    EXITCODE: OK

    docker compose run --rm apalache check --config=Drain.cfg --length=10 model.tla
    The outcome is: NoError
    Checker reports no error up to computation length 10
    Total time: 10.43 sec
    EXITCODE: OK

    docker compose run --rm apalache check --config=Isolation.cfg --length=9 model.tla
    The outcome is: NoError
    Checker reports no error up to computation length 9
    Total time: 9.396 sec
    EXITCODE: OK

The bootstrap bound covers create/prepare/publish of n1, then
publish-uninitialized/add-learner/catch-up/promote for n2 and n3, then the
common voter-set readiness transition. The membership profile checks explicit
pre-submit rejection actions for publication, learner-add, catch-up, and
promotion. These actions do not model an OpenRaft request that was accepted and
then failed after a partial joint-consensus transition; transactional rollback
of such a transition is neither a requirement nor a model claim. The
correlation profile checks wrong group, wrong source, wrong correlation,
matched delivery, and timeout choices without mutating an unrelated request.
The drain profile checks that request and tick work finish before shutdown.
The isolation profile checks that a g1 tick failure does not cancel the pending
g2 tick and that the two storage namespaces remain distinct.

## Non-verdict exploratory runs

The combined MultiRaft.cfg --length=12 exploration did not finish: it reached
state 6 before the session was lost, leaving no checker verdict. It is not
counted as PASS. An earlier combined run produced a real counterexample because
ReplicaPublicationSafe incorrectly required every published replica's group to
remain active; this rejected the legitimate draining state. The invariant was
corrected to allow published replicas through active/draining/stopped, then
rechecked in the completed profiles.

The broad Routing.cfg --length=13 exploration similarly stopped at state 6
without a verdict, and an unconstrained DrainNext exploration reached state 10
without a final verdict. Those runs are also not PASS evidence. The adopted
gate splits the finite state space into the six requirement-aligned profile
boundaries listed above. This is a scope boundary, not a claim that the
combined transition relation was exhaustively checked.

## Refinement and local viewpoints

catalog.yaml is normative. It maps every property to production source and
separates completed local tests from the remaining planned viewpoints. Remote
replica publication, sequential learner catch-up/promotion, manager-side
correlated-response dispatch during drain, cancellation cleanup, and a
three-voter commit now have production-local tests. Unknown-route and
independent tick-failure viewpoints remain planned where the catalog says so.

AssumeReadyBootstrap is used only to set up the consensus profile. It does not
replace the twelve-step detailed bootstrap result and must not be refined as a
production action.

## Unmodeled environment and remaining uncertainty

- unbounded executions, fairness, and liveness
- OpenRaft's complete election, log matching, joint-consensus, snapshot, and
  membership-removal algorithms
- arbitrary retries, message loss/reordering, malformed framing, and Byzantine
  peers beyond explicit source/group/correlation mismatch
- crash persistence, fsync/disk failure, process restart, and real scheduler
  behavior
- physical network faults, multi-host and multi-OS execution
- throughput, latency, CPU, memory, and release performance thresholds

Those behaviors require the mapped Rust unit/component/multi-process tests and
the controlled performance evidence in docs/perf/v0_6_multi_raft.md.
