# Chirps Multi-Raft finite model

This finite TLA+ model checks the v0.6 production bootstrap contract for one
three-voter Raft group together with the existing two-group lifecycle,
storage-isolation, routing, tick, and drain contracts.

The detailed bootstrap is:

1. create and commit the seed replica (n1);
2. publish n2 uninitialized, add it as a learner, catch it up, then promote it;
3. repeat the same sequence for n3;
4. mark the common voter set {n1, n2, n3} ready.

Membership failure actions preserve the prior role/publication/commit state.
Correlated responses are dispatched only when group, peer, and correlation ID
all match the pending request. Consensus state records leader claims, terms,
quorum-certified commits, and per-replica committed values.

## Local checks

Run only the canonical pinned tool from compose.yml:

    cd formal/multi-raft
    docker compose run --rm apalache typecheck model.tla
    docker compose run --rm apalache check --config=Bootstrap.cfg --length=12 model.tla
    docker compose run --rm apalache check --config=MembershipFailure.cfg --length=7 model.tla
    docker compose run --rm apalache check --config=Consensus.cfg --length=5 model.tla
    docker compose run --rm apalache check --config=Correlation.cfg --length=4 model.tla
    docker compose run --rm apalache check --config=Drain.cfg --length=10 model.tla
    docker compose run --rm apalache check --config=Isolation.cfg --length=9 model.tla

Each profile is a bounded safety viewpoint. BootstrapNext, DrainNext, and
IsolationNext intentionally constrain ordering so the complete critical path
fits in a reproducible local check. MembershipFailureNext, ConsensusNext, and
CorrelationNext retain the relevant nondeterministic failure or dispatch
choices. AssumeReadyBootstrap is used only by the consensus profile as a
checked-state setup abstraction; it is not a production transition and does
not replace the detailed bootstrap profile.

MultiRaft.cfg keeps the combined transition relation and Routing.cfg keeps the
broad routing relation for exploratory checks. They are not release gates:
their length-12 and length-13 explorations did not produce verdicts in the
available session, so only the six completed profile results in evidence.md
count as PASS.

The finite domain has two groups, three replicas, two correlations, one
proposal value at a time, and at most one route and one tick in flight per
group. These checks do not prove unbounded liveness/fairness, full OpenRaft
election or log-matching semantics, physical-network behavior, durability,
multi-OS behavior, or performance. Exact mappings and exclusions are in
catalog.yaml.
