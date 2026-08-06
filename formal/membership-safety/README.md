# Membership safety model

This bounded model makes SWIM suspicion/death observational with respect to
Raft voters. Only an explicit quorum-authorized transition may remove a voter;
rejoin advances incarnation. It does not prescribe an automatic orchestrator.

Run from `formal/`:

```sh
docker compose -f compose.yml run --rm apalache typecheck membership-safety/model.tla
docker compose -f compose.yml run --rm apalache check --config=membership-safety/MembershipSafety.cfg --length=8 membership-safety/model.tla
```
