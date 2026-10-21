<p align="center">
  <a href="https://alopex-db.github.io/concepts/chirps/">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-chirps-dark-800.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-chirps-light-800.png">
      <img alt="Alopex Chirps" src="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-chirps-light-800.png" width="400">
    </picture>
  </a>
</p>

<h3 align="center">The control plane your distributed system was going to write anyway.</h3>

<p align="center">
  <b>Gossip membership, QUIC messaging, Multi-Raft, and file transfer in one Rust crate.</b><br>
  Mutual TLS is mandatory, not a deployment option.
</p>

<p align="center">
  <a href="https://alopex-db.github.io/concepts/chirps/">Concept</a> &bull;
  <a href="https://docs.rs/alopex-chirps">API Docs</a> &bull;
  <a href="./CHANGELOG.md">Changelog</a> &bull;
  <a href="./docs">Design Docs</a> &bull;
  <a href="https://github.com/alopex-db/alopex">Alopex DB</a> &bull;
  <a href="https://github.com/sponsors/asopitech">Sponsor</a>
</p>

<p align="center">
  <a href="https://crates.io/crates/alopex-chirps"><img alt="crates.io" src="https://img.shields.io/crates/v/alopex-chirps.svg?label=crates.io&color=5FB4C9"></a>
  <a href="https://docs.rs/alopex-chirps"><img alt="docs.rs" src="https://img.shields.io/docsrs/alopex-chirps?label=docs.rs"></a>
  <a href="https://crates.io/crates/alopex-chirps"><img alt="downloads" src="https://img.shields.io/crates/d/alopex-chirps?color=1E3A5F"></a>
  <a href="#license"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue"></a>
  <a href="https://github.com/sponsors/asopitech"><img alt="Sponsor" src="https://img.shields.io/badge/sponsor-%E2%9D%A4-db61a2"></a>
  <a href="https://github.com/alopex-db/alopex-chirps/stargazers"><img alt="Stars" src="https://img.shields.io/github/stars/alopex-db/alopex-chirps?style=flat&color=7EC8E3"></a>
</p>

---

## Install

```bash
# Membership, QUIC transport, messaging, file transfer
cargo add alopex-chirps

# With consensus: per-group Raft, timestamp oracle, causal gossip, snapshots
cargo add alopex-chirps --features multi-raft,tso,hlc,snapshot
```

Consensus, TSO, HLC, and snapshot transfer are **off by default** — a plain
`cargo add` gives you a membership and messaging mesh and nothing heavier.

## Join a mesh

```rust
use alopex_chirps::{start, Frame, NodeConfig, UserMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = NodeConfig {
        bind_addr: "0.0.0.0:7946".parse()?,
        seeds: vec!["10.0.0.1:7946".parse()?],
        cert_path: Some("/etc/chirps/node.der".into()),
        key_path: Some("/etc/chirps/node.key.der".into()),
        trusted_cert_paths: vec!["/etc/chirps/cluster-ca.der".into()],
        ..NodeConfig::default()
    };

    let mesh = start(config).await?;

    mesh.on_node_join(|node| println!("joined: {node:?}"));
    mesh.on_node_leave(|node| println!("left: {node:?}"));

    let mut inbox = mesh.subscribe().await?;
    tokio::spawn(async move {
        while let Some((from, frame)) = inbox.recv().await {
            println!("{from:?} -> {frame:?}");
        }
    });

    let delivered = mesh
        .broadcast(Frame::User(UserMessage { payload: b"hello mesh".to_vec() }))
        .await?;
    println!("delivered to {delivered} peers");

    println!("membership: {:?}", mesh.membership().await);
    Ok(())
}
```

No DNS and no discovery service: a node joins from a static seed list, and
gossip does the rest. `node_id` is generated once and persisted at
`node_id_path`, so a restarted node keeps its identity.

## Why Chirps

Every distributed system eventually grows the same layer — who is alive, how
to reach them, and how to agree on something. Chirps is that layer as a
dependency instead of a rewrite.

- **Independent by design.** Alopex DB and jjvm are *users* of Chirps, not
  dependencies of it. Nothing here assumes a database.
- **Secure by construction.** QUIC with TLS 1.3, the `alopex` ALPN, and
  certificate verification that cannot be switched off. Since v0.6.3 the
  server verifies client certificates too — mutual TLS is required.
- **Bounded, not best-effort.** Receive buffers are profile-specific, there is
  staged backpressure and a node-wide memory budget, and each Raft group gates
  at eight in-flight proposals.
- **Observable.** A shared Prometheus registry, an authenticated metrics
  endpoint, and a Grafana dashboard at
  [`docs/observability/grafana/chirps-v0.6.json`](docs/observability/grafana/chirps-v0.6.json).

Use it as the control plane for distributed databases, distributed runtimes
and virtual machines, service meshes, and edge or IoT clusters.

## What is in the box

| Layer | What you get |
|:--|:--|
| **Membership** | SWIM-style gossip with `alive` / `suspect` / `dead`, indirect probes, persistent `NodeId`, seed-list join without DNS |
| **Transport** | QUIC (Quinn 0.11) with TLS 1.3, flow-control windows, stream limits, idle timeouts, keep-alive, connection caps, idle eviction, health checks |
| **Messaging** | `send_to`, `broadcast`, `subscribe`, profile-aware and enveloped variants, priority scheduling, bounded send queue |
| **Consensus** `multi-raft` | `MultiRaftManager` and `RaftStorageFactory` — per-group storage isolation, routing, tick, and per-group backpressure |
| **Time** `tso` | `TimestampOracle` and `TsoClient`, monotonic across leader handoff and clock rollback |
| **Causality** `hlc` | `HybridTimestamp` and `HlcGossip` in SWIM messages — per-peer ordering, deduplication, clock-skew rejection |
| **Snapshots** `snapshot` | Chunked Raft snapshot transfer with retransmission, bounded parallelism, and atomic install |
| **File transfer** | `MeshHandle::file_transfer(config)` — Zstd compression, resumable chunks, checksum NACK and retransmit, full-hash verification before atomic placement, symlink policy, Unix mode and mtime preservation |
| **Memory** | `get_memory_stats()`, `resize_memory_budget()`, `trigger_gc()`, integrated cache manager, Raft log cache with LRU eviction |

The workspace ships eight crates (`chirps-core`, `chirps-wire`,
`chirps-transport-quic`, `chirps-gossip-swim`, `chirps-raft-storage`,
`chirps-file-transfer`, `chirps-mock`, and the `alopex-chirps` umbrella), so a
consumer can depend on only the layer it needs.

## TLS, and the v0.6.3 upgrade

Mutual TLS became **mandatory in v0.6.3**. Upgrading an existing cluster is an
ordered operation:

1. Before switching any node, set `NodeConfig::trusted_cert_paths` on every
   node to the cluster CA certificate — or to every permitted peer certificate
   — and confirm each file is readable from that node.
2. Then give each node a client certificate and matching private key, and
   restart it. The server side verifies client certificates with
   `WebPkiClientVerifier`.
3. A node from an older release presents no client certificate and will fail
   the handshake, so do not mix versions into a cluster before the trusted
   paths and certificates are in place.

Certificates and keys are DER. There is no configuration that accepts an
unknown certificate; verification is always on.

## Status and limits

Pre-1.0, under active development, with the edges written down:

- **Feature-gated capabilities are not in the default build.** Multi-Raft,
  TSO, HLC, and snapshot transfer each need their feature enabled.
- **v0.7 "Durable" is contract-only today.** Profile-aware routing, the
  capability and error surface, the Durable envelope reservation, and the
  no-fallback rule are fixed by contract tests; the Iggy-backed persistence
  implementation lands in v0.7.
- **`examples/simple-mesh.rs` is reference source, not a Cargo target.** It
  sits outside every package in the workspace, so `cargo run --example
  simple-mesh` does not resolve. Read it for the minimal
  `start` / `broadcast` / `send_to` / subscribe shape. The runnable examples
  live in the file-transfer crate:
  `cargo run -p alopex-chirps-file-transfer --example two_node_transfer`.
- Self-signed, per-node certificates are fine for local development through
  `trusted_cert_paths`; production wants a cluster CA.

[CHANGELOG.md](CHANGELOG.md) records each release, including the breaking
changes and their upgrade procedures.

## Build and verify

```bash
cargo build --workspace --all-features
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

The deterministic network-failure harness
(`tools/chirps-deterministic-harness`) expands a seed, replays a failure
sequence, and minimizes it. Design records and performance evidence are under
[`docs/`](docs).

## The Alopex family

| Project | What it is |
|:--|:--|
| [**alopex**](https://github.com/alopex-db/alopex) | The unified engine — SQL + vector (HNSW) + columnar, embedded to cluster |
| [**alopex-skulk**](https://github.com/alopex-db/alopex-skulk) | Time-series storage, ingest, and embedded PromQL / SQL-TS |
| **alopex-chirps** | This repo — gossip and messaging mesh, Chirps is the cluster control plane |
| [**alopex-ui**](https://github.com/alopex-db/alopex-ui) | Alopex Data UI — one DataSource contract for data applications |

Chirps is usable on its own; the family is documented at
<https://alopex-db.github.io>.

## Contributing

Issues and pull requests are welcome. The organization
[contributing guide](https://github.com/alopex-db/.github/blob/main/CONTRIBUTING.md)
covers what a pull request needs, and
[SECURITY.md](https://github.com/alopex-db/.github/blob/main/SECURITY.md) has
the private route for vulnerability reports — relevant here, since this crate
is a transport and authentication surface.

## Sponsor

Chirps is built in the open by [@asopitech](https://github.com/asopitech).
GitHub Sponsors funds continued development across the Alopex family, and
higher tiers follow the work before it is public — $3/month OSS Support,
$12/month Early Access, $99/month Enterprise-focused Early Access.

<p align="center">
  <a href="https://github.com/sponsors/asopitech?metadata_source=alopex_chirps_readme&metadata_campaign=sponsor_early_access&metadata_content=readme_sponsor_section"><img alt="Sponsor @asopitech on GitHub Sponsors" src="https://img.shields.io/badge/GitHub%20Sponsors-%E2%9D%A4%20Sponsor%20%40asopitech-db61a2?logo=githubsponsors&logoColor=white&style=for-the-badge"></a>
  <a href="https://buymeacoffee.com/asopitechia"><img alt="Buy Me a Coffee" src="https://img.shields.io/badge/Buy%20Me%20a%20Coffee-%E2%98%95-FFDD00?style=for-the-badge&logo=buymeacoffee&logoColor=black"></a>
</p>

## License

Apache-2.0 OR MIT, at your option.
