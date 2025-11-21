# Alopex Chirps

**A lightweight, secure gossip and messaging mesh built on UDP/QUIC — inspired by the Arctic fox.**

Alopex Chirps is the communication and membership layer for distributed systems. Designed after the fast, light, and adaptive communication patterns of the Arctic fox, Chirps provides node discovery, gossip-based membership, and low-latency messaging over QUIC.

Chirps can be used as the control-plane foundation for:

* distributed databases
* distributed runtimes and virtual machines
* microservices and service meshes
* edge and IoT clusters

It is completely independent from AlopexDB or jjvm — those are *users* of Chirps, not dependencies.

---

## Features (v0.1)

* **UDP/QUIC-based transport** using Rust-native QUIC implementation
* **Node identity** with persistent `node_id`
* **Gossip (SWIM-style)** membership: `alive / suspect / dead`
* **Cluster join without DNS** via static seed list
* **Secure-by-default** through QUIC/TLS
* **Lightweight messaging API**:

  * `send_to(node_id, payload)`
  * `broadcast(payload)`
  * `subscribe(handler)`
* **Event hooks** for `on_node_join`, `on_node_leave`, `on_status_change`

---

## Example

```rust
use alopex_chirps::{Mesh, NodeConfig};

#[tokio::main]
async fn main() {
    let config = NodeConfig::new().with_seeds(vec![
        "192.168.0.10:42000".parse().unwrap(),
    ]);

    let mut mesh = Mesh::start(config).await.unwrap();

    mesh.subscribe(|from, payload| {
        println!("chirp from {from}: {:?}", payload);
    });

    mesh.broadcast(b"alopex says hello").await.unwrap();
}
```

