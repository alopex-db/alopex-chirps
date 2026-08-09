# v0.5.2 QUIC seed bootstrap: design and acceptance

## Evidence gathered before implementation

- `quinn` 0.10.2's bundled server example constructs `rustls::ServerConfig`, sets
  `alpn_protocols`, then wraps it with `quinn::ServerConfig::with_crypto`.
- `quinn-proto` 0.10.6's ALPN tests set an overlapping protocol on both the
  client and server.  A client-only protocol list is rejected during the TLS
  handshake.
- The local failing test produced `peer doesn't support any known protocol`.
  This establishes the immediate failure as missing server ALPN, not a timeout
  or a gossip scheduling issue.
- The existing transport built a distinct self-signed certificate per node and
  added only that node's certificate to its client root store.  Even after ALPN
  is correct, independently generated certificates therefore cannot mutually
  authenticate.

## Contract

1. QUIC server and client configurations must both negotiate the `alopex` ALPN.
2. Certificate verification remains enabled.  The implementation must not add
   a dangerous verifier or silently accept an unknown peer certificate.
3. `NodeConfig::trusted_cert_paths` is an opt-in list of DER trust anchors.
   The local node certificate remains trusted for the existing shared
   self-signed development setup.
4. Missing configured trust-anchor files are rejected at startup through
   `NodeConfig::validate`.
5. The seed-reconnect test uses two distinct self-signed identities that trust
   both public certificates.  The mesh E2E uses three distinct identities,
   including restart/reconnect, to prove that no shared private key is needed.

## Acceptance

- All five ignored `quic_integration` tests pass on real UDP/QUIC.
- Both ignored `three_node_mesh` tests pass: distinct self-signed identities
  and an explicit shared-certificate deployment fixture.
- The release workflow runs both suites before publication.
- The local `simple-mesh` example creates a shared self-signed development
  credential explicitly; production deployments configure their own identity
  and trust roots.
