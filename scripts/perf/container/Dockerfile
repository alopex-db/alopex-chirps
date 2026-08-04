# The harness records the digest of the resulting image. RUST_IMAGE may be
# supplied as a digest-pinned reference by a calibration runner.
ARG RUST_IMAGE=rust:1.96-bookworm
FROM ${RUST_IMAGE} AS build

ARG SOURCE_SHA=unknown

WORKDIR /src
COPY . .
RUN cargo build --locked --release -p alopex-chirps-file-transfer --example two_node_transfer

LABEL org.opencontainers.image.revision="${SOURCE_SHA}"

# This named stage deliberately retains Cargo, the exact source tree, and the
# compiled workspace.  The container-validation harness executes unit,
# integration, and Criterion workloads here before the stripped product image
# is allowed into the ft-1g-v1 binary measurement.
FROM build AS validation
RUN cargo test --locked \
        -p alopex-chirps-file-transfer \
        -p alopex-chirps-transport-quic \
        --tests --no-run \
    && cargo bench --locked -p alopex-chirps-file-transfer --bench file_transfer_components --no-run

FROM debian:bookworm-slim
ARG SOURCE_SHA=unknown

RUN apt-get update \
    && apt-get install --no-install-recommends -y iproute2 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin chirps

COPY --from=build /src/target/release/examples/two_node_transfer /usr/local/bin/two_node_transfer

LABEL org.opencontainers.image.revision="${SOURCE_SHA}"
USER chirps
