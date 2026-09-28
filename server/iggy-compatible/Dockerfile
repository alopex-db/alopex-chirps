FROM --platform=linux/amd64 docker.io/library/debian@sha256:38a76d01668772e381ad2826d876627c89e7133e2f8a0f5d567306798b0f2a16

LABEL org.opencontainers.image.source="https://github.com/apache/iggy" \
      org.opencontainers.image.revision="76dcccf24b27c61a9434a79b3f81f065c4d3a832" \
      org.opencontainers.image.title="Chirps compatible Iggy server" \
      org.opencontainers.image.version="0.7.0" \
      org.alopex.chirps.artifact-kind="production" \
      org.alopex.chirps.failpoints="disabled" \
org.alopex.chirps.server-sha256="254865eb345fd2c095513e95c449d183bef9801f472f6dd50310a2d2421e297c"

# The Debian snapshot fixes the only runtime packages not present in slim.
RUN sed -i \
      -e 's|http://deb.debian.org/debian-security|https://snapshot.debian.org/archive/debian-security/20260810T000000Z|g' \
      -e 's|http://deb.debian.org/debian|https://snapshot.debian.org/archive/debian/20260810T000000Z|g' \
      -e 's|https://deb.debian.org/debian-security|https://snapshot.debian.org/archive/debian-security/20260810T000000Z|g' \
      -e 's|https://deb.debian.org/debian|https://snapshot.debian.org/archive/debian/20260810T000000Z|g' \
      /etc/apt/sources.list.d/debian.sources \
    && printf 'Acquire::Check-Valid-Until "false";\n' >/etc/apt/apt.conf.d/99snapshot \
    && apt-get update \
    && apt-get install --yes --no-install-recommends libhwloc15 libudev1 \
    && rm -rf /var/lib/apt/lists/*

COPY --chmod=0755 iggy-server /usr/local/bin/iggy-server

ENTRYPOINT ["/usr/local/bin/iggy-server"]
