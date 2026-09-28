FROM --platform=linux/amd64 docker.io/library/debian@sha256:38a76d01668772e381ad2826d876627c89e7133e2f8a0f5d567306798b0f2a16

LABEL org.opencontainers.image.source="https://github.com/apache/iggy" \
      org.opencontainers.image.revision="336d20c53b4bba663c257bdc0271373cfc2f1864" \
      org.opencontainers.image.title="Chirps compatible Iggy server" \
      org.opencontainers.image.version="0.7.0" \
      org.alopex.chirps.artifact-kind="production" \
      org.alopex.chirps.failpoints="disabled" \
org.alopex.chirps.server-sha256="3840ead85e35a20c0c86b519c15b87edf2fe08a9dc866b4746915ed8cb312f88"

# The slim base lacks CA roots. Bootstrap them from the same signed snapshot,
# then require HTTPS for the runtime packages. APT signature checks remain enabled.
RUN sed -i \
      -e 's|http://deb.debian.org/debian-security|http://snapshot.debian.org/archive/debian-security/20260810T000000Z|g' \
      -e 's|http://deb.debian.org/debian|http://snapshot.debian.org/archive/debian/20260810T000000Z|g' \
      -e 's|https://deb.debian.org/debian-security|http://snapshot.debian.org/archive/debian-security/20260810T000000Z|g' \
      -e 's|https://deb.debian.org/debian|http://snapshot.debian.org/archive/debian/20260810T000000Z|g' \
      /etc/apt/sources.list.d/debian.sources \
    && printf 'Acquire::Check-Valid-Until "false";\n' >/etc/apt/apt.conf.d/99snapshot \
    && apt-get -o APT::Update::Error-Mode=any update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && sed -i 's|http://snapshot.debian.org|https://snapshot.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get -o APT::Update::Error-Mode=any update \
    && apt-get install --yes --no-install-recommends libhwloc15 libudev1 \
    && rm -rf /var/lib/apt/lists/*

COPY --chmod=0755 iggy-server /usr/local/bin/iggy-server

ENTRYPOINT ["/usr/local/bin/iggy-server"]
