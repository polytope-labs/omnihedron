FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Binary must be built on the host first:
#   cargo build --release --bin omnihedron
COPY target/release/omnihedron /usr/local/bin/omnihedron

EXPOSE 3000
ENTRYPOINT ["omnihedron"]
