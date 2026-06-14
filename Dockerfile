# Build stage
# Pinned to match rust-toolchain.toml's channel (keep these two in sync when bumping).
FROM rust:1.96.0-bookworm AS builder

RUN apt-get update && apt-get install -y \
    libopus-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml core/Cargo.toml
COPY node/Cargo.toml node/Cargo.toml
COPY proto/Cargo.toml proto/Cargo.toml
COPY ui/Cargo.toml ui/Cargo.toml

# Create stub sources so cargo can resolve the workspace
RUN mkdir -p core/src node/src proto/src ui/src && \
    echo "pub fn stub(){}" > core/src/lib.rs && \
    echo "fn main(){}" > node/src/main.rs && \
    echo "" > proto/src/lib.rs && \
    echo "" > ui/src/lib.rs

# Build dependencies (cached unless Cargo.toml changes)
RUN cargo build --release -p OpenBroadcastNetwork-node 2>/dev/null || true

# Copy real source code
COPY core/ core/
COPY node/ node/
COPY proto/ proto/
COPY ui/ ui/

# Touch source files so cargo rebuilds them (not the cached deps)
RUN touch core/src/lib.rs node/src/main.rs proto/src/lib.rs

# Build the actual binary
RUN cargo build --release -p OpenBroadcastNetwork-node

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libopus0 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/relay-node /app/obn-node
COPY web_viewer/ /app/web_viewer/

EXPOSE 8080 9000

ENTRYPOINT ["/app/obn-node"]
