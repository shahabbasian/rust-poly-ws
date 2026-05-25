# Stage 1: Build
FROM rust:1.82-slim AS builder

WORKDIR /app

# Install dependencies for openssl
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml ./
COPY src ./src

RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/polymarket-fetcher /app/polymarket-fetcher

ENV RUST_LOG=info

ENTRYPOINT ["/app/polymarket-fetcher"]
