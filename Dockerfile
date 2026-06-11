# Stage 1: Chef — compute dependency recipe
FROM rust:1.95-slim-bookworm AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked

# Stage 2: Planner
FROM chef AS planner
COPY Cargo.toml ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder — build dependencies (cached layer) + app
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies (this layer is cached if recipe.json hasn't changed)
RUN cargo chef cook --release --recipe-path recipe.json
# Build the application
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

# Stage 4: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/polymarket-fetcher /app/polymarket-fetcher

ENV RUST_LOG=info
ENTRYPOINT ["/app/polymarket-fetcher"]
