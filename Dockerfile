FROM rust:1.94-slim AS base

RUN apt-get update && apt-get install -y \
  pkg-config \
  protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked

WORKDIR /app

FROM base AS planner
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
COPY --from=planner /app/recipe.json /app/recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/app/target \
  cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/app/target \
  cargo build --release --bin kv && cp /app/target/release/kv /app/kv

# ---- runtime ----
FROM gcr.io/distroless/cc-debian12

WORKDIR /app

COPY --from=builder /app/kv /app/kv

ENV RAFT_GRPC_BIND=0.0.0.0:50051

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/app/kv"]
