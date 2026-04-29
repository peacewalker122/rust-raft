FROM rust:1.94-slim AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src

RUN apt-get update && apt-get install -y \
  musl-tools \
  pkg-config \
  protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

RUN cargo build --release

# ---- runtime ----
FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/target/release/rust-raft /app/rust-raft

ENV RAFT_GRPC_BIND=0.0.0.0:50051

EXPOSE 50051

USER nonroot:nonroot

ENTRYPOINT ["/app/rust-raft"]
