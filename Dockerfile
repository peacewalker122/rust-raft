FROM rust:1.79-slim AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/target/release/rust-raft /app/rust-raft

ENV RAFT_GRPC_BIND=0.0.0.0:50051

EXPOSE 50051

ENTRYPOINT ["/app/rust-raft"]
