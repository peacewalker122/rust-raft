# rust-raft (PoC)

rust-raft is a proof-of-concept implementation of the Raft consensus algorithm in Rust. It focuses on learning and validating core Raft ideas—leader election, log replication scaffolding, and crash-safe persistence—rather than production readiness. This repository exists to explore how a Raft node can be modeled with async Rust and gRPC-based RPCs.

## What this project is for

- **Learning Raft by building it**: the codebase is intentionally small and readable to make the core mechanics easy to follow.
- **Exploring Rust + async patterns**: uses Tokio for async runtime and Tonic for gRPC RPCs.
- **Experimentation and iteration**: designed to evolve quickly while verifying assumptions.

This is **not** a complete or production-ready Raft implementation. Expect incomplete features, breaking changes, and rough edges.

## Architecture overview

At a high level, each node:

- Maintains persistent state using a simple file-backed store (term + log)
- Runs an async scheduler for time-based Raft behavior
- Serves gRPC endpoints for Raft RPCs

Key modules:

- `src/node/*`: Raft node state, RPC handlers, and scheduler
- `src/storage/*`: Persistent storage API and implementation
- `src/log/*`: Log structures and helpers

## Current capabilities (PoC)

- Single-node process boot with gRPC server
- Basic persistent state wiring
- Foundation for Raft scheduling and RPC wiring

## Limitations

- Not a full Raft implementation
- No production hardening, security, or stability guarantees
- APIs and behavior may change without notice

## Getting Started

### Prerequisites

- Rust toolchain (stable)

### Build

```bash
cargo build
```

### Run

By default the node binds on `127.0.0.1:50051`.

```bash
cargo run
```

### Test

```bash
cargo test
```

## Configuration

The binary can be configured via environment variables:

- `RAFT_NODE_ID`: unique node identifier (default: `node-1`)
- `RAFT_PEERS`: comma-separated peer IDs (default: empty)
- `RAFT_GRPC_BIND`: gRPC bind address (default: `127.0.0.1:50051`)
- `RAFT_TERM_FILE`: term storage file (default: `raft_term.dat`)
- `RAFT_LOG_FILE`: log storage file (default: `raft_log.dat`)

Example:

```bash
RAFT_NODE_ID=node-1 \
RAFT_PEERS=node-2,node-3 \
RAFT_GRPC_BIND=127.0.0.1:50051 \
cargo run
```

## Using as a library (PoC)

You can embed this crate as a Raft consensus component in another Rust service. This is still a PoC, but the library provides the core building blocks:

- `RaftNode` for node state and transitions
- `NodeScheduler` for election/heartbeat scheduling
- `NodeRpcService` for gRPC-based Raft RPCs
- `PersistentStore` for basic crash-safe state persistence

### Add dependency

Add this crate as a path dependency (or git dependency if you publish it later):

```toml
[dependencies]
rust-raft = { path = "../rust-raft" }
```

### Minimal embed example

```rust
use std::sync::Arc;

use rust_raft::{
    config::RaftConfig,
    node::{
        node::RaftNode,
        rpc::{NodeRpcService, proto::raft_rpc_server::RaftRpcServer},
        scheduler::NodeScheduler,
    },
    storage::storage::PersistentStore,
};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RaftConfig::from_env()?;

    let term_file = tokio::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&config.term_file_path)
        .await?;

    let log_file = tokio::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&config.log_file_path)
        .await?;

    let storage = Box::new(PersistentStore::new(log_file, term_file));

    let shared_node = Arc::new(tokio::sync::RwLock::new(RaftNode::new(
        config.node_id,
        config.peers,
        storage,
    )));

    let scheduler = NodeScheduler::new(shared_node.clone());
    tokio::spawn(async move { scheduler.start().await });

    Server::builder()
        .add_service(RaftRpcServer::new(NodeRpcService::new(shared_node)))
        .serve(config.grpc_bind)
        .await?;

    Ok(())
}
```

### Notes and caveats

- There is no stable public API yet; expect breaking changes.
- Leader election is basic and not hardened.
- You will need to design your own state machine/apply layer on top.

## License

This project is licensed under the MIT License.
