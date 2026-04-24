use std::{io, net::SocketAddr, sync::Arc};

use rust_raft::{
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
    let node_id = std::env::var("RAFT_NODE_ID").unwrap_or_else(|_| "node-1".to_string());
    let peers = std::env::var("RAFT_PEERS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|peer| !peer.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    let bind_addr_raw =
        std::env::var("RAFT_GRPC_BIND").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let bind_addr = parse_bind_addr(&bind_addr_raw)?;

    let term_file_path =
        std::env::var("RAFT_TERM_FILE").unwrap_or_else(|_| "raft_term.dat".to_string());
    let term_file = tokio::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(term_file_path)
        .await?;

    let log_file_path =
        std::env::var("RAFT_LOG_FILE").unwrap_or_else(|_| "raft_log.dat".to_string());
    let log_file = tokio::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(log_file_path)
        .await?;

    let storage = Box::new(PersistentStore::new(log_file, term_file));

    let shared_node = Arc::new(tokio::sync::RwLock::new(RaftNode::new(
        node_id, peers, storage,
    )));
    let scheduler = NodeScheduler::new(shared_node.clone());

    tokio::spawn(async move {
        scheduler.start().await;
    });

    println!(
        "Starting RAFT node with ID '{}' on '{}'",
        shared_node.read().await.get_id(),
        bind_addr
    );

    Server::builder()
        .add_service(RaftRpcServer::new(NodeRpcService::new(shared_node)))
        .serve(bind_addr)
        .await?;

    Ok(())
}

fn parse_bind_addr(raw_addr: &str) -> Result<SocketAddr, io::Error> {
    raw_addr.parse().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid RAFT_GRPC_BIND '{raw_addr}': {e}"),
        )
    })
}
