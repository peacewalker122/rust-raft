use std::{io, net::SocketAddr, sync::Arc};

use rust_raft::node::{
    node::RaftNode,
    rpc::{NodeRpcService, proto::raft_rpc_server::RaftRpcServer},
    scheduler::NodeScheduler,
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

    let shared_node = Arc::new(tokio::sync::RwLock::new(RaftNode::new(node_id, peers)));
    let scheduler = NodeScheduler::new(shared_node.clone());

    tokio::spawn(async move {
        scheduler.start().await;
    });

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
