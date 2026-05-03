use std::sync::Arc;

use rust_raft::{
    config::RaftConfig,
    logging,
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
    // Initialize structured logging
    logging::init();

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
    let (event_sender, _event_receiver) = tokio::sync::mpsc::channel(100);

    let shared_node = Arc::new(tokio::sync::RwLock::new(RaftNode::new(
        config.node_id,
        config.peers,
        storage,
        event_sender.clone(),
    )));
    let (scheduler_tx, scheduler_rx) = tokio::sync::mpsc::channel(100);
    let mut scheduler = NodeScheduler::new(shared_node.clone(), scheduler_rx);

    tokio::spawn(async move {
        scheduler.start().await;
    });

    println!(
        "Starting RAFT node with ID '{}' on '{}'",
        shared_node.read().await.get_id(),
        config.grpc_bind
    );

    Server::builder()
        .add_service(RaftRpcServer::new(NodeRpcService::new(
            shared_node,
            scheduler_tx,
            event_sender,
        )))
        .serve(config.grpc_bind)
        .await?;

    Ok(())
}
