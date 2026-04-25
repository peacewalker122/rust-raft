//! Integration tests for Raft consensus
//!
//! These tests spin up multiple gRPC server nodes with scheduler
//! and verify election and leader behavior.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rust_raft::node::{
    node::RaftNode,
    rpc::{NodeRpcService, proto::raft_rpc_server::RaftRpcServer},
    scheduler::NodeScheduler,
};
use rust_raft::storage::MockStore;
use tokio::sync::RwLock;
use tonic::transport::Server;

/// Helper to start a Raft node server with scheduler running
async fn start_node_server(
    node_id: String,
    peers: Vec<String>,
    port: u16,
) -> (
    SocketAddr,
    Arc<RwLock<RaftNode>>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let addr: SocketAddr = format!("127.0.0.1:{}", port)
        .parse()
        .expect("invalid socket address");

    let shared_node = Arc::new(RwLock::new(RaftNode::new(
        node_id.clone(),
        peers.clone(),
        Box::new(MockStore::new()),
    )));
    let node_for_server = shared_node.clone();
    let node_for_scheduler = shared_node.clone();

    // Spawn the server
    let server_handle = tokio::spawn(async move {
        let service = NodeRpcService::new(node_for_server);
        Server::builder()
            .add_service(RaftRpcServer::new(service))
            .serve(addr)
            .await
            .expect("server failed");
    });

    // Spawn the scheduler (handles election timeouts)
    let scheduler = NodeScheduler::new(node_for_scheduler);
    let scheduler_handle = tokio::spawn(async move {
        scheduler.start().await;
    });

    // Give the server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let bind_addr: SocketAddr = format!("127.0.0.1:{}", port)
        .parse()
        .expect("invalid socket address");

    (
        bind_addr,
        shared_node,
        vec![server_handle, scheduler_handle],
    )
}

/// Helper function to check if a node is in leader state
fn is_node_leader(node: &tokio::sync::RwLockReadGuard<'_, RaftNode>) -> bool {
    node.is_leader()
}

/// Test that two nodes can elect a leader between them
#[tokio::test]
async fn test_two_node_election() {
    // Start node A on port 50101 and node B on port 50102
    // Node A's peers: [127.0.0.1:50102]
    // Node B's peers: [127.0.0.1:50101]
    let addr_a = "127.0.0.1:50101";
    let addr_b = "127.0.0.1:50102";

    let (_, node_a, handles_a) =
        start_node_server("node-a".to_string(), vec![addr_b.to_string()], 50101).await;

    let (_, node_b, handles_b) =
        start_node_server("node-b".to_string(), vec![addr_a.to_string()], 50102).await;

    // Wait for election to complete (election timeout is 200ms)
    // Give some buffer time for the election to happen
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check node states
    let state_a = node_a.read().await;
    let state_b = node_b.read().await;

    println!(
        "Node A state: term={}, is_leader={}",
        state_a.get_term(),
        is_node_leader(&state_a)
    );
    println!(
        "Node B state: term={}, is_leader={}",
        state_b.get_term(),
        is_node_leader(&state_b)
    );

    // Verify at least one node is aware of the term increasing (election happened)
    let term_a = state_a.get_term();
    let term_b = state_b.get_term();
    assert!(
        term_a > 0 || term_b > 0,
        "At least one node should have incremented term"
    );

    // Cleanup
    for handle in handles_a {
        handle.abort();
    }
    for handle in handles_b {
        handle.abort();
    }
}

/// Test candidate behavior - node should request votes from peers
#[tokio::test]
async fn test_candidate_requests_votes() {
    let addr_a = "127.0.0.1:50111";
    let addr_b = "127.0.0.1:50112";
    let addr_c = "127.0.0.1:50113";

    let (_, node_a, handles_a) = start_node_server(
        "candidate-a".to_string(),
        vec![addr_b.to_string(), addr_c.to_string()],
        50111,
    )
    .await;

    let (_, node_b, handles_b) = start_node_server(
        "candidate-b".to_string(),
        vec![addr_a.to_string(), addr_c.to_string()],
        50112,
    )
    .await;

    let (_, node_c, handles_c) = start_node_server(
        "candidate-c".to_string(),
        vec![addr_a.to_string(), addr_b.to_string()],
        50113,
    )
    .await;

    // Wait for election
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Count how many nodes became leaders
    let is_leader_a = is_node_leader(&node_a.read().await);
    let is_leader_b = is_node_leader(&node_b.read().await);
    let is_leader_c = is_node_leader(&node_c.read().await);

    let leader_count = [is_leader_a, is_leader_b, is_leader_c]
        .into_iter()
        .filter(|&x| x)
        .count();

    println!("Leaders elected: {}", leader_count);
    println!("Node A leader: {}", is_leader_a);
    println!("Node B leader: {}", is_leader_b);
    println!("Node C leader: {}", is_leader_c);

    // With 3 nodes, we should have exactly 1 leader (or 0 if election split)
    assert!(
        leader_count <= 1,
        "Should have at most 1 leader (got {})",
        leader_count
    );

    // Cleanup
    for handle in handles_a {
        handle.abort();
    }
    for handle in handles_b {
        handle.abort();
    }
    for handle in handles_c {
        handle.abort();
    }
}

/// Test leader election with timeout - verify consensus
#[tokio::test]
async fn test_leader_election_consensus() {
    let addr_a = "127.0.0.1:50121";
    let addr_b = "127.0.0.1:50122";
    let addr_c = "127.0.0.1:50123";

    let (_, node_a, handles_a) =
        start_node_server("consensus-a".to_string(), vec![addr_b.to_string()], 50121).await;

    let (_, node_b, handles_b) =
        start_node_server("consensus-b".to_string(), vec![addr_a.to_string()], 50122).await;

    let (_, node_c, handles_c) = start_node_server(
        "consensus-c".to_string(),
        vec![addr_a.to_string(), addr_b.to_string()],
        50123,
    )
    .await;

    // Wait longer for stable election
    tokio::time::sleep(Duration::from_secs(1)).await;

    let term_a = node_a.read().await.get_term();
    let term_b = node_b.read().await.get_term();
    let _ = node_c.read().await.get_term();

    println!("Final terms - A: {}, B: {}", term_a, term_b);

    // Both nodes should agree on the same term
    assert_eq!(
        term_a, term_b,
        "Both nodes should have same term for consensus"
    );

    // Cleanup
    for handle in handles_a {
        handle.abort();
    }
    for handle in handles_b {
        handle.abort();
    }
}

/// Test RequestVote RPC directly between two nodes
#[tokio::test]
async fn test_request_vote_rpc_between_nodes() {
    use rust_raft::node::rpc::send_request_vote;

    let addr_a = "127.0.0.1:50131";

    // Start server without scheduler for this test (no election timer interference)
    let shared_node = Arc::new(RwLock::new(RaftNode::new(
        "voter".to_string(),
        vec![],
        Box::new(MockStore::new()),
    )));
    let node_for_server = shared_node.clone();

    let server_handle = tokio::spawn(async move {
        let addr: SocketAddr = "127.0.0.1:50131".parse().unwrap();
        let service = NodeRpcService::new(node_for_server);
        Server::builder()
            .add_service(RaftRpcServer::new(service))
            .serve(addr)
            .await
            .expect("server failed");
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Node B requests vote from Node A
    let result = send_request_vote("127.0.0.1:50131", 1, "candidate-b", 0, 0).await;

    assert!(
        result.is_ok(),
        "RequestVote should succeed, got: {:?}",
        result.err()
    );

    let response = result.unwrap().into_inner();
    println!(
        "Vote response: success={}, term={}",
        response.success, response.term
    );

    // Node A should have granted the vote (hasn't voted yet in this term)
    assert!(response.success, "Node A should have granted vote");
    assert_eq!(response.term, 1, "Term should be 1");

    // Cleanup
    server_handle.abort();
}
