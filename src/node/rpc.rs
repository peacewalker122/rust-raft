use std::net::SocketAddr;
use std::sync::Arc;

use tonic::{
    Request, Response, Status,
    transport::{Endpoint, Server},
};

use crate::node::{error::NodeError, node::RaftNode};

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    AppendEntriesRequest as ProtoAppendEntriesRequest,
    AppendEntriesResponse as ProtoAppendEntriesResponse,
    RequestVoteRequest as ProtoRequestVoteRequest, RequestVoteResponse as ProtoRequestVoteResponse,
    raft_rpc_client::RaftRpcClient, raft_rpc_server::RaftRpc,
};

pub struct NodeRpcService {
    node: Arc<tokio::sync::RwLock<RaftNode>>,
}

impl NodeRpcService {
    pub fn new(node: Arc<tokio::sync::RwLock<RaftNode>>) -> Self {
        NodeRpcService { node }
    }
}

// server implementation
#[tonic::async_trait]
impl RaftRpc for NodeRpcService {
    async fn request_vote(
        &self,
        request: Request<ProtoRequestVoteRequest>,
    ) -> Result<Response<ProtoRequestVoteResponse>, Status> {
        let node = self.node.read().await;
        let term = node.get_term();
        let voted_for = node.get_voted_for();
        let node_last_log_index = node.last_log_index().unwrap_or(0);

        if request.get_ref().term < term {
            return Ok(Response::new(ProtoRequestVoteResponse {
                success: false,
                term,
            }));
        }

        if voted_for.is_some()
            && voted_for.as_deref() != Some(&request.get_ref().candidate_id)
            && request.get_ref().last_log_index < node_last_log_index
        {
            return Ok(Response::new(ProtoRequestVoteResponse {
                success: false,
                term,
            }));
        }

        self.node
            .write()
            .await
            .set_voted_for(Some(request.get_ref().candidate_id.clone()));

        Ok(Response::new(ProtoRequestVoteResponse {
            success: true,
            term,
        }))
    }

    async fn append_entries(
        &self,
        _request: Request<ProtoAppendEntriesRequest>,
    ) -> Result<Response<ProtoAppendEntriesResponse>, Status> {
        Ok(Response::new(ProtoAppendEntriesResponse { success: true }))
    }
}

pub async fn send_request_vote(
    target: &str,
    term: u64,
    candidate_id: &str,
    last_log_index: u64,
    last_log_term: u64,
) -> Result<(), NodeError> {
    let endpoint = normalize_target_uri(target)?;
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    let mut client = RaftRpcClient::new(channel);

    client
        .request_vote(Request::new(ProtoRequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
            last_log_index,
            last_log_term,
        }))
        .await
        .map_err(|e| NodeError::RequestVoteFailed(e.to_string()))?;

    Ok(())
}

fn normalize_target_uri(target: &str) -> Result<String, NodeError> {
    if target.trim().is_empty() {
        return Err(NodeError::InvalidPeerTarget(target.to_string()));
    }

    if target.starts_with("http://") || target.starts_with("https://") {
        Ok(target.to_string())
    } else {
        Ok(format!("http://{target}"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tonic::Request;

    use super::{
        NodeRpcService, ProtoRequestVoteRequest, RaftRpc, normalize_target_uri,
        proto::AppendEntriesRequest as ProtoAppendEntriesRequest,
    };
    use crate::node::{error::NodeError, node::RaftNode};

    #[test]
    fn normalize_target_uri_rejects_empty_target() {
        // Arrange
        let target = "   ";

        // Act
        let result = normalize_target_uri(target);

        // Assert
        assert!(matches!(
            result,
            Err(NodeError::InvalidPeerTarget(value)) if value == "   "
        ));
    }

    #[test]
    fn normalize_target_uri_keeps_existing_scheme() {
        // Arrange
        let target = "https://127.0.0.1:50051";

        // Act
        let result = normalize_target_uri(target).expect("normalize should succeed");

        // Assert
        assert_eq!(result, "https://127.0.0.1:50051");
    }

    #[test]
    fn normalize_target_uri_adds_http_scheme_when_missing() {
        // Arrange
        let target = "127.0.0.1:50051";

        // Act
        let result = normalize_target_uri(target).expect("normalize should succeed");

        // Assert
        assert_eq!(result, "http://127.0.0.1:50051");
    }

    #[tokio::test]
    async fn request_vote_rejects_when_candidate_term_is_stale() {
        // Arrange
        let mut node = RaftNode::new("node-1".to_string(), vec![]);
        node.become_follower(3);
        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node.clone());

        // Act
        let response = service
            .request_vote(Request::new(ProtoRequestVoteRequest {
                term: 2,
                candidate_id: "candidate-1".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            }))
            .await
            .expect("request_vote should return response");

        // Assert
        let payload = response.into_inner();
        assert!(!payload.success);
        assert_eq!(payload.term, 3);
        assert!(node.read().await.get_voted_for().is_none());
    }

    #[tokio::test]
    async fn request_vote_grants_vote_and_sets_voted_for() {
        // Arrange
        let node = Arc::new(RwLock::new(RaftNode::new("node-1".to_string(), vec![])));
        let service = NodeRpcService::new(node.clone());

        // Act
        let response = service
            .request_vote(Request::new(ProtoRequestVoteRequest {
                term: 0,
                candidate_id: "candidate-1".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            }))
            .await
            .expect("request_vote should return response");

        // Assert
        let payload = response.into_inner();
        assert!(payload.success);
        assert_eq!(payload.term, 0);
        assert_eq!(
            node.read().await.get_voted_for().map(|v| v.as_str()),
            Some("candidate-1")
        );
    }

    #[tokio::test]
    async fn append_entries_returns_success_true() {
        // Arrange
        let node = Arc::new(RwLock::new(RaftNode::new("node-1".to_string(), vec![])));
        let service = NodeRpcService::new(node);

        // Act
        let response = service
            .append_entries(Request::new(ProtoAppendEntriesRequest {
                term: 1,
                leader_id: "leader-1".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            }))
            .await
            .expect("append_entries should return response");

        // Assert
        assert!(response.into_inner().success);
    }
}
