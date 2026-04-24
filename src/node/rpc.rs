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

    async fn request_vote_svc(
        &self,
        request: Request<ProtoRequestVoteRequest>,
    ) -> Result<Response<ProtoRequestVoteResponse>, Status> {
        // Read values first, drop guard, then write
        let (mut term, voted_for, node_last_log_index) = {
            let node = self.node.read().await;
            (
                node.get_term(),
                node.get_voted_for().cloned(),
                node.last_log_index().unwrap_or(0),
            )
        };

        let req = request.get_ref();

        // Raft spec: if candidate's term > current_term, update term
        if req.term > term {
            term = req.term;
        }

        // Reject if candidate's term is stale
        if req.term < term {
            return Ok(Response::new(ProtoRequestVoteResponse {
                success: false,
                term,
            }));
        }

        // Raft spec: grant vote if (votedFor == null OR votedFor == candidateId)
        // Also check candidate's log is at least as up-to-date
        let already_voted_for_different = voted_for
            .as_ref()
            .map(|v| v.as_str() != req.candidate_id.as_str())
            .unwrap_or(false);

        // Check log up-to-date: candidate's last index >= node's last index
        let log_ok = req.last_log_index >= node_last_log_index;

        // Grant vote only if Haven't voted for different AND log is ok
        if !already_voted_for_different && log_ok {
            self.node
                .write()
                .await
                .set_voted_for(Some(req.candidate_id.clone()));

            return Ok(Response::new(ProtoRequestVoteResponse {
                success: true,
                term,
            }));
        }

        Ok(Response::new(ProtoRequestVoteResponse {
            success: false,
            term,
        }))
    }
}

// server implementation
#[tonic::async_trait]
impl RaftRpc for NodeRpcService {
    async fn request_vote(
        &self,
        request: Request<ProtoRequestVoteRequest>,
    ) -> Result<Response<ProtoRequestVoteResponse>, Status> {
        println!(
            "Received RequestVote from {}",
            request.get_ref().candidate_id
        );

        return self.request_vote_svc(request).await;
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
) -> Result<Response<ProtoRequestVoteResponse>, NodeError> {
    let endpoint = normalize_target_uri(target)?;
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    let mut client = RaftRpcClient::new(channel);

    let res = client
        .request_vote(Request::new(ProtoRequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
            last_log_index,
            last_log_term,
        }))
        .await
        .map_err(|e| NodeError::RequestVoteFailed(e.to_string()))?;

    Ok(res)
}

pub async fn send_append_entries(
    target: &str,
    term: u64,
    leader_id: &str,
) -> Result<Response<ProtoAppendEntriesResponse>, NodeError> {
    let endpoint = normalize_target_uri(target)?;
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    let mut client = RaftRpcClient::new(channel);

    let res = client
        .append_entries(Request::new(ProtoAppendEntriesRequest {
            term,
            leader_id: leader_id.to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        }))
        .await
        .map_err(|e| NodeError::AppendEntriesFailed(e.to_string()))?;

    Ok(res)
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
        let target = "   ";
        let result = normalize_target_uri(target);
        assert!(matches!(
            result,
            Err(NodeError::InvalidPeerTarget(value)) if value == "   "
        ));
    }

    #[test]
    fn normalize_target_uri_keeps_existing_scheme() {
        let target = "https://127.0.0.1:50051";
        let result = normalize_target_uri(target).expect("normalize should succeed");
        assert_eq!(result, "https://127.0.0.1:50051");
    }

    #[test]
    fn normalize_target_uri_adds_http_scheme_when_missing() {
        let target = "127.0.0.1:50051";
        let result = normalize_target_uri(target).expect("normalize should succeed");
        assert_eq!(result, "http://127.0.0.1:50051");
    }

    // Tests for request_vote_svc logic (no network involved)

    #[tokio::test]
    async fn request_vote_svc_rejects_stale_term() {
        // Node has term 3, candidate has term 2
        let mut node = RaftNode::new("node-1".to_string(), vec![]);
        node.become_follower(3);
        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node);

        let response = service
            .request_vote(Request::new(ProtoRequestVoteRequest {
                term: 2,
                candidate_id: "candidate-1".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            }))
            .await
            .expect("should succeed");

        let payload = response.into_inner();
        assert!(!payload.success);
        assert_eq!(payload.term, 3);
    }

    #[tokio::test]
    async fn request_vote_svc_grants_vote_when_not_voted() {
        // Node has not voted yet, term matches
        let node = Arc::new(RwLock::new(RaftNode::new("node-1".to_string(), vec![])));
        let service = NodeRpcService::new(node.clone());

        let response = service
            .request_vote(Request::new(ProtoRequestVoteRequest {
                term: 0,
                candidate_id: "candidate-1".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            }))
            .await
            .expect("should succeed");

        let payload = response.into_inner();
        assert!(payload.success);
        assert_eq!(payload.term, 0);
        assert_eq!(
            node.read().await.get_voted_for().map(|v| v.as_str()),
            Some("candidate-1")
        );
    }

    #[tokio::test]
    async fn request_vote_svc_rejects_already_voted_different_candidate() {
        // Node already voted for different candidate
        let mut node = RaftNode::new("node-1".to_string(), vec![]);
        node.become_follower(1);
        node.set_voted_for(Some("other-candidate".to_string()));

        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node.clone());

        let response = service
            .request_vote(Request::new(ProtoRequestVoteRequest {
                term: 1,
                candidate_id: "new-candidate".to_string(),
                last_log_index: 0,
                last_log_term: 0,
            }))
            .await
            .expect("should succeed");

        let payload = response.into_inner();
        assert!(!payload.success);
        assert_eq!(payload.term, 1);
        // voted_for unchanged
        assert_eq!(
            node.read().await.get_voted_for().map(|v| v.as_str()),
            Some("other-candidate")
        );
    }

    #[tokio::test]
    async fn request_vote_svc_accepts_same_candidate_again() {
        // Node already voted for same candidate - should still grant
        let mut node = RaftNode::new("node-1".to_string(), vec![]);
        node.become_follower(1);
        node.set_voted_for(Some("same-candidate".to_string()));

        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node.clone());

        let response = service
            .request_vote(Request::new(ProtoRequestVoteRequest {
                term: 2, // higher term
                candidate_id: "same-candidate".to_string(),
                last_log_index: 5,
                last_log_term: 1,
            }))
            .await
            .expect("should succeed");

        let payload = response.into_inner();
        assert!(payload.success);
        assert_eq!(payload.term, 2);
    }

    #[tokio::test]
    async fn append_entries_returns_success_true() {
        let node = Arc::new(RwLock::new(RaftNode::new("node-1".to_string(), vec![])));
        let service = NodeRpcService::new(node);

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
            .expect("should succeed");

        assert!(response.into_inner().success);
    }
}
