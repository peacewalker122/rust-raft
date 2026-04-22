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
    raft_rpc_client::RaftRpcClient, raft_rpc_server::RaftRpc, raft_rpc_server::RaftRpcServer,
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
        if request.get_ref().term < self.node.read().await.get_term() {
            return Ok(Response::new(ProtoRequestVoteResponse { success: false }));
        }

        let node = self.node.read().await;
        let voted_for = node.get_voted_for();

        if voted_for.is_some() && voted_for.as_deref() != Some(&request.get_ref().candidate_id) {
            return Ok(Response::new(ProtoRequestVoteResponse { success: false }));
        }

        self.node
            .write()
            .await
            .set_voted_for(Some(request.get_ref().candidate_id.clone()));

        Ok(Response::new(ProtoRequestVoteResponse { success: true }))
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
) -> Result<(), NodeError> {
    let endpoint = normalize_target_uri(target)?;
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    let mut client = RaftRpcClient::new(channel);

    client
        .request_vote(Request::new(ProtoRequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
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
