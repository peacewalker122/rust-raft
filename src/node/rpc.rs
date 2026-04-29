use std::sync::Arc;

use tonic::{Request, Response, Status, transport::Endpoint};

use crate::node::{error::NodeError, node::RaftNode};

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    AppendEntriesRequest as ProtoAppendEntriesRequest,
    AppendEntriesResponse as ProtoAppendEntriesResponse,
    InstallSnapshotRequest as ProtoInstallSnapshotRequest,
    InstallSnapshotResponse as ProtoInstallSnapshotResponse,
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
            // Persist term before responding (Raft spec: must persist before replying)
            {
                let mut node = self.node.write().await;
                node.set_term(term)
                    .await
                    .map_err(|err| Status::internal(format!("failed to persist term: {err}")))?;
            }
        }

        // Reject if candidate's term is staleand the request if th
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
                .set_voted_for(Some(req.candidate_id.clone()))
                .await
                .map_err(|err| Status::internal(format!("failed to update voted_for: {err}")))?;

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

    async fn append_entries_svc(
        &self,
        request: Request<ProtoAppendEntriesRequest>,
    ) -> Result<Response<ProtoAppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let (mut term, node_log_len) = {
            let node = self.node.read().await;
            (node.get_term(), node.log.len() as u64)
        };

        // Step 1: Reply false if term < currentTerm
        if req.term < term {
            return Ok(Response::new(ProtoAppendEntriesResponse {
                success: false,
                term,
            }));
        }

        // Step 2: Update term and become follower if leader's term > currentTerm
        if req.term > term {
            term = req.term;
            let mut node = self.node.write().await;
            node.become_follower(term);
        }

        // Step 3: Check log matching - reject if no entry at prevLogIndex with prevLogTerm
        let prev_log_matches = if req.prev_log_index == 0 {
            req.prev_log_term == 0
        } else if req.prev_log_index > node_log_len {
            false
        } else {
            let node = self.node.read().await;
            let entry_at_prev = node.log.get((req.prev_log_index - 1) as usize);
            match entry_at_prev {
                Some(entry) => entry.term == req.prev_log_term,
                None => false,
            }
        };

        if !prev_log_matches {
            return Ok(Response::new(ProtoAppendEntriesResponse {
                success: false,
                term,
            }));
        }

        // Step 4 & 5: Handle log consistency and append new entries
        // Remove conflicting entries and append new ones
        let mut node = self.node.write().await;
        let log_len = node.log.len() as u64;

        // Truncate log at conflict point (entries after prev_log_index)
        if req.prev_log_index < log_len {
            node.log.truncate(req.prev_log_index as usize);
        }

        // Append new entries not already in log
        for proto_entry in req.entries {
            let new_entry = crate::log::log::LogEntry::new(proto_entry.term, proto_entry.command);
            node.log.push(new_entry);
        }

        // Step 6: Update commit index if leader committed more
        let current_commit = node.get_commit_index();
        let new_commit_index = req.leader_commit.min(node.log.len() as u64);
        if new_commit_index > current_commit {
            node.set_commit_index(new_commit_index);
        }

        // Persist state before responding (Raft spec: must persist before replying)
        node.persist()
            .await
            .map_err(|err| Status::internal(format!("failed to persist log: {err}")))?;

        Ok(Response::new(ProtoAppendEntriesResponse {
            success: true,
            term,
        }))
    }

    async fn install_snapshot_svc(
        &self,
        request: Request<ProtoInstallSnapshotRequest>,
    ) -> Result<Response<ProtoInstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let mut term = {
            let node = self.node.read().await;
            node.get_term()
        };

        // Step 1: Reply false if term < currentTerm
        if req.term < term {
            return Ok(Response::new(ProtoInstallSnapshotResponse { term }));
        }

        // Step 2: Update term and become follower if leader's term > currentTerm.
        // Snapshot payload application is intentionally deferred in this PoC.
        if req.term > term {
            term = req.term;
            let mut node = self.node.write().await;
            node.become_follower(term);
        }

        Ok(Response::new(ProtoInstallSnapshotResponse { term }))
    }
}

// server implementation
#[tonic::async_trait]
impl RaftRpc for NodeRpcService {
    // For simplicity, just return success=true for now
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
        request: Request<ProtoAppendEntriesRequest>,
    ) -> Result<Response<ProtoAppendEntriesResponse>, Status> {
        println!(
            "Received AppendEntries from {}",
            request.get_ref().leader_id
        );

        self.append_entries_svc(request).await
    }

    async fn install_snapshot(
        &self,
        request: Request<ProtoInstallSnapshotRequest>,
    ) -> Result<Response<ProtoInstallSnapshotResponse>, Status> {
        println!(
            "Received InstallSnapshot from {}",
            request.get_ref().leader_id
        );

        self.install_snapshot_svc(request).await
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

pub async fn send_install_snapshot(
    target: &str,
    term: u64,
    leader_id: &str,
    last_included_index: u64,
    last_included_term: u64,
    data: Vec<u8>,
) -> Result<Response<ProtoInstallSnapshotResponse>, NodeError> {
    let endpoint = normalize_target_uri(target)?;
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    let mut client = RaftRpcClient::new(channel);

    let res = client
        .install_snapshot(Request::new(ProtoInstallSnapshotRequest {
            term,
            leader_id: leader_id.to_string(),
            last_included_index,
            last_included_term,
            data,
        }))
        .await
        .map_err(|e| NodeError::InstallSnapshotFailed(e.to_string()))?;

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
        proto::{
            AppendEntriesRequest as ProtoAppendEntriesRequest,
            InstallSnapshotRequest as ProtoInstallSnapshotRequest,
            LogEntry as ProtoLogEntry,
        },
    };
    use crate::node::{error::NodeError, node::RaftNode};
    use crate::storage::MockStore;

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
        let mut node = RaftNode::new("node-1".to_string(), vec![], Box::new(MockStore::new()));
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
        let node = Arc::new(RwLock::new(RaftNode::new(
            "node-1".to_string(),
            vec![],
            Box::new(MockStore::new()),
        )));
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
        let mut node = RaftNode::new("node-1".to_string(), vec![], Box::new(MockStore::new()));
        node.become_follower(1);

        node.set_voted_for(Some("other-candidate".to_string()))
            .await
            .expect("should set voted_for");

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
        let mut node = RaftNode::new("node-1".to_string(), vec![], Box::new(MockStore::new()));
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
        let node = Arc::new(RwLock::new(RaftNode::new(
            "node-1".to_string(),
            vec![],
            Box::new(MockStore::new()),
        )));
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

    #[tokio::test]
    async fn append_entries_rejects_stale_term() {
        // Node has term 3, leader has term 2
        let mut node = RaftNode::new("node-1".to_string(), vec![], Box::new(MockStore::new()));
        node.become_follower(3);
        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node);

        let response = service
            .append_entries(Request::new(ProtoAppendEntriesRequest {
                term: 2,
                leader_id: "leader-1".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            }))
            .await
            .expect("should succeed");

        let payload = response.into_inner();
        assert!(!payload.success);
        assert_eq!(payload.term, 3);
    }

    #[tokio::test]
    async fn append_entries_updates_term_and_becomes_follower() {
        // Leader has higher term, node should update and become follower
        let node = Arc::new(RwLock::new(RaftNode::new(
            "node-1".to_string(),
            vec![],
            Box::new(MockStore::new()),
        )));
        let service = NodeRpcService::new(node);

        let response = service
            .append_entries(Request::new(ProtoAppendEntriesRequest {
                term: 5,
                leader_id: "leader-1".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            }))
            .await
            .expect("should succeed");

        let payload = response.into_inner();
        assert!(payload.success);
        assert_eq!(payload.term, 5);
    }

    #[tokio::test]
    async fn append_entries_appends_entries_to_log() {
        let node = Arc::new(RwLock::new(RaftNode::new(
            "node-1".to_string(),
            vec![],
            Box::new(MockStore::new()),
        )));
        let service = NodeRpcService::new(node.clone());

        let response = service
            .append_entries(Request::new(ProtoAppendEntriesRequest {
                term: 1,
                leader_id: "leader-1".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    ProtoLogEntry {
                        term: 1,
                        index: 1,
                        command: b"command1".to_vec(),
                    },
                    ProtoLogEntry {
                        term: 1,
                        index: 2,
                        command: b"command2".to_vec(),
                    },
                ],
                leader_commit: 0,
            }))
            .await
            .expect("should succeed");

        assert!(response.into_inner().success);
        let node = node.read().await;
        assert_eq!(node.log.len(), 2);
    }

    #[tokio::test]
    async fn append_entries_rejects_mismatched_prev_log() {
        // Node has entry at index 1 with term 1, but leader says prev_log_term 2
        let mut node = RaftNode::new("node-1".to_string(), vec![], Box::new(MockStore::new()));
        node.push_log(crate::log::log::LogEntry::new(1, b"old cmd"))
            .await
            .expect("should push log");
        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node.clone());

        let response = service
            .append_entries(Request::new(ProtoAppendEntriesRequest {
                term: 1,
                leader_id: "leader-1".to_string(),
                prev_log_index: 1,
                prev_log_term: 2, // Mismatched - entry has term 1
                entries: vec![ProtoLogEntry {
                    term: 1,
                    index: 2,
                    command: b"new cmd".to_vec(),
                }],
                leader_commit: 0,
            }))
            .await
            .expect("should succeed");

        let payload = response.into_inner();
        assert!(!payload.success);
        // Log should not have been appended
        let node = node.read().await;
        assert_eq!(node.log.len(), 1);
    }

    #[tokio::test]
    async fn append_entries_truncates_conflicting_entries() {
        // Node has entries [1, 2], leader wants to replace from index 1
        let mut node = RaftNode::new("node-1".to_string(), vec![], Box::new(MockStore::new()));
        node.push_log(crate::log::log::LogEntry::new(1, b"cmd1"))
            .await
            .expect("should push log");
        node.push_log(crate::log::log::LogEntry::new(1, b"cmd2"))
            .await
            .expect("should push log");
        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node.clone());

        let response = service
            .append_entries(Request::new(ProtoAppendEntriesRequest {
                term: 2,
                leader_id: "leader-1".to_string(),
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![ProtoLogEntry {
                    term: 2,
                    index: 2,
                    command: b"new_cmd2".to_vec(),
                }],
                leader_commit: 0,
            }))
            .await
            .expect("should succeed");

        assert!(response.into_inner().success);
        let node = node.read().await;
        assert_eq!(node.log.len(), 2);
        assert_eq!(node.log[1].term, 2); // Replaced with new term
    }

    #[tokio::test]
    async fn append_entries_updates_commit_index() {
        let node = Arc::new(RwLock::new(RaftNode::new(
            "node-1".to_string(),
            vec![],
            Box::new(MockStore::new()),
        )));
        let service = NodeRpcService::new(node.clone());

        service
            .append_entries(Request::new(ProtoAppendEntriesRequest {
                term: 1,
                leader_id: "leader-1".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    ProtoLogEntry {
                        term: 1,
                        index: 1,
                        command: b"cmd1".to_vec(),
                    },
                    ProtoLogEntry {
                        term: 1,
                        index: 2,
                        command: b"cmd2".to_vec(),
                    },
                ],
                leader_commit: 2, // Leader committed up to index 2
            }))
            .await
            .expect("should succeed");

        let node = node.read().await;
        assert_eq!(node.get_commit_index(), 2);
    }

    #[tokio::test]
    async fn install_snapshot_rejects_stale_term() {
        let mut node = RaftNode::new("node-1".to_string(), vec![], Box::new(MockStore::new()));
        node.become_follower(3);
        let node = Arc::new(RwLock::new(node));
        let service = NodeRpcService::new(node);

        let response = service
            .install_snapshot(Request::new(ProtoInstallSnapshotRequest {
                term: 2,
                leader_id: "leader-1".to_string(),
                last_included_index: 0,
                last_included_term: 0,
                data: vec![],
            }))
            .await
            .expect("should succeed");

        assert_eq!(response.into_inner().term, 3);
    }

    #[tokio::test]
    async fn install_snapshot_updates_term_when_higher() {
        let node = Arc::new(RwLock::new(RaftNode::new(
            "node-1".to_string(),
            vec![],
            Box::new(MockStore::new()),
        )));
        let service = NodeRpcService::new(node.clone());

        let response = service
            .install_snapshot(Request::new(ProtoInstallSnapshotRequest {
                term: 7,
                leader_id: "leader-1".to_string(),
                last_included_index: 10,
                last_included_term: 2,
                data: b"snapshot-bytes".to_vec(),
            }))
            .await
            .expect("should succeed");

        assert_eq!(response.into_inner().term, 7);
        assert_eq!(node.read().await.get_term(), 7);
    }
}
