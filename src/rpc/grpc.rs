use tonic::{Request, transport::Endpoint};

use crate::{
    log::log::LogEntry,
    rpc::rpc::{AppendEntriesRequest, Rpc},
};

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    AppendEntriesRequest as ProtoAppendEntriesRequest, LogEntry as ProtoLogEntry,
    RequestVoteRequest as ProtoRequestVoteRequest, raft_rpc_client::RaftRpcClient,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct GrpcRpc;

impl GrpcRpc {
    pub fn new() -> Self {
        Self
    }

    async fn connect_client(
        &self,
        target: &str,
    ) -> Result<RaftRpcClient<tonic::transport::Channel>, String> {
        let endpoint = normalize_target_uri(target);
        let channel = Endpoint::from_shared(endpoint)
            .map_err(|e| format!("invalid endpoint: {e}"))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect grpc channel: {e}"))?;

        Ok(RaftRpcClient::new(channel))
    }
}

fn normalize_target_uri(target: &str) -> String {
    if target.starts_with("http://") || target.starts_with("https://") {
        target.to_string()
    } else {
        format!("http://{target}")
    }
}

fn to_proto_log_entry(entry: &LogEntry) -> ProtoLogEntry {
    ProtoLogEntry {
        term: entry.term,
        index: entry.index,
        command: entry.command.clone(),
    }
}

fn to_proto_append_entries_request(request: AppendEntriesRequest) -> ProtoAppendEntriesRequest {
    ProtoAppendEntriesRequest {
        term: request.term,
        leader_id: request.leader_id,
        prev_log_index: request.prev_log_index,
        prev_log_term: request.prev_log_term,
        entries: request.entries.iter().map(to_proto_log_entry).collect(),
        leader_commit: request.leader_commit,
    }
}

impl Rpc for GrpcRpc {
    async fn send_request_vote(
        &self,
        target: &str,
        term: u64,
        candidate_id: &str,
    ) -> Result<(), String> {
        let mut client = self.connect_client(target).await?;

        let request = Request::new(ProtoRequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
        });

        client
            .request_vote(request)
            .await
            .map_err(|e| format!("request_vote failed: {e}"))?;

        Ok(())
    }

    async fn send_append_entries(&self, request: AppendEntriesRequest) -> Result<(), String> {
        let target = request.leader_id.clone();
        let mut client = self.connect_client(&target).await?;

        let proto_request = to_proto_append_entries_request(request);
        client
            .append_entries(Request::new(proto_request))
            .await
            .map_err(|e| format!("append_entries failed: {e}"))?;

        Ok(())
    }
}
