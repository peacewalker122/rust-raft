use crate::log::log::LogEntry;

pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

pub trait Rpc {
    async fn send_request_vote(
        &self,
        target: &str,
        term: u64,
        candidate_id: &str,
    ) -> Result<(), String>;
    async fn send_append_entries(&self, request: AppendEntriesRequest) -> Result<(), String>;
}
