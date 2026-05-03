use std::collections::HashMap;

use tokio::sync::mpsc::Sender;

use crate::log::log::LogEntry;
use crate::node::error::NodeError;
use crate::node::rpc;
use crate::storage::api::Store;
use crate::storage::storage::PersistentState;

#[derive(Debug)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    pub id: String,
    peers: Vec<String>,

    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,

    commit_index: u64,
    last_applied: u64,

    // Snapshot state for log compaction
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub snapshot_data: Option<Vec<u8>>,

    next_index: Option<HashMap<String, u64>>, // Only used by leader to track next log index to send to each follower
    match_index: Option<HashMap<String, u64>>, // Only used by leader to track replication status of followers

    state: NodeState,
    leader_id: Option<String>, // Track who the current leader is

    storage: Box<dyn Store>,

    event_sender: Sender<Vec<u8>>,
}

impl RaftNode {
    pub fn new(
        id: String,
        peers: Vec<String>,
        storage: Box<dyn Store>,
        event_sender: Sender<Vec<u8>>,
    ) -> Self {
        // Initialize with defaults - load() should be called after to recover state
        RaftNode {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            last_included_index: 0,
            last_included_term: 0,
            snapshot_data: None,
            next_index: None,
            match_index: None,
            state: NodeState::Follower,
            leader_id: None,
            storage,
            event_sender,
        }
    }

    /// Create a new node and load persisted state from storage
    pub async fn new_with_persistence(
        id: String,
        peers: Vec<String>,
        storage: Box<dyn Store>,
    ) -> Result<Self, NodeError> {
        let mut node = Self::new(id, peers, storage, tokio::sync::mpsc::channel(100).0);

        // Load persisted state
        if let Ok(state) = node.storage.load().await {
            node.current_term = state.current_term;
            node.voted_for = state.voted_for;
            // Note: log loading would go here too
        }

        Ok(node)
    }

    // Methods for handling RPCs, state transitions, and log replication would be implemented here
    pub async fn become_candidate(&mut self) -> Result<(), NodeError> {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());

        // Persist both term and voted_for
        self.storage.update_term(self.current_term).await?;
        self.storage
            .update_voted_for(self.voted_for.clone())
            .await?;

        Ok(())
    }

    pub fn become_leader(&mut self) -> Result<(), NodeError> {
        self.state = NodeState::Leader;
        self.leader_id = Some(self.id.clone());
        // Initialize next_index and match_index for each peer
        let last_idx = self.last_log_index()?;

        for peer in &self.peers {
            self.next_index
                .get_or_insert_with(HashMap::new)
                .insert(peer.clone(), last_idx + 1);
            self.match_index
                .get_or_insert_with(HashMap::new)
                .insert(peer.clone(), 0);
        }

        Ok(())
    }

    pub fn become_follower(&mut self, term: u64) {
        self.state = NodeState::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = None; // Clear leader when becoming follower
    }

    pub fn set_leader(&mut self, leader_id: String) {
        self.leader_id = Some(leader_id);
    }

    pub fn get_leader_id(&self) -> Option<&str> {
        self.leader_id.as_deref()
    }

    pub fn last_log_index(&self) -> Result<u64, NodeError> {
        self.log
            .last()
            .map(|entry| entry.index)
            .ok_or(NodeError::EmptyLog)
    }

    pub fn last_log_term(&self) -> Result<u64, NodeError> {
        self.log
            .last()
            .map(|entry| entry.term)
            .ok_or(NodeError::EmptyLog)
    }

    pub fn get_peers(&self) -> &[String] {
        &self.peers
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_term(&self) -> u64 {
        self.current_term
    }

    pub async fn set_term(&mut self, term: u64) -> Result<(), NodeError> {
        self.current_term = term;
        self.storage.update_term(self.current_term).await?;
        Ok(())
    }

    pub fn get_voted_for(&self) -> Option<&String> {
        self.voted_for.as_ref()
    }

    pub async fn set_voted_for(&mut self, candidate_id: Option<String>) -> Result<(), NodeError> {
        self.voted_for = candidate_id;
        self.storage
            .update_voted_for(self.voted_for.clone())
            .await?;

        Ok(())
    }

    pub async fn push_log(&mut self, data: Vec<u8>, term: Option<u64>) -> Result<(), NodeError> {
        // Get prev log index/term before pushing
        let log_len = self.log.len() as u64;
        let prev_log_index = log_len.saturating_sub(1);
        let prev_log_term = self.log.last().map(|e| e.term).unwrap_or(0);

        // Use provided term or fall back to current_term
        let entry_term = term.unwrap_or(self.current_term);

        // 1. Append to local log
        self.log.push(LogEntry {
            term: entry_term,
            index: log_len,
            command: data,
        });
        self.persist().await?;

        // 2. Replicate to all peers
        let term = self.current_term;
        let leader_id = self.id.clone();

        // Convert entry to proto format
        let proto_entries = vec![rpc::proto::LogEntry {
            term: self.log.last().unwrap().term,
            index: self.log.len() as u64,
            command: self.log.last().unwrap().command.clone(),
        }];

        let min_qourum = self.get_min_majority_vote();
        let mut success_count = 1; // Count self vote

        for peer in &self.peers {
            let prev_idx = prev_log_index;
            let prev_term = prev_log_term;

            // Send to peer (ignore errors for now - leader will retry)
            let res = rpc::send_append_entries(
                peer,
                term,
                &leader_id,
                prev_idx,
                prev_term,
                proto_entries.clone(),
                self.commit_index, // don't commit yet - wait for quorum
            )
            .await?;

            if res.get_ref().success {
                success_count += 1;
            }
        }

        if success_count >= min_qourum {
            self.commit_index = self.log.len() as u64;

            self.event_sender
                .send(self.log.last().unwrap().command.clone())
                .await
                .map_err(|e| NodeError::EventSend(e))?;
        }

        Ok(())
    }

    /// Persist the entire node state to storage
    pub async fn persist(&mut self) -> Result<(), NodeError> {
        let state = crate::storage::storage::PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            log: self.log.clone(),
        };
        self.storage.save(state).await?;
        Ok(())
    }

    pub fn get_min_majority_vote(&self) -> u64 {
        (self.peers.len() as u64).div_ceil(2) + 1
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, NodeState::Leader)
    }

    pub fn get_commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn set_commit_index(&mut self, index: u64) {
        self.commit_index = index;
    }

    // -- Snapshot management --

    /// Install a snapshot from the leader, replacing state machine and truncating log
    pub async fn install_snapshot(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: Vec<u8>,
    ) -> Result<(), NodeError> {
        // Raft spec: truncate log to lastIncludedIndex - 1, delete conflicting entries
        // Since our log is 0-indexed, we keep entries before last_included_index
        if last_included_index > 0 {
            self.log.truncate(last_included_index as usize);
        }

        // Apply snapshot state
        self.last_included_index = last_included_index;
        self.last_included_term = last_included_term;
        self.snapshot_data = Some(snapshot_data);

        // Reset volatile state per Raft spec
        self.commit_index = last_included_index;
        self.last_applied = last_included_index;

        Ok(())
    }

    /// Get snapshot state for sending to followers
    pub fn get_snapshot_state(&self) -> (u64, u64, Option<&Vec<u8>>) {
        (
            self.last_included_index,
            self.last_included_term,
            self.snapshot_data.as_ref(),
        )
    }

    /// Check if node has a snapshot to send
    pub fn has_snapshot(&self) -> bool {
        self.snapshot_data.is_some()
    }

    pub async fn get_raft_state(&self) -> Result<PersistentState, NodeError> {
        let res = self
            .storage
            .load()
            .await
            .map_err(|e| NodeError::Storage(e))?;

        Ok(res)
    }
}
