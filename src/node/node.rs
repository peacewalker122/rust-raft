use std::collections::HashMap;

use rand::RngExt;

use crate::log::log::LogEntry;
use crate::node::error::NodeError;

#[derive(Debug)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
}

struct RaftNode {
    id: String,
    peers: Vec<String>,

    current_term: u64,
    voted_for: Option<String>,
    log: Vec<LogEntry>,

    commit_index: u64,
    last_applied: u64,

    next_index: Option<HashMap<String, u64>>,
    match_index: Option<HashMap<String, u64>>,

    state: NodeState,

    // Hearbeat timer and election timer would be implemented here
    heartbeat_timer: std::time::Instant,
    election_timer: std::time::Instant,
}

fn randomized_election_timeout() -> std::time::Duration {
    let mut rng = rand::rng();
    let timeout_ms = rng.random_range(150..300);

    std::time::Duration::from_millis(timeout_ms)
}

impl RaftNode {
    fn new(id: String, peers: Vec<String>) -> Self {
        RaftNode {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            match_index: None,
            state: NodeState::Follower,
            heartbeat_timer: std::time::Instant::now(),
            election_timer: std::time::Instant::now() + randomized_election_timeout(),
        }
    }

    pub fn reset_election_timer(&mut self) {
        self.election_timer = std::time::Instant::now() + randomized_election_timeout();
    }

    // Methods for handling RPCs, state transitions, and log replication would be implemented here
    pub fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        // Reset election timer
        self.election_timer = std::time::Instant::now() + randomized_election_timeout();
    }

    pub fn become_leader(&mut self) -> Result<(), NodeError> {
        self.state = NodeState::Leader;
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
        // Reset election timer
        self.election_timer = std::time::Instant::now() + randomized_election_timeout();
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
}
