use std::collections::HashMap;
use std::time::Duration;

use rand::RngExt;
use tokio::time::interval;

use crate::log::log::LogEntry;
use crate::node::error::NodeError;
use crate::node::rpc;

#[derive(Debug)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    pub id: String,
    peers: Vec<String>,

    current_term: u64,
    voted_for: Option<String>,
    log: Vec<LogEntry>,

    commit_index: u64,
    last_applied: u64,

    next_index: Option<HashMap<String, u64>>, // Only used by leader to track next log index to send to each follower
    match_index: Option<HashMap<String, u64>>, // Only used by leader to track replication status of followers

    state: NodeState,

    // Hearbeat timer and election timer would be implemented here
    pub heartbeat_timer: tokio::time::Interval,
    pub election_timer: tokio::time::Interval,
}

fn randomized_election_timeout() -> u64 {
    let mut rng = rand::rng();
    let timeout_ms = rng.random_range(150..300);

    timeout_ms
}

fn randomized_heartbeat_timeout() -> u64 {
    let mut rng = rand::rng();
    let timeout_ms = rng.random_range(50..100);

    timeout_ms
}

impl RaftNode {
    pub fn new(id: String, peers: Vec<String>) -> Self {
        let mut node = RaftNode {
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
            heartbeat_timer: interval(Duration::from_millis(randomized_heartbeat_timeout())),
            election_timer: interval(Duration::from_millis(randomized_election_timeout())),
        };

        node
    }

    pub fn reset_election_timer(&mut self) {
        self.election_timer = interval(Duration::from_millis(randomized_election_timeout()));
    }

    // Methods for handling RPCs, state transitions, and log replication would be implemented here
    pub fn become_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());

        // Reset election timer
        self.reset_election_timer();
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
        self.reset_election_timer();
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

    pub fn get_voted_for(&self) -> Option<&String> {
        self.voted_for.as_ref()
    }

    pub fn set_voted_for(&mut self, candidate_id: Option<String>) {
        self.voted_for = candidate_id;
    }

    pub fn push_log(&mut self, entry: crate::log::log::LogEntry) {
        self.log.push(entry);
    }

    pub fn get_min_majority_vote(&self) -> u64 {
        (self.peers.len() as u64).div_ceil(2) + 1
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, NodeState::Leader)
    }
}
