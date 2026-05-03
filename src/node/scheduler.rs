use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use tokio::sync::mpsc::Receiver;
use tonic::Response;
use tracing;

use crate::node::{
    error::NodeError,
    node::RaftNode,
    rpc::{
        self,
        proto::{AppendEntriesResponse, RequestVoteResponse},
    },
};

pub enum SchedulerEvent {
    ElectionTimeout,
    HeartbeatTimeout,
}

pub struct NodeScheduler {
    node: Arc<tokio::sync::RwLock<RaftNode>>,

    event_recv: tokio::sync::mpsc::Receiver<SchedulerEvent>,

    heartbeat_timer: tokio::time::Interval,
    election_timer: tokio::time::Interval,
}

impl NodeScheduler {
    pub fn new(
        node: Arc<tokio::sync::RwLock<RaftNode>>,
        event_recv: Receiver<SchedulerEvent>,
    ) -> Self {
        NodeScheduler {
            node,
            event_recv,
            heartbeat_timer: tokio::time::interval(Duration::from_millis(
                randomized_heartbeat_timeout(),
            )),
            election_timer: tokio::time::interval(Duration::from_millis(
                randomized_election_timeout(),
            )),
        }
    }

    fn reset_election_timer(&mut self) {
        self.election_timer
            .reset_after(Duration::from_millis(randomized_election_timeout()));
    }

    fn reset_heartbeat_timer(&mut self) {
        self.heartbeat_timer
            .reset_after(Duration::from_millis(randomized_heartbeat_timeout()));
    }

    async fn send_request_vote(
        &self,
        peer: &str,
    ) -> Result<Response<RequestVoteResponse>, NodeError> {
        let (term, id, last_log_idx, last_log_term) = {
            let node = self.node.read().await;
            (
                node.get_term(),
                node.id.clone(),
                node.last_log_index().unwrap_or(0),
                node.last_log_term().unwrap_or(0),
            )
        };

        rpc::send_request_vote(peer, term, &id, last_log_idx, last_log_term).await
    }

    async fn send_append_entries(
        &self,
        peer: &str,
    ) -> Result<Response<AppendEntriesResponse>, NodeError> {
        let (term, id, prev_log_index, prev_log_term, entries, commit) = {
            let node = self.node.read().await;
            let prev_idx = node.last_log_index().unwrap_or(0);
            let prev_term = node.last_log_term().unwrap_or(0);
            let commit = node.get_commit_index();
            (
                node.get_term(),
                node.id.clone(),
                prev_idx,
                prev_term,
                Vec::new(), // entries sent separately
                commit,
            )
        };

        rpc::send_append_entries(
            peer,
            term,
            &id,
            prev_log_index,
            prev_log_term,
            entries,
            commit,
        )
        .await
    }

    /// Run election: become candidate, request votes, try to become leader
    async fn run_election(&mut self) {
        self.reset_election_timer();

        // 1. Become candidate ( acquire lock briefly)
        let node_id = {
            let mut node = self.node.write().await;
            node.become_candidate()
                .await
                .expect("Failed to become candidate (should not fail)");

            let id = node.get_id().to_string();
            let term = node.get_term();
            tracing::info!(node_id = %id, term = term, event = "election_started");
            id
        };

        // 2. Request votes from peers (no lock held)
        let peers = {
            let node = self.node.read().await;
            node.get_peers().to_vec()
        };

        let mut votes_received = 1u64; // Vote for self
        for peer in &peers {
            match self.send_request_vote(peer).await {
                Ok(resp) => {
                    let granted = resp.into_inner().success;
                    tracing::debug!(node_id = %node_id, peer = %peer, granted = granted, event = "vote_received");
                    if granted {
                        votes_received += 1;
                    }
                }
                Err(error) => {
                    tracing::error!(node_id = %node_id, error = %error, context = "request_vote_failed");
                }
            }
        }

        // 3. Check if won (brief lock)
        let election_won = {
            let node = self.node.read().await;
            votes_received >= node.get_min_majority_vote()
        };

        if election_won {
            // 4. Become leader (brief lock)
            let term = {
                let mut node = self.node.write().await;
                if let Err(err) = node.become_leader() {
                    tracing::error!(node_id = %node_id, error = %err, context = "become_leader_failed");
                    return;
                }
                tracing::info!(node_id = %node_id, term = node.get_term(), vote_count = votes_received, event = "became_leader");
                node.get_term()
            };

            // 5. Send initial AppendEntries to assert leadership
            let peers = {
                let node = self.node.read().await;
                node.get_peers().to_vec()
            };

            for peer in &peers {
                match self.send_append_entries(peer).await {
                    Ok(_) => tracing::trace!(node_id = %node_id, term = term, target = %peer, event = "heartbeat"),
                    Err(e) => tracing::error!(node_id = %node_id, error = %format!("heartbeat_failed: {}", e), context = "send_append_entries"),
                }
            }
        }
    }

    /// Send heartbeat to all peers (leader only)
    async fn send_heartbeats(&self) {
        let (node_id, term, peers) = {
            let node = self.node.read().await;
            if !node.is_leader() {
                return;
            }
            (node.get_id().to_string(), node.get_term(), node.get_peers().to_vec())
        };

        for peer in &peers {
            match self.send_append_entries(peer).await {
                Ok(_) => tracing::trace!(node_id = %node_id, term = term, target = %peer, event = "heartbeat"),
                Err(e) => tracing::error!(node_id = %node_id, error = %format!("heartbeat_failed: {}", e), context = "send_append_entries"),
            }
        }
    }

    pub async fn start(&mut self) {
        loop {
            tokio::select! {
                _ = self.election_timer.tick() => {
                    let is_leader = {
                        let node = self.node.read().await;
                        node.is_leader()
                    };
                    if !is_leader {
                        self.run_election().await;
                    }
                }
                _ = self.heartbeat_timer.tick() => {
                    let is_leader = {
                        let node = self.node.read().await;
                        node.is_leader()
                    };

                    // Only send heartbeats if we're the leader
                    if is_leader {
                        self.send_heartbeats().await;
                    }
                }
                Some(event) = self.event_recv.recv() => {
                    match event {
                        SchedulerEvent::ElectionTimeout => {
                            self.reset_election_timer();
                        }
                        SchedulerEvent::HeartbeatTimeout => {
                            // No-op: timer always runs, we check is_leader() before sending
                        }
                    }
                }
            }
        }
    }
}

fn randomized_election_timeout() -> u64 {
    let mut rng = rand::rng();
    let timeout_ms = rng.random_range(500..1000);

    timeout_ms
}

fn randomized_heartbeat_timeout() -> u64 {
    let mut rng = rand::rng();
    let timeout_ms = rng.random_range(0..50);

    timeout_ms
}
