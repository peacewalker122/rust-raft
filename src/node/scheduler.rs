use std::sync::Arc;
use std::time::Duration;

use tonic::Response;

use crate::node::{
    error::NodeError,
    node::RaftNode,
    rpc::{
        self,
        proto::{AppendEntriesResponse, RequestVoteResponse},
    },
};

pub struct NodeScheduler {
    node: Arc<tokio::sync::RwLock<RaftNode>>,
}

impl NodeScheduler {
    pub fn new(node: Arc<tokio::sync::RwLock<RaftNode>>) -> Self {
        NodeScheduler { node }
    }

    // ============================================================
    // Option 3: Separate Timer Tasks (runs outside lock)
    // ============================================================

    /// Spawns a background task that signals election timeout
    fn spawn_election_timer(&self) -> tokio::sync::mpsc::Receiver<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            loop {
                // Use a simple timeout (fixed or use StdRng if needed)
                // rand::rng() is not Send, so use thread_rng from thread-local
                let timeout_ms = 200u64;
                tokio::time::sleep(Duration::from_millis(timeout_ms)).await;

                let _ = tx.send(()).await;
            }
        });

        rx
    }

    /// Spawns a background task that signals heartbeat timeout
    fn spawn_heartbeat_timer(&self) -> tokio::sync::mpsc::Receiver<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            loop {
                let timeout_ms = 50u64;
                tokio::time::sleep(Duration::from_millis(timeout_ms)).await;

                let _ = tx.send(()).await;
            }
        });

        rx
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
        let (term, id) = {
            let node = self.node.read().await;
            (node.get_term(), node.id.clone())
        };

        rpc::send_append_entries(peer, term, &id).await
    }

    /// Run election: become candidate, request votes, try to become leader
    async fn run_election(&self) {
        // 1. Become candidate ( acquire lock briefly)
        {
            let mut node = self.node.write().await;
            node.become_candidate();
            println!(
                "Node {} started election for term {}",
                node.get_id(),
                node.get_term()
            );
        }

        // 2. Request votes from peers (no lock held)
        let peers = {
            let node = self.node.read().await;
            node.get_peers().to_vec()
        };

        let mut votes_received = 1u64; // Vote for self
        for peer in peers {
            match self.send_request_vote(&peer).await {
                Ok(resp) => {
                    if resp.into_inner().success {
                        println!("Received vote from {}", peer);
                        votes_received += 1;
                    }
                }
                Err(error) => println!("Failed to request vote from {}: {}", peer, error),
            }
        }

        // 3. Check if won (brief lock)
        let election_won = {
            let node = self.node.read().await;
            votes_received >= node.get_min_majority_vote()
        };

        if election_won {
            // 4. Become leader (brief lock)
            {
                let mut node = self.node.write().await;
                if let Err(err) = node.become_leader() {
                    println!("Failed to become leader: {}", err);
                    return;
                }
                println!(
                    "Node {} became leader with {} votes",
                    node.get_id(),
                    votes_received
                );
            }

            // 5. Send initial AppendEntries to assert leadership
            let peers = {
                let node = self.node.read().await;
                node.get_peers().to_vec()
            };

            for peer in peers {
                match self.send_append_entries(&peer).await {
                    Ok(_) => println!("Sent heartbeat to {}", peer),
                    Err(e) => println!("Failed to send heartbeat to {}: {}", peer, e),
                }
            }
        }
    }

    /// Send heartbeat to all peers (leader only)
    async fn send_heartbeats(&self) {
        let peers = {
            let node = self.node.read().await;
            if !node.is_leader() {
                return;
            }
            node.get_peers().to_vec()
        };

        for peer in peers {
            match self.send_append_entries(&peer).await {
                Ok(_) => println!("Sent heartbeat to {}", peer),
                Err(e) => println!("Failed to send heartbeat to {}: {}", peer, e),
            }
        }
    }

    pub async fn start(&self) {
        // Spawn timer tasks (Option 3)
        let mut election_timer = self.spawn_election_timer();
        let mut heartbeat_timer = self.spawn_heartbeat_timer();

        loop {
            // Use tokio::select! to wait on whichever timer fires (Option 2)
            tokio::select! {
                // Election timeout fired -> run election
                _ = election_timer.recv() => {
                    self.run_election().await;
                }

                // Heartbeat timeout fired -> send heartbeats
                _ = heartbeat_timer.recv() => {
                    self.send_heartbeats().await;
                }
            }
        }
    }
}

