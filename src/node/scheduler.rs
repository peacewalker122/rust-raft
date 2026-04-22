use std::sync::Arc;

use crate::node::{error::NodeError, node::RaftNode, rpc};

pub struct NodeScheduler {
    node: Arc<tokio::sync::RwLock<RaftNode>>,
}

impl NodeScheduler {
    pub fn new(node: Arc<tokio::sync::RwLock<RaftNode>>) -> Self {
        NodeScheduler { node }
    }

    async fn send_request_vote(&self, peer: &str) -> Result<(), NodeError> {
        let (term, id) = {
            let node = self.node.read().await;
            (node.get_term(), node.id.clone())
        };

        rpc::send_request_vote(peer, term, &id).await
    }

    pub async fn start(&self) {
        loop {
            {
                let mut node = self.node.write().await;
                node.election_timer.tick().await;
                node.become_candidate();
            }

            let peers = {
                let node = self.node.read().await;
                node.get_peers().to_vec()
            };

            for peer in peers {
                if let Err(error) = self.send_request_vote(&peer).await {
                    println!("Failed to request vote from {}: {}", peer, error);
                }
            }
        }
    }
}
