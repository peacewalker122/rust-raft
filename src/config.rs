use std::{io, net::SocketAddr};

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: String,
    pub peers: Vec<String>,
    pub grpc_bind: SocketAddr,
    pub term_file_path: String,
    pub log_file_path: String,
}

impl RaftConfig {
    pub fn from_env() -> Result<Self, io::Error> {
        let node_id = std::env::var("RAFT_NODE_ID").unwrap_or_else(|_| "node-1".to_string());
        let peers = std::env::var("RAFT_PEERS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|peer| !peer.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        let grpc_bind_raw =
            std::env::var("RAFT_GRPC_BIND").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
        let grpc_bind = parse_bind_addr(&grpc_bind_raw)?;

        let term_file_path =
            std::env::var("RAFT_TERM_FILE").unwrap_or_else(|_| "raft_term.dat".to_string());
        let log_file_path =
            std::env::var("RAFT_LOG_FILE").unwrap_or_else(|_| "raft_log.dat".to_string());

        Ok(Self {
            node_id,
            peers,
            grpc_bind,
            term_file_path,
            log_file_path,
        })
    }
}

fn parse_bind_addr(raw_addr: &str) -> Result<SocketAddr, io::Error> {
    raw_addr.parse().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid RAFT_GRPC_BIND '{raw_addr}': {e}"),
        )
    })
}
