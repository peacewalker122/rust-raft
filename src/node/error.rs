use thiserror::Error;

use crate::{log::error::LogError, storage::error::StorageError};

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("node log is empty")]
    EmptyLog,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Log(#[from] LogError),

    #[error(transparent)]
    GrpcTransport(#[from] tonic::transport::Error),

    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),

    #[error("invalid peer target: {0}")]
    InvalidPeerTarget(String),

    #[error("request_vote failed: {0}")]
    RequestVoteFailed(String),

    #[error("append_entries failed: {0}")]
    AppendEntriesFailed(String),

    #[error("internal server error: {0}")]
    InternalServerError(String),
}
