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
}
