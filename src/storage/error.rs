use std::string::FromUtf8Error;

use thiserror::Error;

use crate::log::error::LogError;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Log(#[from] LogError),

    #[error(transparent)]
    Utf8(#[from] FromUtf8Error),

    #[error("runtime initialization failed: {0}")]
    RuntimeInit(std::io::Error),

    #[error("not enough bytes at offset {offset}: need {needed}, available {available}")]
    UnexpectedEof {
        offset: usize,
        needed: usize,
        available: usize,
    },

    #[error(
        "invalid log entry length at offset {offset}: declared {declared}, available {available}"
    )]
    InvalidLogEntryLength {
        offset: usize,
        declared: usize,
        available: usize,
    },

    #[error("invalid data: {0}")]
    InvalidData(String),
}
