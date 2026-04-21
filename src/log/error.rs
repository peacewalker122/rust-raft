use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("not enough bytes at offset {offset}: need {needed}, available {available}")]
    UnexpectedEof {
        offset: usize,
        needed: usize,
        available: usize,
    },

    #[error("invalid command length: declared {declared}, available {available}")]
    InvalidCommandLength { declared: usize, available: usize },
}
