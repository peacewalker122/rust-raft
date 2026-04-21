use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("internal server error: {0}")]
    InternalServerError(String),
}
