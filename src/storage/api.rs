use crate::storage::error::StorageError;
use crate::storage::storage::PersistentState;

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    async fn save(&mut self, state: PersistentState) -> Result<(), StorageError>;
    async fn load(&self) -> Result<PersistentState, StorageError>;
    async fn update_term(&mut self, term: u64) -> Result<(), StorageError>;
    async fn update_voted_for(&mut self, voted_for: Option<String>) -> Result<(), StorageError>;
}

