use crate::storage::error::StorageError;
use crate::storage::storage::PersistentState;

pub trait Store {
    fn save(&mut self, state: PersistentState) -> Result<(), StorageError>;
    fn load(&self) -> Result<PersistentState, StorageError>;
}
