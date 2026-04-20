use crate::storage::storage::PersistentState;

pub trait Store {
    fn save(&mut self, state: PersistentState) -> Result<(), String>;
    fn load(&self) -> Result<&PersistentState, String>;
}
