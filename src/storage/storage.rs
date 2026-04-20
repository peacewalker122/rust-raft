use crate::{log::log::LogEntry, storage::api::Store};

pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,
}

struct MemoryStore {
    state: PersistentState,
}

impl Store for MemoryStore {
    fn save(&mut self, state: PersistentState) -> Result<(), String> {
        self.state = state;
        Ok(())
    }

    fn load(&self) -> Result<&PersistentState, String> {
        Ok(&self.state)
    }
}
