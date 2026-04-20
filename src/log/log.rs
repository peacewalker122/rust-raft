pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}
