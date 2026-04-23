use crate::log::error::LogError;

pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

impl LogEntry {
    pub fn new(term: u64, command: impl Into<Vec<u8>>) -> Self {
        let command = command.into();
        LogEntry {
            term,
            index: 0, // Will be set when appending to log
            command,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = vec![];
        data.extend(&self.term.to_le_bytes());
        data.extend(&self.index.to_le_bytes());
        data.extend(&(self.command.len() as u64).to_le_bytes());
        data.extend(&self.command);
        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, LogError> {
        if data.len() < 24 {
            return Err(LogError::UnexpectedEof {
                offset: 0,
                needed: 24,
                available: data.len(),
            });
        }

        let mut term_bytes = [0u8; 8];
        term_bytes.copy_from_slice(&data[0..8]);
        let term = u64::from_le_bytes(term_bytes);

        let mut index_bytes = [0u8; 8];
        index_bytes.copy_from_slice(&data[8..16]);
        let index = u64::from_le_bytes(index_bytes);

        let mut command_len_bytes = [0u8; 8];
        command_len_bytes.copy_from_slice(&data[16..24]);
        let command_len = u64::from_le_bytes(command_len_bytes) as usize;

        let command_start = 24;
        let command_end = command_start + command_len;
        if command_end > data.len() {
            return Err(LogError::InvalidCommandLength {
                declared: command_len,
                available: data.len().saturating_sub(command_start),
            });
        }

        let command = data[command_start..command_end].to_vec();

        Ok(LogEntry {
            term,
            index,
            command,
        })
    }
}
