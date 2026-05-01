use crate::{
    log::log::LogEntry,
    storage::{api::Store, error::StorageError},
};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Clone)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,
}

impl PersistentState {
    /// Serializes only term and voted_for.
    pub fn serialize_term_and_vote(&self) -> Vec<u8> {
        let mut data = vec![];

        data.extend(&self.current_term.to_le_bytes());
        if let Some(voted_for) = &self.voted_for {
            data.extend(&(voted_for.len() as u64).to_le_bytes());
            data.extend(voted_for.as_bytes());
        } else {
            data.extend(&0u64.to_le_bytes());
        }

        data
    }

    /// Serializes only term and voted_for (async version).
    pub async fn serialize_term_and_vote_async(&self) -> Vec<u8> {
        self.serialize_term_and_vote()
    }

    /// Deserializes only term and voted_for.
    pub fn deserialize_term_and_vote(data: &[u8]) -> Result<(u64, Option<String>), StorageError> {
        let mut offset = 0usize;
        Self::deserialize_term_and_vote_at(data, &mut offset)
    }

    /// Deserializes only term and voted_for (async version).
    pub async fn deserialize_term_and_vote_async(
        data: &[u8],
    ) -> Result<(u64, Option<String>), StorageError> {
        Self::deserialize_term_and_vote(data)
    }

    /// Deserializes only term and voted_for, starting from given offset.
    /// Updates offset to end of term/voted_for data.
    pub fn deserialize_term_and_vote_at(
        data: &[u8],
        offset: &mut usize,
    ) -> Result<(u64, Option<String>), StorageError> {
        let current_term = read_u64(data, offset)?;

        let voted_for_len = read_u64(data, offset)? as usize;
        let voted_for = if voted_for_len > 0 {
            let bytes = read_bytes(data, offset, voted_for_len)?;
            Some(
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| StorageError::InvalidData(e.to_string()))?,
            )
        } else {
            None
        };

        Ok((current_term, voted_for))
    }

    /// Serializes only the log entries.
    pub fn serialize_log(&self) -> Vec<u8> {
        let mut data = vec![];

        data.extend(&(self.log.len() as u64).to_le_bytes());
        for entry in &self.log {
            let entry_data = entry.serialize();
            data.extend(&(entry_data.len() as u64).to_le_bytes());
            data.extend(entry_data);
        }

        data
    }

    /// Serializes only the log entries (async version).
    pub async fn serialize_log_async(&self) -> Vec<u8> {
        self.serialize_log()
    }

    /// Deserializes only the log entries.
    pub fn deserialize_log(data: &[u8]) -> Result<Vec<LogEntry>, StorageError> {
        let mut offset = 0usize;
        Self::deserialize_log_at(data, &mut offset)
    }

    /// Deserializes only the log entries (async version).
    pub async fn deserialize_log_async(data: &[u8]) -> Result<Vec<LogEntry>, StorageError> {
        Self::deserialize_log(data)
    }

    /// Deserializes only the log entries, starting from given offset.
    /// Updates offset to end of log data.
    pub fn deserialize_log_at(
        data: &[u8],
        offset: &mut usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        let log_len = read_u64(data, offset)? as usize;
        let mut log = Vec::with_capacity(log_len);
        for _ in 0..log_len {
            let entry_len = read_u64(data, offset)? as usize;
            let entry_data = read_bytes(data, offset, entry_len)?;
            log.push(LogEntry::deserialize(entry_data)?);
        }

        Ok(log)
    }

    /// Full serialization (for backward compatibility).
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = self.serialize_term_and_vote();
        data.extend(self.serialize_log());
        data
    }

    /// Full serialization (async version).
    pub async fn serialize_async(&self) -> Vec<u8> {
        let term_data = self.serialize_term_and_vote_async().await;
        let log_data = self.serialize_log_async().await;
        let mut data = term_data;
        data.extend(log_data);
        data
    }

    /// Full deserialization (for backward compatibility).
    pub fn deserialize(data: &[u8]) -> Result<Self, StorageError> {
        let mut offset = 0usize;
        let (current_term, voted_for) = Self::deserialize_term_and_vote_at(data, &mut offset)?;
        let log = Self::deserialize_log_at(data, &mut offset)?;
        Ok(PersistentState {
            current_term,
            voted_for,
            log,
        })
    }

    /// Full deserialization (async version).
    pub async fn deserialize_async(data: &[u8]) -> Result<Self, StorageError> {
        let mut offset = 0usize;
        let (current_term, voted_for) = Self::deserialize_term_and_vote_at(data, &mut offset)?;
        let log = Self::deserialize_log_at(data, &mut offset)?;
        Ok(PersistentState {
            current_term,
            voted_for,
            log,
        })
    }
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, StorageError> {
    let bytes = read_bytes(data, offset, 8)?;
    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(array))
}

fn read_bytes<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], StorageError> {
    let start = *offset;
    let end = start + len;

    if end > data.len() {
        return Err(StorageError::UnexpectedEof {
            offset: start,
            needed: len,
            available: data.len().saturating_sub(start),
        });
    }

    *offset = end;
    Ok(&data[start..end])
}

pub struct PersistentStore {
    log_file: tokio::fs::File,
    term_file: tokio::fs::File,
    state: PersistentState,
}

impl PersistentStore {
    pub fn new(log_file: tokio::fs::File, term_file: tokio::fs::File) -> Self {
        PersistentStore {
            log_file,
            term_file,
            state: PersistentState {
                current_term: 0,
                voted_for: None,
                log: vec![],
            },
        }
    }

    /// Get mutable reference to state for testing
    pub fn state_mut(&mut self) -> &mut PersistentState {
        &mut self.state
    }

    /// Updates term and voted_for with immediate flush to term_file for crash safety.
    pub async fn update_term_and_voted_for(&mut self) -> Result<(), StorageError> {
        let data = self.state.serialize_term_and_vote();

        // Truncate and seek to start
        self.term_file.set_len(0).await?;
        self.term_file.seek(std::io::SeekFrom::Start(0)).await?;

        // Write and flush immediately for crash safety
        self.term_file.write_all(&data).await?;
        self.term_file.sync_all().await?;

        Ok(())
    }

    /// Loads term and voted_for from term_file.
    pub async fn load_term_and_voted_for(&self) -> Result<(u64, Option<String>), StorageError> {
        let mut file = match tokio::fs::File::open("raft_term.bin").await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok((0u64, None));
            }
            Err(e) => return Err(StorageError::IoError(e.to_string())),
        };

        let mut data = vec![];
        file.read_to_end(&mut data)
            .await
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        if data.is_empty() {
            return Ok((0u64, None));
        }

        PersistentState::deserialize_term_and_vote_async(&data).await
    }
}

#[async_trait]
impl Store for PersistentStore {
    async fn save(&mut self, state: PersistentState) -> Result<(), StorageError> {
        self.state = state;

        // Write log to log_file
        let log_data = self.state.serialize_log();
        self.log_file.set_len(0).await?;
        self.log_file.seek(std::io::SeekFrom::Start(0)).await?;
        self.log_file.write_all(&log_data).await?;
        self.log_file.sync_all().await?;

        // Write term and voted_for to term_file
        let term_data = self.state.serialize_term_and_vote();
        self.term_file.set_len(0).await?;
        self.term_file.seek(std::io::SeekFrom::Start(0)).await?;
        self.term_file.write_all(&term_data).await?;
        self.term_file.sync_all().await?;

        Ok(())
    }

    async fn load(&self) -> Result<PersistentState, StorageError> {
        // Read term and voted_for from stored term_file
        let mut term_file_clone = self
            .term_file
            .try_clone()
            .await
            .map_err(|e| StorageError::IoError(e.to_string()))?;
        let mut term_data = vec![];
        term_file_clone
            .read_to_end(&mut term_data)
            .await
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        let (term, voted_for) = if term_data.is_empty() {
            (0u64, None)
        } else {
            PersistentState::deserialize_term_and_vote(&term_data)?
        };

        // Read log from stored log_file
        let mut log_file_clone = self
            .log_file
            .try_clone()
            .await
            .map_err(|e| StorageError::IoError(e.to_string()))?;
        let mut log_data = vec![];
        log_file_clone
            .read_to_end(&mut log_data)
            .await
            .map_err(|e| StorageError::IoError(e.to_string()))?;

        let log = if log_data.is_empty() {
            vec![]
        } else {
            PersistentState::deserialize_log(&log_data)?
        };

        Ok(PersistentState {
            current_term: term,
            voted_for,
            log,
        })
    }

    async fn update_term(&mut self, term: u64) -> Result<(), StorageError> {
        self.state.current_term = term;
        self.update_term_and_voted_for().await
    }

    async fn update_voted_for(&mut self, voted_for: Option<String>) -> Result<(), StorageError> {
        self.state.voted_for = voted_for;
        self.update_term_and_voted_for().await
    }
}

/// Mock store for testing - stores everything in memory
#[derive(Clone)]
pub struct MockStore {
    state: PersistentState,
}

impl MockStore {
    pub fn new() -> Self {
        MockStore {
            state: PersistentState {
                current_term: 0,
                voted_for: None,
                log: vec![],
            },
        }
    }
}

impl Default for MockStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Store for MockStore {
    async fn save(&mut self, state: PersistentState) -> Result<(), StorageError> {
        self.state = state;
        Ok(())
    }

    async fn load(&self) -> Result<PersistentState, StorageError> {
        Ok(self.state.clone())
    }

    async fn update_term(&mut self, term: u64) -> Result<(), StorageError> {
        self.state.current_term = term;
        Ok(())
    }

    async fn update_voted_for(&mut self, voted_for: Option<String>) -> Result<(), StorageError> {
        self.state.voted_for = voted_for;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PersistentState;
    use crate::{
        log::{error::LogError, log::LogEntry},
        storage::error::StorageError,
    };

    #[test]
    fn serialize_deserialize_roundtrip_with_full_data() {
        // Arrange
        let state = PersistentState {
            current_term: 7,
            voted_for: Some("node-1".to_string()),
            log: vec![
                LogEntry {
                    term: 1,
                    index: 1,
                    command: b"set x 10".to_vec(),
                },
                LogEntry {
                    term: 2,
                    index: 2,
                    command: vec![0, 159, 146, 150],
                },
            ],
        };

        // Act
        let encoded = state.serialize();
        let decoded = PersistentState::deserialize(&encoded).expect("deserialize should succeed");

        // Assert
        assert_eq!(decoded.current_term, state.current_term);
        assert_eq!(decoded.voted_for, state.voted_for);
        assert_eq!(decoded.log.len(), state.log.len());
        assert_eq!(decoded.log[0].term, state.log[0].term);
        assert_eq!(decoded.log[0].index, state.log[0].index);
        assert_eq!(decoded.log[0].command, state.log[0].command);
        assert_eq!(decoded.log[1].term, state.log[1].term);
        assert_eq!(decoded.log[1].index, state.log[1].index);
        assert_eq!(decoded.log[1].command, state.log[1].command);
    }

    #[test]
    fn serialize_deserialize_roundtrip_with_empty_state() {
        // Arrange
        let state = PersistentState {
            current_term: 0,
            voted_for: None,
            log: vec![],
        };

        // Act
        let encoded = state.serialize();
        let decoded = PersistentState::deserialize(&encoded).expect("deserialize should succeed");

        // Assert
        assert_eq!(decoded.current_term, 0);
        assert_eq!(decoded.voted_for, None);
        assert!(decoded.log.is_empty());
    }

    #[test]
    fn deserialize_returns_unexpected_eof_for_truncated_header() {
        // Arrange
        let data = vec![1, 2, 3, 4, 5, 6, 7];

        // Act
        let result = PersistentState::deserialize(&data);

        // Assert
        assert!(matches!(
            result,
            Err(StorageError::UnexpectedEof {
                offset: 0,
                needed: 8,
                available: 7
            })
        ));
    }

    #[test]
    fn deserialize_returns_invalid_data_for_invalid_utf8_voted_for() {
        // Arrange
        let mut data = Vec::new();
        data.extend(&1u64.to_le_bytes()); // current_term
        data.extend(&1u64.to_le_bytes()); // voted_for len
        data.push(0xFF); // invalid UTF-8 byte
        data.extend(&0u64.to_le_bytes()); // log len

        // Act
        let result = PersistentState::deserialize(&data);

        // Assert
        assert!(matches!(result, Err(StorageError::InvalidData(_))));
    }

    #[test]
    fn deserialize_returns_log_error_for_invalid_log_entry_payload() {
        // Arrange
        let mut data = Vec::new();
        data.extend(&1u64.to_le_bytes()); // current_term
        data.extend(&0u64.to_le_bytes()); // voted_for len
        data.extend(&1u64.to_le_bytes()); // log len

        let mut bad_entry = Vec::new();
        bad_entry.extend(&1u64.to_le_bytes()); // term
        bad_entry.extend(&1u64.to_le_bytes()); // index
        bad_entry.extend(&10u64.to_le_bytes()); // command len says 10
        bad_entry.extend([1, 2]); // only 2 bytes provided

        data.extend(&(bad_entry.len() as u64).to_le_bytes());
        data.extend(bad_entry);

        // Act
        let result = PersistentState::deserialize(&data);

        // Assert
        assert!(matches!(
            result,
            Err(StorageError::Log(LogError::InvalidCommandLength { .. }))
                | Err(StorageError::Log(LogError::UnexpectedEof { .. }))
        ));
    }

    #[test]
    fn deserialize_is_crash_proof_for_all_truncated_tails() {
        // Arrange
        let state = PersistentState {
            current_term: 42,
            voted_for: Some("node-crash-proof".to_string()),
            log: vec![
                LogEntry {
                    term: 3,
                    index: 1,
                    command: b"set a 1".to_vec(),
                },
                LogEntry {
                    term: 3,
                    index: 2,
                    command: b"set b 2".to_vec(),
                },
            ],
        };
        let encoded = state.serialize();

        // Act + Assert
        for cut in 0..encoded.len() {
            let truncated = &encoded[..cut];
            let result = PersistentState::deserialize(truncated);
            assert!(
                result.is_err(),
                "expected Err for truncated input length {cut}, got Ok"
            );
        }

        let full_result = PersistentState::deserialize(&encoded);
        assert!(full_result.is_ok(), "expected Ok for full encoded payload");
    }
}

