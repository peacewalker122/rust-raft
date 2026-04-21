use crate::{
    log::log::LogEntry,
    storage::{api::Store, error::StorageError},
};
use tokio::{io::AsyncWriteExt, runtime::Runtime};

pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<LogEntry>,
}

impl PersistentState {
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = vec![];

        data.extend(&self.current_term.to_le_bytes());
        if let Some(voted_for) = &self.voted_for {
            data.extend(&(voted_for.len() as u64).to_le_bytes());
            data.extend(voted_for.as_bytes());
        } else {
            data.extend(&0u64.to_le_bytes());
        }

        data.extend(&(self.log.len() as u64).to_le_bytes());
        for entry in &self.log {
            let entry_data = entry.serialize();
            data.extend(&(entry_data.len() as u64).to_le_bytes());
            data.extend(entry_data);
        }

        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, StorageError> {
        let mut offset = 0usize;

        let current_term = read_u64(data, &mut offset)?;

        let voted_for_len = read_u64(data, &mut offset)? as usize;
        let voted_for = if voted_for_len > 0 {
            let bytes = read_bytes(data, &mut offset, voted_for_len)?;
            Some(
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| StorageError::InvalidData(e.to_string()))?,
            )
        } else {
            None
        };

        let log_len = read_u64(data, &mut offset)? as usize;
        let mut log = Vec::with_capacity(log_len);
        for _ in 0..log_len {
            let entry_len = read_u64(data, &mut offset)? as usize;
            let entry_data = read_bytes(data, &mut offset, entry_len)?;
            log.push(LogEntry::deserialize(entry_data)?);
        }

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

struct PersistentStore {
    log_file: tokio::fs::File,
    state: PersistentState,
}

impl PersistentStore {
    pub fn new(log_file: tokio::fs::File, state: PersistentState) -> Self {
        PersistentStore { log_file, state }
    }
}

impl Store for PersistentStore {
    fn save(&mut self, state: PersistentState) -> Result<(), StorageError> {
        self.state = state;
        let data = self.state.serialize();

        let rt = Runtime::new().map_err(StorageError::RuntimeInit)?;
        rt.block_on(async {
            self.log_file.write_all(&data).await?;

            tokio::fs::File::sync_all(&self.log_file).await?;

            Ok::<_, StorageError>(())
        })?;

        Ok(())
    }

    fn load(&self) -> Result<PersistentState, StorageError> {
        // Read file contents synchronously using runtime
        let rt = Runtime::new().map_err(StorageError::RuntimeInit)?;

        // TODO: change this to read one by one and deserialize on the fly to avoid loading the entire file into memory
        let data = rt.block_on(async { tokio::fs::read("raft_log.bin").await })?;

        PersistentState::deserialize(&data)
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
        assert!(
            full_result.is_ok(),
            "expected Ok for full encoded payload"
        );
    }
}
