//! Integration tests for crash safety of term and voted_for persistence
//!
//! These tests verify that `current_term` and `voted_for` are properly
//! persisted when the node becomes candidate (updates term) and when
//! it receives/gives votes (updates voted_for).

use async_trait::async_trait;
use rust_raft::node::node::RaftNode;
use rust_raft::storage::api::Store;
use rust_raft::storage::error::StorageError;
use rust_raft::storage::storage::PersistentState;
use rust_raft::storage::storage::PersistentStore;
use tokio::fs::OpenOptions;

/// Test store that wraps PersistentStore
struct TestStore {
    inner: PersistentStore,
    term_file_path: String,
    log_file_path: String,
}

impl TestStore {
    /// Create new store (first time - creates/truncates files)
    async fn create_new(term_file: &str, log_file: &str) -> Self {
        let term_file_handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(term_file)
            .await
            .expect("should open term file");

        let log_file_handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(log_file)
            .await
            .expect("should open log file");

        TestStore {
            inner: PersistentStore::new(log_file_handle, term_file_handle),
            term_file_path: term_file.to_string(),
            log_file_path: log_file.to_string(),
        }
    }

    /// Open existing store (for recovery - does NOT truncate)
    async fn open_existing(term_file: &str, log_file: &str) -> Self {
        let term_file_handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(term_file)
            .await
            .expect("should open term file");

        let log_file_handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(log_file)
            .await
            .expect("should open log file");

        TestStore {
            inner: PersistentStore::new(log_file_handle, term_file_handle),
            term_file_path: term_file.to_string(),
            log_file_path: log_file.to_string(),
        }
    }
}

#[async_trait]
impl Store for TestStore {
    async fn save(&mut self, state: PersistentState) -> Result<(), StorageError> {
        self.inner.save(state).await
    }

    async fn load(&self) -> Result<PersistentState, StorageError> {
        self.inner.load().await
    }

    async fn update_term(&mut self, term: u64) -> Result<(), StorageError> {
        self.inner.update_term(term).await
    }

    async fn update_voted_for(&mut self, voted_for: Option<String>) -> Result<(), StorageError> {
        self.inner.update_voted_for(voted_for).await
    }
}

/// Cleanup test files
async fn cleanup_test_files(term_file: &str, log_file: &str) {
    let _ = tokio::fs::remove_file(term_file).await;
    let _ = tokio::fs::remove_file(log_file).await;
}

/// Test: term is persisted when node becomes candidate
#[tokio::test]
async fn test_term_persisted_on_become_candidate() {
    let term_file = "test_candidate_term.bin";
    let log_file = "test_candidate_term_log.bin";

    cleanup_test_files(term_file, log_file).await;

    // Create store and node
    let mut store = TestStore::create_new(term_file, log_file).await;
    let mut node = RaftNode::new("node-a".to_string(), vec![], Box::new(store));

    // Verify initial state
    assert_eq!(node.get_term(), 0, "initial term should be 0");
    assert_eq!(
        node.get_voted_for(),
        None,
        "initial voted_for should be None"
    );

    // Become candidate (this should persist term=1, voted_for=self)
    node.become_candidate()
        .await
        .expect("should become candidate");

    // Verify in-memory state changed
    assert_eq!(
        node.get_term(),
        1,
        "term should be 1 after becoming candidate"
    );
    assert_eq!(
        node.get_voted_for().map(|v| v.as_str()),
        Some("node-a"),
        "voted_for should be self"
    );

    // Simulate crash - reopen store from existing files (no truncate!)
    let store = TestStore::open_existing(term_file, log_file).await;
    let loaded = store.load().await.expect("should load persisted state");

    // Verify recovered state matches what was persisted
    assert_eq!(loaded.current_term, 1, "recovered term should be 1");
    assert_eq!(
        loaded.voted_for.as_deref(),
        Some("node-a"),
        "recovered voted_for should be self"
    );

    cleanup_test_files(term_file, log_file).await;
}

/// Test: voted_for is persisted when node receives vote request and grants vote
#[tokio::test]
async fn test_voted_for_persisted_on_grant_vote() {
    let term_file = "test_voter_term.bin";
    let log_file = "test_voter_term_log.bin";

    cleanup_test_files(term_file, log_file).await;

    // Create store and node
    let mut store = TestStore::create_new(term_file, log_file).await;
    let mut node = RaftNode::new("voter".to_string(), vec![], Box::new(store));

    // Simulate receiving a vote request from candidate-x
    node.set_voted_for(Some("candidate-x".to_string()))
        .await
        .expect("should set voted_for");

    // Verify in-memory state
    assert_eq!(
        node.get_voted_for().map(|v| v.as_str()),
        Some("candidate-x"),
        "voted_for should be candidate-x"
    );

    // Simulate crash - reopen store from existing files
    let store = TestStore::open_existing(term_file, log_file).await;
    let loaded = store.load().await.expect("should load persisted state");

    assert_eq!(
        loaded.voted_for.as_deref(),
        Some("candidate-x"),
        "recovered voted_for should be 'candidate-x'"
    );

    cleanup_test_files(term_file, log_file).await;
}

/// Test: both term and voted_for persist through election cycle
#[tokio::test]
async fn test_term_and_voted_for_persist_through_election() {
    let term_file = "test_election_term.bin";
    let log_file = "test_election_log.bin";

    cleanup_test_files(term_file, log_file).await;

    // Create store and node
    let mut store = TestStore::create_new(term_file, log_file).await;
    let mut node = RaftNode::new("candidate".to_string(), vec![], Box::new(store));

    // Become candidate (term=1, voted_for=self)
    node.become_candidate().await.expect("first election");

    // Set voted_for to peer
    node.set_voted_for(Some("peer-voter".to_string()))
        .await
        .expect("should set voted_for");

    // Verify both in memory
    assert_eq!(node.get_term(), 1, "term should be 1");
    assert_eq!(
        node.get_voted_for().map(|v| v.as_str()),
        Some("peer-voter"),
        "voted_for should be 'peer-voter'"
    );

    // Simulate crash - reopen store
    let store = TestStore::open_existing(term_file, log_file).await;
    let loaded = store.load().await.expect("should load persisted state");

    assert_eq!(loaded.current_term, 1, "recovered term should be 1");
    assert_eq!(
        loaded.voted_for.as_deref(),
        Some("peer-voter"),
        "recovered voted_for should be 'peer-voter'"
    );

    cleanup_test_files(term_file, log_file).await;
}

/// Test: partial/corrupt file is handled gracefully
#[tokio::test]
async fn test_corrupt_file_handled_gracefully() {
    let term_file = "test_corrupt_term.bin";
    let log_file = "test_corrupt_log.bin";

    cleanup_test_files(term_file, log_file).await;

    // Create a store and set state
    {
        let mut store = TestStore::create_new(term_file, log_file).await;
        let mut node = RaftNode::new("node".to_string(), vec![], Box::new(store));
        node.current_term = 1;
        node.voted_for = Some("node".to_string());
    }

    // Corrupt the term file (simulating crash mid-write)
    {
        let file = OpenOptions::new()
            .write(true)
            .open(term_file)
            .await
            .expect("should open term file");

        let metadata = file.metadata().await.expect("should get metadata");
        let current_size = metadata.len();
        let mut file = file;
        file.set_len(current_size / 2)
            .await
            .expect("should truncate");
    }

    // Recovery should handle gracefully (not panic)
    let store = TestStore::open_existing(term_file, log_file).await;
    let result = store.load().await;

    match result {
        Ok(state) => {
            println!(
                "Node recovered with term={}, voted_for={:?}",
                state.current_term, state.voted_for
            );
        }
        Err(e) => {
            println!("Node failed to recover (acceptable): {}", e);
        }
    }

    cleanup_test_files(term_file, log_file).await;
}

/// Test: consecutive elections preserve correct state
#[tokio::test]
async fn test_consecutive_elections_crash_safe() {
    let term_file = "test_consecutive_election.bin";
    let log_file = "test_consecutive_election_log.bin";

    cleanup_test_files(term_file, log_file).await;

    // First election - become candidate (term=1)
    {
        let store = TestStore::create_new(term_file, log_file).await;
        let mut node = RaftNode::new("node".to_string(), vec![], Box::new(store));
        node.become_candidate().await.expect("first election");
    }

    // Second election - become candidate again (term=2)
    {
        let store = TestStore::open_existing(term_file, log_file).await;
        let mut node = RaftNode::new_with_persistence("node".to_string(), vec![], Box::new(store))
            .await
            .unwrap();
        node.become_candidate().await.expect("second election");
    }

    // Third election (term=3)
    {
        let store = TestStore::open_existing(term_file, log_file).await;
        let mut node = RaftNode::new_with_persistence("node".to_string(), vec![], Box::new(store))
            .await
            .unwrap();
        node.become_candidate().await.expect("third election");
    }

    // Simulate crash - verify final state via load()
    let store = TestStore::open_existing(term_file, log_file).await;
    let loaded = store.load().await.expect("should load persisted state");

    assert_eq!(
        loaded.current_term, 3,
        "final term should be 3 after 3 elections"
    );
    assert_eq!(
        loaded.voted_for.as_deref(),
        Some("node"),
        "voted_for should be self after each election"
    );

    cleanup_test_files(term_file, log_file).await;
}

