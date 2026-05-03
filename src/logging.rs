//! Structured logging for Raft consensus implementation.
//!
//! Uses the `tracing` crate for structured logging.
//! Initialize with `logging::init()` at the start of your application.
//!
//! ## Usage
//!
//! ```rust
//! use logging;
//!
//! // At the start of main()
//! logging::init();
//!
//! // Then use tracing directly:
//! tracing::info!(node_id = "node-1", term = 5, "election started");
//! tracing::debug!(target = "node-2", success = true, "rpc call");
//! tracing::error!(error = "connection refused", "rpc failed");
//! ```

use std::sync::Once;
use tracing::Level;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize logging with human-readable output (default)
///
/// Output includes: timestamp, level, target, message
pub fn init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        tracing_subscriber::registry()
            .with(filter)
            .with(
                fmt::layer()
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_file(true)
                    .with_line_number(true),
            )
            .init();
    });
}

/// Initialize logging with JSON format (better for machine parsing)
pub fn init_json() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .init();
    });
}

/// Initialize logging with pretty (human-readable) format
pub fn init_pretty() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));

        tracing_subscriber::registry()
            .with(filter)
            .with(
                fmt::layer()
                    .with_target(true)
                    .with_thread_ids(false)
                    .with_file(true)
                    .with_line_number(true)
                    .pretty(),
            )
            .init();
    });
}

/// Initialize logging with specific level
pub fn init_with_level(level: Level) {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        tracing_subscriber::registry()
            .with(fmt::layer().with_target(true))
            .with(tracing_subscriber::filter::LevelFilter::from_level(level))
            .init();
    });
}

/// Set global log level at runtime
#[allow(dead_code)]
pub fn set_level(level: Level) {
    // Note: This requires tracing-subscriber's reload feature
    // For simplicity, just use init() variants instead
}

// Re-export tracing for convenience
pub use tracing;
