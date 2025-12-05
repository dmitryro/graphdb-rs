// lib/src/history/mod.rs

//! The history module manages the auditing and storage of user-executed commands
//! using a Sled-backed non-blocking registry, similar to the Daemon Registry.

// Declare the modules contained in the history directory
pub mod history_types;
pub mod history_service;

// Re-export core types and the global instance for ease of use
// This allows users to import HistoryMetadata and GLOBAL_HISTORY_SERVICE directly
// from `crate::history` instead of `crate::history::history_types` and `crate::history::history_service`.

pub use history_types::{
    HistoryMetadata,
    HistoryFilter,
    HistoryStatus,
};

pub use history_service::{
    HistoryService,
    HistoryServiceWrapper,
    GLOBAL_HISTORY_SERVICE,
};
