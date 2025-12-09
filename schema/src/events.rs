use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::path::PathBuf;

/// The primary event enum for all significant actions and status changes 
/// within the storage daemon and client applications.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "details")]
pub enum StorageEvent {
    /// System has successfully initialized and is ready to accept connections.
    SystemStarted {
        /// The unique ID of the running daemon instance.
        daemon_id: String,
        /// The storage engine being used (e.g., "Sled", "RocksDB").
        engine_type: String,
        /// The port the IPC is listening on.
        port: u16,
    },

    /// A query request has been received by the daemon.
    QueryReceived(QueryExecutionDetails),

    /// A query has finished execution, successfully or not.
    QueryExecuted {
        /// Details of the query attempt.
        details: QueryExecutionDetails,
        /// The result size (e.g., number of rows returned).
        result_size: usize,
        /// Execution duration in milliseconds.
        duration_ms: u64,
    },

    /// An internal data operation (like a transaction commit or batch write) occurred.
    DataWritten {
        /// The key space or collection the write occurred in.
        namespace: String,
        /// Number of records/keys affected.
        records_affected: usize,
        /// Write duration in milliseconds.
        duration_ms: u64,
    },

    /// The state of a managed daemon process has changed.
    DaemonStateChanged(DaemonStatus),

    /// Events specific to graph data integrity and schema management.
    GraphIntegrity(GraphEvent),

    /// A critical error occurred that affects service stability but isn't fatal.
    CriticalError {
        /// A short, descriptive title of the error.
        title: String,
        /// A detailed description or backtrace of the error.
        description: String,
    },

    /// General information or debug message.
    InfoMessage {
        message: String,
    },
}

/// Helper struct for query-specific details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutionDetails {
    /// Unique identifier for this specific query execution.
    pub query_id: String,
    /// The raw query string submitted.
    pub query_text: String,
    /// The language of the query (e.g., "cypher", "sql").
    pub language: String,
    /// The user or client identifier that submitted the query (if authenticated).
    pub client_id: Option<String>,
    /// Timestamp when the query was received.
    pub timestamp: DateTime<Utc>,
}

/// Helper struct for tracking daemon process status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    /// The unique ID of the daemon instance.
    pub daemon_id: String,
    /// The process ID (PID) of the daemon.
    pub pid: Option<u32>,
    /// The new state of the daemon.
    pub state: DaemonState,
    /// Optional path to the configuration file used.
    pub config_path: PathBuf,
}

/// Defines the operational state of a managed daemon process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DaemonState {
    /// The daemon process is starting up.
    Starting,
    /// The daemon process is running and healthy.
    Running,
    /// The daemon process is shutting down gracefully.
    Stopping,
    /// The daemon process has unexpectedly terminated.
    Crashed,
    /// The daemon process is not running.
    Stopped,
    /// Daemon is running, but health checks are failing (e.g., IPC unreachable).
    Degraded,
}

/// Dedicated events for reporting graph database schema and data integrity issues.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "integrity_issue", content = "details")]
pub enum GraphEvent {
    /// A relationship's start or end node is missing (Dangling reference).
    BrokenRelationship {
        /// ID of the relationship record that is broken.
        relationship_id: String,
        /// ID of the missing node (start or end).
        missing_node_id: String,
        /// Label of the relationship type (e.g., "ACTED_IN").
        relationship_type: String,
    },
    
    /// A relationship that should exist based on schema or constraints is missing.
    MissingRelationship {
        /// The node ID where the relationship should start.
        start_node_id: String,
        /// The node ID where the relationship should end.
        end_node_id: String,
        /// The relationship type that is missing (e.g., "OWNS").
        expected_type: String,
        /// A brief explanation of the schema/business rule violated.
        rule_violation: String,
    },

    /// A node has no incoming or outgoing relationships.
    OrphanNode {
        /// ID of the isolated node.
        node_id: String,
        /// Label of the orphan node (e.g., "Product").
        node_label: String,
    },

    /// A node or relationship property violates a defined data type or constraint.
    SchemaConflict {
        /// The ID of the affected element (Node or Relationship).
        element_id: String,
        /// The key of the property causing the conflict.
        property_key: String,
        /// Description of the conflict (e.g., "Expected Integer, found String").
        conflict_description: String,
    },

    /// A relationship violates a cardinality rule or an allowed Node-Label combination.
    InconsistentRelationship {
        /// ID of the inconsistent relationship.
        relationship_id: String,
        /// The type of inconsistency (e.g., "Cardinality violation", "Forbidden Node Labels").
        inconsistency_type: String,
        /// Detailed explanation of the rule violated.
        rule_violation: String,
    },
    
    /// A generic schema change event (e.g., a new index created).
    SchemaChange {
        /// The type of change (e.g., "IndexCreated", "ConstraintDropped").
        change_type: String,
        /// The entity affected (e.g., "Node:Person", "Relationship:ACTED_IN").
        entity_affected: String,
    },
}

impl StorageEvent {
    /// Creates a QueryExecutionDetails for a new received query.
    pub fn new_query_received(query_text: String, language: String, client_id: Option<String>) -> Self {
        // NOTE: uuid crate is assumed to be available
        StorageEvent::QueryReceived(QueryExecutionDetails {
            query_id: uuid::Uuid::new_v4().to_string(), 
            query_text,
            language,
            client_id,
            timestamp: Utc::now(),
        })
    }
}
