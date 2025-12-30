use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::identifiers::SerializableUuid;
use crate::properties::PropertyValue;
use std::collections::BTreeMap;

/// Top-level response structure for the MPI Stewardship Dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MpiStewardshipDashboard {
    /// Total number of Golden Records in the system
    pub golden_count: usize,
    /// Total number of Patient records (source identities)
    pub total_patient_count: usize,
    /// Number of Golden Records currently requiring review
    pub conflict_count: usize,
    /// Detailed list of Golden Records shown on the dashboard
    pub records: Vec<StewardshipRecord>,
}

/// A summary item for the Data Stewardship Dashboard.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DashboardItem {
    /// The primary identifier (usually the Canonical MRN).
    pub id: String,
    /// The internal Golden Record UUID.
    pub internal_id: SerializableUuid,
    /// Number of Patient records merged into this Golden Record.
    pub link_density: usize,
    /// List of active Red Flags requiring review.
    pub active_flags: Vec<String>,
    /// Raw ISO timestamp from the graph.
    pub last_event: String,
    /// Computed display timestamp (maps to last_updated in your service).
    pub last_updated: String,
    /// Current status (e.g., "REQUIRES_REVIEW", "RESOLVED").
    pub stewardship_status: String,
    /// "HEALTHY" or "CRITICAL" based on the presence of flags.
    pub health_status: String,
}

/// A single Golden Record entry as displayed on the stewardship dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StewardshipRecord {
    /// First name from the canonical Golden Record
    pub first_name: String,
    /// Last name from the canonical Golden Record
    pub last_name: String,
    /// Whether this record has unresolved conflicts/red flags
    pub has_unresolved_conflict: bool,
    /// Primary MRN used as the canonical identifier
    pub primary_mrn: Option<String>,
    /// Current stewardship status (e.g., "REQUIRES_REVIEW", "STABLE")
    pub status: String,
    /// Links to source system identities contributing to this Golden Record
    pub source_links: Vec<SourceLink>,
    /// Recent evolution events for this Golden Record
    pub recent_events: Vec<GraphEventSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceLink {
    pub system_name: String,
    pub external_id: String,
    pub local_alias: String,
}

#[derive(Debug, Clone,  Serialize, Deserialize)]
pub struct GraphEventSummary {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityEvent {
    /// Unique event identifier
    pub id: SerializableUuid,
    /// Type of event (from event_type or action)
    pub event_type: Option<String>,
    /// Action name
    pub action: Option<String>,
    /// Event metadata
    pub metadata: Option<String>,
    /// Human-readable reason
    pub reason: Option<String>,
    /// ISO 8601 timestamp
    pub timestamp: String,
    /// User or system that triggered the event
    pub user_id: Option<String>,
    /// Source system
    pub source_system: Option<String>,
}
