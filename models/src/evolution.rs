use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::identifiers::SerializableUuid;
use crate::properties::PropertyValue;
use std::collections::BTreeMap;


/// Represents a single state change or transaction in the Identity Graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvolutionStep {
    /// The ID of the IdentityEvent vertex.
    pub event_id: SerializableUuid,
    /// The type of action (mapped from 'event_type').
    pub action: Option<PropertyValue>,
    /// ISO 8601 timestamp of the event.
    pub timestamp: Option<PropertyValue>,
    /// Who or what performed the change.
    pub user_id: String,
    /// Why the change occurred (logical code).
    pub reason: Option<String>,
    /// Human-readable explanation of the event.
    pub description: Option<PropertyValue>,
    /// The system where the record originated.
    pub source_system: Option<PropertyValue>,
    /// Any red flags associated with this specific step.
    pub flags: Option<PropertyValue>,
    /// Metadata associated with the change.
    pub metadata: BTreeMap<String, PropertyValue>,
}

/// A complete trace of an identity's history across the Graph of Events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineageReport {
    /// The External MPI ID or MRN being reported on.
    pub mpi_id: String,
    /// The internal Golden Record UUID.
    pub root_id: SerializableUuid,
    /// Chronological list of events (mapped to 'steps' in your service).
    pub steps: Vec<EvolutionStep>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MpiSnapshot {
    pub first_name: String,
    pub last_name: String,
    pub dob: DateTime<Utc>,
    pub gender: Option<String>,
    pub cross_refs: Vec<SnapshotXRef>,
    pub version_id: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotXRef {
    pub system: String,
    pub mrn: String,
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct LineageReportTrace {
    pub first_name: String,
    pub last_name: String,
    pub red_flag_count: usize,
    pub history_chain: Vec<HistoryEntry>,
}

#[derive(Debug, Deserialize)]
pub struct HistoryEntry {
    pub timestamp: DateTime<Utc>,
    pub action_type: String,
    pub user_id: String,
    pub source_system: String,
    pub change_reason: Option<String>,
    pub mutations: Vec<FieldMutation>,
    pub is_structural: bool,
    pub involved_identity_alias: String,
}

#[derive(Debug, Deserialize)]
pub struct FieldMutation {
    pub field: String,
    pub old_val: String,
    pub new_val: String,
}

#[derive(Debug, Deserialize)]
pub struct MpiStewardshipDashboard {
    pub golden_count: usize,
    pub total_patient_count: usize,
    pub conflict_count: usize,
    pub records: Vec<StewardshipRecord>,
}

#[derive(Debug, Deserialize)]
pub struct StewardshipRecord {
    pub first_name: String,
    pub last_name: String,
    pub has_unresolved_conflict: bool,
    pub primary_mrn: Option<String>,
    pub status: String,
    pub source_links: Vec<SourceLink>,
    pub recent_events: Vec<GraphEventSummary>,
}

#[derive(Debug, Deserialize)]
pub struct SourceLink {
    pub system_name: String,
    pub external_id: String,
    pub local_alias: String,
}

#[derive(Debug, Deserialize)]
pub struct GraphEventSummary {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub description: String,
}