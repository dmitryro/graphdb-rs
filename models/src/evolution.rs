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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineageReportTrace {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub red_flag_count:Option<usize>,
    pub history_chain: Option<Vec<HistoryEntry>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub timestamp: Option<DateTime<Utc>>,
    pub action_type: Option<String>,
    pub user_id: Option<String>,
    pub source_system: Option<String>,
    pub change_reason: Option<String>,
    pub mutations: Option<Vec<FieldMutation>>,
    pub is_structural: Option<bool>,
    pub involved_identity_alias: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldMutation {
    pub field: Option<String>,
    pub old_val: Option<String>,
    pub new_val: Option<String>,
}
