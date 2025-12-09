use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue}; 
use crate::edges::relationship::EdgeSchema;

/// Represents the relationship type for a patient's encounter with a healthcare entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasEncounter {
    /// The unique identifier for this specific edge instance.
    pub id: String,
    
    /// IDs are required for ToEdge/FromEdge, assuming they are Uuid-based
    pub source_id: Uuid,
    pub target_id: Uuid,

    /// The date and time the relationship (encounter) began. 
    pub start_time: DateTime<Utc>,

    /// The date and time the relationship (encounter) ended. If null, the encounter is ongoing.
    pub end_time: Option<DateTime<Utc>>,

    /// The duration of the relationship/encounter in minutes. Calculated from start and end times.
    pub duration_minutes: Option<i64>,

    /// A brief description of the primary reason for the encounter.
    pub reason_for_encounter: String,
    
    /// A map for any additional, non-standard properties.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasEncounter {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("start_time", true)
                .with_description("Start time of the encounter (RFC3339)."),
            PropertyConstraint::new("end_time", false)
                .with_description("End time of the encounter (RFC3339)."),
            PropertyConstraint::new("duration_minutes", false)
                .with_description("Duration in minutes (i64, stored as String)."),
            PropertyConstraint::new("reason_for_encounter", true)
                .with_description("Primary reason for the encounter."),
        ]
    }
}
// --- END EdgeSchema ---

impl ToEdge for HasEncounter {
    fn to_edge(&self) -> Edge {
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("start_time".to_string(), PropertyValue::String(self.start_time.to_rfc3339()))
            .with_property("reason_for_encounter".to_string(), PropertyValue::String(self.reason_for_encounter.clone()));

        if let Some(val) = self.end_time {
            e = e.with_property("end_time".to_string(), PropertyValue::String(val.to_rfc3339()));
        }
        if let Some(val) = self.duration_minutes {
            e = e.with_property("duration_minutes".to_string(), PropertyValue::String(val.to_string()));
        }

        for (key, val) in &self.additional_properties {
             e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}

impl FromEdge for HasEncounter {
    // FIX E0185: Removed '&self' to match the trait definition (now a static method)
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::RELATIONSHIP_TYPE { return None; }
        
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        let parse_i64 = |prop_name: &str, edge: &Edge| -> Option<i64> {
            edge.properties.get(prop_name).and_then(|v| v.as_str()?.parse().ok())
        };

        let mut additional_properties = HashMap::new();
        let known_keys: &[&str] = &["id", "start_time", "end_time", "duration_minutes", "reason_for_encounter"];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.clone(), s.clone());
                }
            }
        }
        
        Some(HasEncounter {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            start_time: parse_datetime("start_time", edge)?,
            end_time: parse_datetime("end_time", edge),
            duration_minutes: parse_i64("duration_minutes", edge),
            reason_for_encounter: edge.properties.get("reason_for_encounter")?.as_str()?.to_string(),
            additional_properties,
            // These fixes for E0599 are retained
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,
        })
    }
}

impl HasEncounter {
    pub const RELATIONSHIP_TYPE: &'static str = "HAS_ENCOUNTER";

    /// Creates a new, minimal HasEncounter instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        start_time: DateTime<Utc>,
        reason: String,
    ) -> Self {
        Self {
            id,
            start_time,
            end_time: None,
            duration_minutes: None,
            reason_for_encounter: reason,
            additional_properties: HashMap::new(),
            source_id,
            target_id,
        }
    }

    /// Sets the end time for the encounter and calculates the duration.
    pub fn end_encounter(&mut self, end_time: DateTime<Utc>) -> Result<(), String> {
        if end_time < self.start_time {
            return Err("End time cannot be before start time.".to_string());
        }
        
        let duration = end_time.signed_duration_since(self.start_time);
        self.duration_minutes = Some(duration.num_minutes());
        self.end_time = Some(end_time);
        Ok(())
    }
}
