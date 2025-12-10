use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
// IMPORTANT: Updated imports to match internal structure
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the relationship type for a patient's encounter with a healthcare entity.
/// This edge is designed to be flexible, allowing different source vertices (Patient, EpisodeOfCare)
/// to link to a target vertex (Encounter).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasEncounter {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (e.g., Patient, EpisodeOfCare)
    pub source_id: Uuid,
    /// ID of the target vertex (e.g., Encounter)
    pub target_id: Uuid,

    /// The date and time the relationship (encounter) began. 
    pub start_time: DateTime<Utc>,

    /// The date and time the relationship (encounter) ended. If null, the encounter is ongoing.
    pub end_time: Option<DateTime<Utc>>,

    /// The duration of the relationship/encounter in minutes. Calculated from start and end times.
    pub duration_minutes: Option<i64>,

    /// A brief description of the primary reason for the encounter.
    pub reason_for_encounter: String,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
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
        
        // Calculate duration and store in minutes (i64)
        let duration: Duration = end_time.signed_duration_since(self.start_time);
        self.duration_minutes = Some(duration.num_minutes());
        self.end_time = Some(end_time);
        Ok(())
    }
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


// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for HasEncounter {
    /// Converts the HasEncounter struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("start_time".to_string(), PropertyValue::String(self.start_time.to_rfc3339()))
            .with_property("reason_for_encounter".to_string(), PropertyValue::String(self.reason_for_encounter.clone()));

        // Optional properties
        if let Some(val) = self.end_time {
            e = e.with_property("end_time".to_string(), PropertyValue::String(val.to_rfc3339()));
        }
        if let Some(val) = self.duration_minutes {
            // i64 converted to String
            e = e.with_property("duration_minutes".to_string(), PropertyValue::String(val.to_string()));
        }

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
             e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for HasEncounter {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::RELATIONSHIP_TYPE { return None; }
        
        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        // Helper to parse string property into i64
        let parse_i64 = |prop_name: &str, edge: &Edge| -> Option<i64> {
            edge.properties.get(prop_name).and_then(|v| v.as_str()?.parse().ok())
        };

        // 1. Collect Additional Properties (must run before returning the struct)
        let mut additional_properties = HashMap::new();
        // Define all standard keys explicitly to exclude them from the dynamic map
        let known_keys: &[&str] = &["id", "start_time", "end_time", "duration_minutes", "reason_for_encounter"];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.clone(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the HasEncounter struct
        Some(HasEncounter {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            start_time: parse_datetime("start_time", edge)?,
            reason_for_encounter: edge.properties.get("reason_for_encounter")?.as_str()?.to_string(),
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            // Optional fields do NOT use '?' when calling the helper, 
            // as the helper returns Option and we want to preserve 'None' if the property is missing.
            end_time: parse_datetime("end_time", edge),
            duration_minutes: parse_i64("duration_minutes", edge),

            additional_properties,
        })
    }
}