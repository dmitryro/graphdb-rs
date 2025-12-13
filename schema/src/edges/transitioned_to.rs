use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
// IMPORTANT: Updated imports to match internal structure
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a sequential transition or handoff of care from one encounter to the next.
/// This edge links a preceding Encounter (Encounter A) to a succeeding Encounter (Encounter B).
/// Clinical Purpose: Care handoff: ED -> ICU -> Floor -> SNF -> Home.
/// FHIR Mapping: Encounter.partOf (in reverse, or indicating continuation).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitionedTo {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (Encounter A - the preceding encounter)
    pub source_id: Uuid,
    /// ID of the target vertex (Encounter B - the succeeding encounter)
    pub target_id: Uuid,

    /// The date and time the transition/handoff occurred.
    pub transition_time: DateTime<Utc>,

    /// The type of care setting being transitioned *from* (e.g., 'ED', 'ICU', 'Floor').
    pub source_setting: String,

    /// The type of care setting being transitioned *to* (e.g., 'ICU', 'SNF', 'Home').
    pub target_setting: String,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
impl TransitionedTo {
    pub const RELATIONSHIP_TYPE: &'static str = "TRANSITIONED_TO";

    /// Creates a new, minimal TransitionedTo instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        transition_time: DateTime<Utc>,
        source_setting: String,
        target_setting: String,
    ) -> Self {
        Self {
            id,
            transition_time,
            source_setting,
            target_setting,
            additional_properties: HashMap::new(),
            source_id,
            target_id,
        }
    }
}


// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for TransitionedTo {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("transition_time", true)
                .with_description("The date and time the care handoff occurred (RFC3339)."),
            PropertyConstraint::new("source_setting", true)
                .with_description("The type of care setting being transitioned FROM (String)."),
            PropertyConstraint::new("target_setting", true)
                .with_description("The type of care setting being transitioned TO (String)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for TransitionedTo {
    /// Converts the TransitionedTo struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("transition_time".to_string(), PropertyValue::String(self.transition_time.to_rfc3339()))
            .with_property("source_setting".to_string(), PropertyValue::String(self.source_setting.clone()))
            .with_property("target_setting".to_string(), PropertyValue::String(self.target_setting.clone()));

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
             e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for TransitionedTo {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::RELATIONSHIP_TYPE { return None; }
        
        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        // Helper to parse string property into String (must succeed for mandatory fields)
        let parse_string = |prop_name: &str, edge: &Edge| -> Option<String> {
            edge.properties.get(prop_name).and_then(|v| v.as_str()).map(|s| s.to_string())
        };

        // 1. Collect Additional Properties (must run before returning the struct)
        let mut additional_properties = HashMap::new();
        // Define all standard keys explicitly to exclude them from the dynamic map
        let known_keys: &[&str] = &["id", "transition_time", "source_setting", "target_setting"];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.clone(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the TransitionedTo struct
        Some(TransitionedTo {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            transition_time: parse_datetime("transition_time", edge)?,
            source_setting: parse_string("source_setting", edge)?,
            target_setting: parse_string("target_setting", edge)?,
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            additional_properties,
        })
    }
}
