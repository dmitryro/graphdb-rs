use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
// IMPORTANT: Updated imports to match internal structure
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents an unplanned readmission event occurring within 30 days of the preceding encounter's discharge.
/// This edge links the initial Encounter (Encounter A) to the readmission Encounter (Encounter B).
/// Clinical Purpose: Readmission tracking for quality metrics.
/// FHIR Mapping: CMS Measure (e.g., Readmission Rate).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadmittedWithin30Days {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (Encounter A - the initial discharge)
    pub source_id: Uuid,
    /// ID of the target vertex (Encounter B - the subsequent readmission)
    pub target_id: Uuid,

    /// The date and time the patient was readmitted (start time of Encounter B).
    pub readmission_time: DateTime<Utc>,

    /// The number of days between the discharge of Encounter A and the start of Encounter B.
    pub days_since_discharge: i64,

    /// The primary reason or diagnosis that led to the readmission.
    pub readmission_reason: Option<String>,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
impl ReadmittedWithin30Days {
    pub const RELATIONSHIP_TYPE: &'static str = "READMITTED_WITHIN_30_DAYS";

    /// Creates a new ReadmittedWithin30Days instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        readmission_time: DateTime<Utc>,
        days_since_discharge: i64,
    ) -> Self {
        Self {
            id,
            readmission_time,
            days_since_discharge,
            readmission_reason: None,
            additional_properties: HashMap::new(),
            source_id,
            target_id,
        }
    }
}


// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for ReadmittedWithin30Days {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("readmission_time", true)
                .with_description("The date and time of the readmission (RFC3339)."),
            PropertyConstraint::new("days_since_discharge", true)
                .with_description("The number of days between discharge and readmission (i64)."),
            PropertyConstraint::new("readmission_reason", false)
                .with_description("Primary reason for readmission (String)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for ReadmittedWithin30Days {
    /// Converts the ReadmittedWithin30Days struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("readmission_time".to_string(), PropertyValue::String(self.readmission_time.to_rfc3339()))
            .with_property("days_since_discharge".to_string(), PropertyValue::String(self.days_since_discharge.to_string()));

        // Optional properties
        if let Some(val) = self.readmission_reason.clone() {
            e = e.with_property("readmission_reason".to_string(), PropertyValue::String(val));
        }

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
             e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for ReadmittedWithin30Days {
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

        // Helper to parse string property into String (wrapped in Option for optional fields)
        let parse_string = |prop_name: &str, edge: &Edge| -> Option<String> {
            edge.properties.get(prop_name).and_then(|v| v.as_str()).map(|s| s.to_string())
        };

        // 1. Collect Additional Properties (must run before returning the struct)
        let mut additional_properties = HashMap::new();
        // Define all standard keys explicitly to exclude them from the dynamic map
        let known_keys: &[&str] = &["id", "readmission_time", "days_since_discharge", "readmission_reason"];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.clone(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the ReadmittedWithin30Days struct
        Some(ReadmittedWithin30Days {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            readmission_time: parse_datetime("readmission_time", edge)?,
            days_since_discharge: parse_i64("days_since_discharge", edge)?,
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            // Optional fields do NOT use '?' when calling the helper
            readmission_reason: parse_string("readmission_reason", edge),

            additional_properties,
        })
    }
}
