use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
// IMPORTANT: Updated imports to match internal structure
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the final outcome or destination of a patient following a clinical encounter.
/// This edge links a PatientJourney (timeline) vertex to a Disposition vertex.
/// Clinical Purpose: Final outcome of each encounter.
/// FHIR Mapping: Encounter.hospitalization.dischargeDisposition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasDisposition {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (PatientJourney)
    pub source_id: Uuid,
    /// ID of the target vertex (Disposition)
    pub target_id: Uuid,

    /// The date and time the disposition was recorded or became effective (e.g., discharge time).
    pub disposition_time: DateTime<Utc>,

    /// The category or type of disposition (e.g., 'Discharged', 'Transfer', 'Expired').
    pub disposition_type: String,

    /// A detailed description or code for the final destination (e.g., 'Home', 'Skilled Nursing Facility').
    pub destination_detail: Option<String>,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
impl HasDisposition {
    pub const RELATIONSHIP_TYPE: &'static str = "HAS_DISPOSITION";

    /// Creates a new, minimal HasDisposition instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        disposition_time: DateTime<Utc>,
        disposition_type: String,
    ) -> Self {
        Self {
            id,
            disposition_time,
            disposition_type,
            destination_detail: None,
            additional_properties: HashMap::new(),
            source_id,
            target_id,
        }
    }
}


// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasDisposition {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("disposition_time", true)
                .with_description("The date and time the disposition was recorded (RFC3339)."),
            PropertyConstraint::new("disposition_type", true)
                .with_description("The primary category of disposition (String)."),
            PropertyConstraint::new("destination_detail", false)
                .with_description("Detailed destination or outcome (String)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for HasDisposition {
    /// Converts the HasDisposition struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("disposition_time".to_string(), PropertyValue::String(self.disposition_time.to_rfc3339()))
            .with_property("disposition_type".to_string(), PropertyValue::String(self.disposition_type.clone()));

        // Optional properties
        if let Some(val) = self.destination_detail.clone() {
            e = e.with_property("destination_detail".to_string(), PropertyValue::String(val));
        }

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
             e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for HasDisposition {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::RELATIONSHIP_TYPE { return None; }
        
        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        // Helper to parse string property into String (wrapped in Option for optional fields)
        let parse_string = |prop_name: &str, edge: &Edge| -> Option<String> {
            edge.properties.get(prop_name).and_then(|v| v.as_str()).map(|s| s.to_string())
        };

        // 1. Collect Additional Properties (must run before returning the struct)
        let mut additional_properties = HashMap::new();
        // Define all standard keys explicitly to exclude them from the dynamic map
        let known_keys: &[&str] = &["id", "disposition_time", "disposition_type", "destination_detail"];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.clone(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the HasDisposition struct
        Some(HasDisposition {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            disposition_time: parse_datetime("disposition_time", edge)?,
            disposition_type: parse_string("disposition_type", edge)?,
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            // Optional fields do NOT use '?' when calling the helper
            destination_detail: parse_string("destination_detail", edge),

            additional_properties,
        })
    }
}
