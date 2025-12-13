use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
// IMPORTANT: Updated imports to match internal structure
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a clinical order placed for a patient as part of their care journey.
/// This edge links a PatientJourney (timeline) vertex to an Order vertex.
/// Clinical Purpose: All orders (labs, meds, imaging) across lifetime.
/// FHIR Mapping: ServiceRequest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasOrder {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (PatientJourney)
    pub source_id: Uuid,
    /// ID of the target vertex (Order)
    pub target_id: Uuid,

    /// The date and time the order was placed or recorded.
    pub order_date: DateTime<Utc>,

    /// The current status of the order (e.g., 'active', 'completed', 'cancelled', 'draft').
    pub status: String,

    /// A brief reason or indication for why the order was placed.
    pub reason: Option<String>,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
impl HasOrder {
    pub const RELATIONSHIP_TYPE: &'static str = "HAS_ORDER";

    /// Creates a new, minimal HasOrder instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        order_date: DateTime<Utc>,
        status: String,
    ) -> Self {
        Self {
            id,
            order_date,
            status,
            reason: None,
            additional_properties: HashMap::new(),
            source_id,
            target_id,
        }
    }
}


// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasOrder {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("order_date", true)
                .with_description("The date and time the order was placed (RFC3339)."),
            PropertyConstraint::new("status", true)
                .with_description("The current status of the order (String)."),
            PropertyConstraint::new("reason", false)
                .with_description("Brief reason for the order (String)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for HasOrder {
    /// Converts the HasOrder struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("order_date".to_string(), PropertyValue::String(self.order_date.to_rfc3339()))
            .with_property("status".to_string(), PropertyValue::String(self.status.clone()));

        // Optional properties
        if let Some(val) = self.reason.clone() {
            e = e.with_property("reason".to_string(), PropertyValue::String(val));
        }

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
             e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for HasOrder {
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
        let known_keys: &[&str] = &["id", "order_date", "status", "reason"];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.clone(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the HasOrder struct
        Some(HasOrder {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            order_date: parse_datetime("order_date", edge)?,
            status: parse_string("status", edge)?,
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            // Optional fields do NOT use '?' when calling the helper
            reason: parse_string("reason", edge),

            additional_properties,
        })
    }
}
