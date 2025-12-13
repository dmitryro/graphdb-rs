use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
// IMPORTANT: Updated imports to match internal structure
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a formal plan for managing a patient's conditions, particularly chronic diseases.
/// This edge links a PatientJourney (timeline) vertex to a CarePlan vertex.
/// Clinical Purpose: Chronic disease management plans.
/// FHIR Mapping: CarePlan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasCarePlan {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (PatientJourney)
    pub source_id: Uuid,
    /// ID of the target vertex (CarePlan)
    pub target_id: Uuid,

    /// The date and time the care plan was created or became effective.
    pub effective_start: DateTime<Utc>,

    /// The date and time the care plan is scheduled to end or was discontinued.
    pub effective_end: Option<DateTime<Utc>>,

    /// The current status of the plan (e.g., 'active', 'completed', 'revoked', 'draft').
    pub status: String,

    /// The primary clinical focus or goal of the care plan (e.g., 'Diabetes Management', 'Asthma Action Plan').
    pub goal_description: Option<String>,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
impl HasCarePlan {
    pub const RELATIONSHIP_TYPE: &'static str = "HAS_CARE_PLAN";

    /// Creates a new, minimal HasCarePlan instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        effective_start: DateTime<Utc>,
        status: String,
    ) -> Self {
        Self {
            id,
            effective_start,
            effective_end: None,
            status,
            goal_description: None,
            additional_properties: HashMap::new(),
            source_id,
            target_id,
        }
    }
}


// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasCarePlan {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("effective_start", true)
                .with_description("The start date and time the care plan became effective (RFC3339)."),
            PropertyConstraint::new("effective_end", false)
                .with_description("The end date and time the care plan stopped being effective (RFC3339)."),
            PropertyConstraint::new("status", true)
                .with_description("The current status of the care plan (String)."),
            PropertyConstraint::new("goal_description", false)
                .with_description("Primary goal of the care plan (String)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for HasCarePlan {
    /// Converts the HasCarePlan struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("effective_start".to_string(), PropertyValue::String(self.effective_start.to_rfc3339()))
            .with_property("status".to_string(), PropertyValue::String(self.status.clone()));

        // Optional properties
        if let Some(val) = self.effective_end {
            e = e.with_property("effective_end".to_string(), PropertyValue::String(val.to_rfc3339()));
        }
        if let Some(val) = self.goal_description.clone() {
            e = e.with_property("goal_description".to_string(), PropertyValue::String(val));
        }

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
             e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for HasCarePlan {
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
        let known_keys: &[&str] = &["id", "effective_start", "effective_end", "status", "goal_description"];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.clone(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the HasCarePlan struct
        Some(HasCarePlan {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            effective_start: parse_datetime("effective_start", edge)?,
            status: parse_string("status", edge)?,
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            // Optional fields do NOT use '?' when calling the helper
            effective_end: parse_datetime("effective_end", edge),
            goal_description: parse_string("goal_description", edge),

            additional_properties,
        })
    }
}
