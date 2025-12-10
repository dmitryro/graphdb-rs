use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the relationship between a Patient and their PatientJourney.
/// This edge links a patient to their master timeline of all care events across their lifetime.
/// 
/// Edge Label: HAS_JOURNEY
/// Source Vertex: Patient
/// Target Vertex: PatientJourney
/// 
/// Clinical Purpose: Master timeline of all care events across lifetime
/// FHIR Mapping: CarePlan + EpisodeOfCare
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasJourney {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (Patient)
    pub source_id: Uuid,
    /// ID of the target vertex (PatientJourney)
    pub target_id: Uuid,

    /// The date and time when this journey was initiated.
    pub journey_start_date: DateTime<Utc>,

    /// The date and time when this journey was concluded. If null, the journey is ongoing.
    pub journey_end_date: Option<DateTime<Utc>>,

    /// The primary condition or reason for this journey (e.g., "Diabetes Management", "Cancer Treatment").
    pub primary_condition: String,

    /// Current status of the journey (e.g., "Active", "Completed", "OnHold", "Cancelled").
    pub status: String,

    /// Optional description or notes about this journey.
    pub description: Option<String>,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
impl HasJourney {
    pub const RELATIONSHIP_TYPE: &'static str = "HAS_JOURNEY";

    /// Creates a new, minimal HasJourney instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        journey_start_date: DateTime<Utc>,
        primary_condition: String,
    ) -> Self {
        Self {
            id,
            source_id,
            target_id,
            journey_start_date,
            journey_end_date: None,
            primary_condition,
            status: "Active".to_string(),
            description: None,
            additional_properties: HashMap::new(),
        }
    }

    /// Concludes the journey by setting the end date and updating status.
    pub fn conclude_journey(&mut self, end_date: DateTime<Utc>, status: String) -> Result<(), String> {
        if end_date < self.journey_start_date {
            return Err("Journey end date cannot be before start date.".to_string());
        }
        
        self.journey_end_date = Some(end_date);
        self.status = status;
        Ok(())
    }

    /// Updates the status of the journey.
    pub fn update_status(&mut self, new_status: String) {
        self.status = new_status;
    }
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasJourney {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("journey_start_date", true)
                .with_description("Start date of the patient journey (RFC3339)."),
            PropertyConstraint::new("journey_end_date", false)
                .with_description("End date of the patient journey (RFC3339)."),
            PropertyConstraint::new("primary_condition", true)
                .with_description("Primary condition or reason for this journey."),
            PropertyConstraint::new("status", true)
                .with_description("Current status of the journey (Active, Completed, OnHold, Cancelled)."),
            PropertyConstraint::new("description", false)
                .with_description("Optional description or notes about this journey."),
        ]
    }
}
// --- END EdgeSchema ---

// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for HasJourney {
    /// Converts the HasJourney struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("journey_start_date".to_string(), PropertyValue::String(self.journey_start_date.to_rfc3339()))
            .with_property("primary_condition".to_string(), PropertyValue::String(self.primary_condition.clone()))
            .with_property("status".to_string(), PropertyValue::String(self.status.clone()));

        // Optional properties
        if let Some(val) = self.journey_end_date {
            e = e.with_property("journey_end_date".to_string(), PropertyValue::String(val.to_rfc3339()));
        }
        if let Some(ref val) = self.description {
            e = e.with_property("description".to_string(), PropertyValue::String(val.clone()));
        }

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
            e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}

// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for HasJourney {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::RELATIONSHIP_TYPE { return None; }
        
        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };

        // 1. Collect Additional Properties (must run before returning the struct)
        let mut additional_properties = HashMap::new();
        // Define all standard keys explicitly to exclude them from the dynamic map
        let known_keys: &[&str] = &[
            "id", 
            "journey_start_date", 
            "journey_end_date", 
            "primary_condition", 
            "status", 
            "description"
        ];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.to_string(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the HasJourney struct
        Some(HasJourney {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            journey_start_date: parse_datetime("journey_start_date", edge)?,
            primary_condition: edge.properties.get("primary_condition")?.as_str()?.to_string(),
            status: edge.properties.get("status")?.as_str()?.to_string(),
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            // Optional fields do NOT use '?' when calling the helper, 
            // as the helper returns Option and we want to preserve 'None' if the property is missing.
            journey_end_date: parse_datetime("journey_end_date", edge),
            description: edge.properties.get("description").and_then(|v| v.as_str()).map(|s| s.to_string()),

            additional_properties,
        })
    }
}
