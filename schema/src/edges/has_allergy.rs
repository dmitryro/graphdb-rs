use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
// IMPORTANT: Updated imports to match internal structure
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the relationship (edge) between a Patient and an Allergy record,
/// signifying that the Patient has been diagnosed with that specific allergy.
#[derive(Debug, Clone)]
pub struct HasAllergy {
    /// Unique ID for this relationship instance
    pub id: i32,
    /// ID of the source vertex (Patient)
    pub source_id: i32,
    /// ID of the target vertex (Allergy)
    pub target_id: i32,
    /// Timestamp when this relationship record was created
    pub created_at: DateTime<Utc>,
    /// Optional date when the relationship/allergy status was first effective or recorded
    pub effective_date: Option<DateTime<Utc>>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasAllergy {
    fn edge_label() -> &'static str {
        "HAS_ALLERGY"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (i32, stored as String)."),
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp of relationship creation (RFC3339)."),
            PropertyConstraint::new("effective_date", false)
                .with_description("Date allergy status became effective (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


impl ToEdge for HasAllergy {
    /// Converts the HasAllergy struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid by parsing them as UUIDs
        let outbound_id: SerializableUuid = Uuid::parse_str(&self.source_id.to_string())
            .expect("Invalid source_id UUID format")
            .into();
        let inbound_id: SerializableUuid = Uuid::parse_str(&self.target_id.to_string())
            .expect("Invalid target_id UUID format")
            .into();
        
        // 2. Fix E0308: Convert the edge label string into an Identifier 
        let edge_label_identifier = Identifier::new(Self::edge_label().to_string())
            .expect("Invalid edge label identifier");
        
        let mut e = Edge::new(
            outbound_id, 
            edge_label_identifier, // Use Identifier for the label/type
            inbound_id
        );
        
        // Fix E0308 (PropertyValue): All property values must be wrapped in PropertyValue::String(...)
        e = e.with_property(
            "id".to_string(), 
            PropertyValue::String(self.id.to_string())
        );
        e = e.with_property(
            "created_at".to_string(), 
            PropertyValue::String(self.created_at.to_rfc3339())
        );
        
        if let Some(ref val) = self.effective_date {
            e = e.with_property(
                "effective_date".to_string(), 
                PropertyValue::String(val.to_rfc3339())
            );
        }
        
        e
    }
}

