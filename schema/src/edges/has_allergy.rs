use chrono::{DateTime, Utc};
use uuid::Uuid;
// IMPORTANT: Added PropertyValue to imports
use models::{Edge, ToEdge, Identifier, SerializableUuid, PropertyValue}; 

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
        // as required by the new Edge::new signature.
        let edge_label_identifier = Identifier::new("HAS_ALLERGY".to_string())
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

impl HasAllergy {
    /// Attempts to construct a HasAllergy struct from a generic Edge representation.
    pub fn from_edge(edge: &Edge) -> Option<Self> {
        // Fix E0283: Explicitly use as_str() for the comparison to resolve type ambiguity
        if edge.label.as_str() != "HAS_ALLERGY" { return None; }
        
        // Helper closure to parse DateTime<Utc> from property string
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                // Access the underlying string value from the PropertyValue enum
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        Some(HasAllergy {
            // Need to retrieve the string from PropertyValue before parsing
            id: edge.properties.get("id")?.as_str()?.parse().ok()?,
            // Use the renamed fields: outbound_id (source) and inbound_id (target)
            source_id: edge.outbound_id.to_string().parse().ok()?, 
            target_id: edge.inbound_id.to_string().parse().ok()?, 
            created_at: parse_datetime("created_at", edge)?,
            effective_date: parse_datetime("effective_date", edge),
        })
    }
}