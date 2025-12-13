use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a deterministic blocking key assigned to a Patient record, linking
/// it to a dedicated BlockingKeyVertex. These keys are used during the
/// initial blocking phase of the Master Patient Index (MPI) matching process.
///
/// Edge Label: HAS_BLOCKING_KEY
/// Source Vertex: Patient
/// Target Vertex: BlockingKeyVertex
#[derive(Debug, Clone)]
pub struct HasBlockingKey {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (Patient)
    pub source_id: Uuid,
    /// ID of the target vertex (BlockingKeyVertex)
    pub target_id: Uuid,

    /// The generated key value (e.g., "S53019800101").
    pub key_value: String,

    /// The method or algorithm used to generate the key (e.g., "SOUNDEX_DOB", "FIRST3_LAST3").
    pub key_type: String,

    /// The date and time when this blocking key was first assigned.
    pub created_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasBlockingKey {
    fn edge_label() -> &'static str {
        "HAS_BLOCKING_KEY"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("key_value", true)
                .with_description("The deterministic blocking key string."),
            PropertyConstraint::new("key_type", true)
                .with_description("The algorithm or method used to generate the key."),
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the key was created (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasBlockingKey {
    /// Converts the HasBlockingKey struct into a generic Edge structure.
    fn to_edge(&self) -> Edge {
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::edge_label().to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id);

        // Add relationship-specific properties
        e = e.with_property(
            "id".to_string(), 
            PropertyValue::String(self.id.clone())
        );
        e = e.with_property(
            "key_value".to_string(), 
            PropertyValue::String(self.key_value.clone())
        );
        e = e.with_property(
            "key_type".to_string(), 
            PropertyValue::String(self.key_type.clone())
        );
        e = e.with_property(
            "created_at".to_string(), 
            PropertyValue::String(self.created_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasBlockingKey {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::edge_label() { return None; }

        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        // 1. Convert IDs from SerializableUuid back to Uuid
        let source_id = Uuid::parse_str(&edge.outbound_id.to_string()).ok()?; 
        let target_id = Uuid::parse_str(&edge.inbound_id.to_string()).ok()?;

        // 2. Safely extract mandatory properties
        Some(HasBlockingKey {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            key_value: edge.properties.get("key_value")?.as_str()?.to_string(),
            key_type: edge.properties.get("key_type")?.as_str()?.to_string(),
            created_at: parse_datetime("created_at", edge)?,
        })
    }
}
