use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Links a ClinicalNote document to one of its structured sections.
/// This allows for document content to be broken down into discrete, queryable components.
///
/// Edge Label: HAS_SECTION
/// Source Vertex: ClinicalNote
/// Target Vertex: SectionVertex (e.g., HPI, ROS, Assessment)
#[derive(Debug, Clone)]
pub struct HasSection {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the ClinicalNote)
    pub source_id: Uuid,
    /// ID of the target vertex (the SectionVertex)
    pub target_id: Uuid,

    /// The zero-based ordering of the section within the note.
    /// This is crucial for reconstructing the note's original flow.
    pub sequence_order: i32,

    /// The date and time when this section was added to the note.
    pub created_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasSection {
    fn edge_label() -> &'static str {
        "HAS_SECTION"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("sequence_order", true)
                .with_description("The zero-based index for ordering the section within the note."),
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the section was linked to the note (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasSection {
    /// Converts the HasSection struct into a generic Edge structure.
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
            "sequence_order".to_string(), 
            PropertyValue::Integer(self.sequence_order as i64) // Convert i32 to i64
        );
        e = e.with_property(
            "created_at".to_string(), 
            PropertyValue::String(self.created_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasSection {
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
        let sequence_order = match edge.properties.get("sequence_order")? {
            PropertyValue::Integer(val) => (*val) as i32, // Convert i64 to i32
            _ => return None,
        };

        Some(HasSection {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            sequence_order,
            created_at: parse_datetime("created_at", edge)?,
        })
    }
}
