use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Links a parent Section (or SectionVertex) to a nested, child Subsection (SectionVertex).
/// This defines the internal, hierarchical structure of content within a major section.
///
/// Edge Label: HAS_SUBSECTION
/// Source Vertex: SectionVertex (Parent)
/// Target Vertex: SectionVertex (Child/Nested Subsection)
#[derive(Debug, Clone)]
pub struct HasSubsection {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the parent Section)
    pub source_id: Uuid,
    /// ID of the target vertex (the child Subsection)
    pub target_id: Uuid,

    /// The zero-based ordering of the subsection within its parent section.
    /// This is crucial for reconstructing the sequence of elements.
    pub sequence_order: i32,

    /// The date and time when this subsection relationship was established.
    pub established_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasSubsection {
    fn edge_label() -> &'static str {
        "HAS_SUBSECTION"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("sequence_order", true)
                .with_description("The zero-based index for ordering the subsection within its parent."),
            PropertyConstraint::new("established_at", true)
                .with_description("Timestamp when the subsection link was established (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasSubsection {
    /// Converts the HasSubsection struct into a generic Edge structure.
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
            "established_at".to_string(), 
            PropertyValue::String(self.established_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasSubsection {
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

        Some(HasSubsection {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            sequence_order,
            established_at: parse_datetime("established_at", edge)?,
        })
    }
}
