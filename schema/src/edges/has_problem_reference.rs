use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Links a ClinicalNote (Source) to an existing, active Problem (Target) in the
/// patient's problem list, indicating that the note directly references or
/// addresses that specific problem.
///
/// Edge Label: HAS_PROBLEM_REFERENCE
/// Source Vertex: ClinicalNote
/// Target Vertex: Problem
#[derive(Debug, Clone)]
pub struct HasProblemReference {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the ClinicalNote)
    pub source_id: Uuid,
    /// ID of the target vertex (the Problem list entry)
    pub target_id: Uuid,

    /// The specific text snippet in the note that references the problem.
    /// E.g., "The patient's Type 2 Diabetes is well controlled."
    pub matched_text: String,

    /// The date and time when this reference link was established.
    pub established_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasProblemReference {
    fn edge_label() -> &'static str {
        "HAS_PROBLEM_REFERENCE"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("matched_text", true)
                .with_description("The text in the clinical note that references the problem."),
            PropertyConstraint::new("established_at", true)
                .with_description("Timestamp when the reference link was created (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasProblemReference {
    /// Converts the HasProblemReference struct into a generic Edge structure.
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
            "matched_text".to_string(), 
            PropertyValue::String(self.matched_text.clone())
        );
        e = e.with_property(
            "established_at".to_string(), 
            PropertyValue::String(self.established_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasProblemReference {
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
        let matched_text = edge.properties.get("matched_text")?.as_str()?.to_string();

        Some(HasProblemReference {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            matched_text,
            established_at: parse_datetime("established_at", edge)?,
        })
    }
}
