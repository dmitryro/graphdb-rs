use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Links an OriginalNote (Source) to a subsequent AddendumNote (Target), indicating
/// that the target note provides later correction, clarification, or additional
/// information related to the original source note.
///
/// Edge Label: HAS_ADDENDUM
/// Source Vertex: ClinicalNote (OriginalNote)
/// Target Vertex: ClinicalNote (AddendumNote)
#[derive(Debug, Clone)]
pub struct HasAddendum {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the Original Clinical Note)
    pub source_id: Uuid,
    /// ID of the target vertex (the subsequent Addendum Note)
    pub target_id: Uuid,

    /// The reason or context for the addendum (e.g., "Correction of dosage," "Late lab result addition").
    pub reason: String,

    /// The date and time when this addendum link was established.
    pub established_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasAddendum {
    fn edge_label() -> &'static str {
        "HAS_ADDENDUM"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("reason", true)
                .with_description("The reason or context for the addendum."),
            PropertyConstraint::new("established_at", true)
                .with_description("Timestamp when the addendum link was created (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasAddendum {
    /// Converts the HasAddendum struct into a generic Edge structure.
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
            "reason".to_string(), 
            PropertyValue::String(self.reason.clone())
        );
        e = e.with_property(
            "established_at".to_string(), 
            PropertyValue::String(self.established_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasAddendum {
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
        let reason = edge.properties.get("reason")?.as_str()?.to_string();

        Some(HasAddendum {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            reason,
            established_at: parse_datetime("established_at", edge)?,
        })
    }
}
