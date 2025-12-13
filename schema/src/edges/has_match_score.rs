use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use models::properties::SerializableFloat; // Added import for the float wrapper
use crate::edges::relationship::EdgeSchema;

/// Links an authoritative MasterPatientIndex record to the detailed result (MatchScoreVertex)
/// of a probabilistic matching calculation, providing an audit trail for consolidation decisions.
///
/// Edge Label: HAS_MATCH_SCORE
/// Source Vertex: MasterPatientIndex (The confirmed grouping record)
/// Target Vertex: MatchScoreVertex (The dedicated vertex storing score details)
#[derive(Debug, Clone)]
pub struct HasMatchScore {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (MasterPatientIndex)
    pub source_id: Uuid,
    /// ID of the target vertex (MatchScoreVertex)
    pub target_id: Uuid,

    /// The calculated probabilistic score (0.0 to 1.0).
    pub score_value: f64,

    /// The name of the matching algorithm used (e.g., "Fellegi-Sunter", "ML-Model-v3").
    pub algorithm_name: String,

    /// The date and time when the score was calculated.
    pub calculated_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasMatchScore {
    fn edge_label() -> &'static str {
        "HAS_MATCH_SCORE"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("score_value", true)
                .with_description("The probabilistic score (0.0 to 1.0)."),
            PropertyConstraint::new("algorithm_name", true)
                .with_description("The name/version of the matching algorithm used."),
            PropertyConstraint::new("calculated_at", true)
                .with_description("Timestamp when the score was computed (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasMatchScore {
    /// Converts the HasMatchScore struct into a generic Edge structure.
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
        // FIX: Wrap f64 in SerializableFloat for PropertyValue::Float variant
        e = e.with_property(
            "score_value".to_string(), 
            PropertyValue::Float(SerializableFloat(self.score_value))
        );
        e = e.with_property(
            "algorithm_name".to_string(), 
            PropertyValue::String(self.algorithm_name.clone())
        );
        e = e.with_property(
            "calculated_at".to_string(), 
            PropertyValue::String(self.calculated_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasMatchScore {
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
        let score_value = match edge.properties.get("score_value")? {
            // FIX: Pattern match to extract the inner f64 from SerializableFloat
            PropertyValue::Float(sf) => sf.0, 
            _ => return None,
        };

        Some(HasMatchScore {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            score_value,
            algorithm_name: edge.properties.get("algorithm_name")?.as_str()?.to_string(),
            calculated_at: parse_datetime("calculated_at", edge)?,
        })
    }
}
