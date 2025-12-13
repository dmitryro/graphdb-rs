use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a probabilistic or "soft link" between two patient records (Patient A -> Patient B)
/// that are candidate duplicates and may be awaiting review or confirmation.
///
/// Edge Label: IS_LINKED_TO
/// Source Vertex: Patient A
/// Target Vertex: Patient B
#[derive(Debug, Clone)]
pub struct IsLinkedTo {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (Patient A)
    pub source_id: Uuid,
    /// ID of the target vertex (Patient B)
    pub target_id: Uuid,

    /// The calculated score (0.0 to 1.0) indicating the probability of the two records matching.
    pub match_score: f64,

    /// The specific type of link, e.g., "candidate", "duplicate", "seealso".
    pub link_type: String,

    /// The algorithm used to generate the match (e.g., "probabilistic", "deterministic", "fuzzy").
    pub match_algorithm: String,

    /// The date and time when this soft link was generated.
    pub link_date: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for IsLinkedTo {
    fn edge_label() -> &'static str {
        "IS_LINKED_TO"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("match_score", true)
                .with_description("Probability score of the match (f64, stored as String)."),
            PropertyConstraint::new("link_type", true)
                .with_description("The nature of the link (e.g., 'candidate', 'seealso')."),
            PropertyConstraint::new("match_algorithm", true)
                .with_description("The matching algorithm used (e.g., 'probabilistic')."),
            PropertyConstraint::new("link_date", true)
                .with_description("Timestamp when the link was generated (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for IsLinkedTo {
    /// Converts the IsLinkedTo struct into a generic Edge structure.
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
            "match_score".to_string(), 
            PropertyValue::String(self.match_score.to_string()) // f64 to String
        );
        e = e.with_property(
            "link_type".to_string(), 
            PropertyValue::String(self.link_type.clone())
        );
        e = e.with_property(
            "match_algorithm".to_string(), 
            PropertyValue::String(self.match_algorithm.clone())
        );
        e = e.with_property(
            "link_date".to_string(), 
            PropertyValue::String(self.link_date.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for IsLinkedTo {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::edge_label() { return None; }

        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };

        // Helper to parse string property into f64 (for match score)
        let parse_f64 = |prop_name: &str, edge: &Edge| -> Option<f64> {
             edge.properties.get(prop_name).and_then(|v| v.as_str()?.parse().ok())
        };
        
        // 1. Convert IDs from SerializableUuid back to Uuid
        let source_id = Uuid::parse_str(&edge.outbound_id.to_string()).ok()?; 
        let target_id = Uuid::parse_str(&edge.inbound_id.to_string()).ok()?;

        // 2. Safely extract mandatory properties
        Some(IsLinkedTo {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            match_score: parse_f64("match_score", edge)?,
            link_type: edge.properties.get("link_type")?.as_str()?.to_string(),
            match_algorithm: edge.properties.get("match_algorithm")?.as_str()?.to_string(),
            link_date: parse_datetime("link_date", edge)?,
        })
    }
}
