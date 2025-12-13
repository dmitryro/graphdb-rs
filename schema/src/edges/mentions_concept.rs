use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Links a ClinicalNote (or a SectionVertex) to an NLP-extracted standardized
/// clinical concept, such as a SNOMED CT code.
/// This enables semantic search and analysis over unstructured notes.
///
/// Edge Label: MENTIONS_CONCEPT
/// Source Vertex: ClinicalNote (or SectionVertex)
/// Target Vertex: SNOMED_Concept
#[derive(Debug, Clone)]
pub struct MentionsConcept {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the ClinicalNote or Section)
    pub source_id: Uuid,
    /// ID of the target vertex (the SNOMED_Concept)
    pub target_id: Uuid,

    /// The exact text snippet from the note that triggered the concept extraction.
    pub matched_text: String,

    /// The confidence score (0.0 to 1.0) of the NLP model for this extraction.
    pub confidence_score: f32,

    /// The date and time when the concept extraction occurred.
    pub extracted_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for MentionsConcept {
    fn edge_label() -> &'static str {
        "MENTIONS_CONCEPT"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("matched_text", true)
                .with_description("The exact phrase in the clinical note that was matched to the concept."),
            PropertyConstraint::new("confidence_score", true)
                .with_description("NLP model's confidence in the extraction (0.0 - 1.0)."),
            PropertyConstraint::new("extracted_at", true)
                .with_description("Timestamp when the concept was extracted/linked (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for MentionsConcept {
    /// Converts the MentionsConcept struct into a generic Edge structure.
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
        // Store float as a String to avoid potential cross-language float parsing issues, 
        // especially important in graph databases which may store numbers with varying precision.
        e = e.with_property(
            "confidence_score".to_string(), 
            PropertyValue::String(self.confidence_score.to_string())
        );
        e = e.with_property(
            "extracted_at".to_string(), 
            PropertyValue::String(self.extracted_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for MentionsConcept {
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
        
        // Deserialize the confidence score (stored as String)
        let confidence_score = edge.properties.get("confidence_score")?
            .as_str()?
            .parse::<f32>()
            .ok()?;

        Some(MentionsConcept {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            matched_text,
            confidence_score,
            extracted_at: parse_datetime("extracted_at", edge)?,
        })
    }
}
