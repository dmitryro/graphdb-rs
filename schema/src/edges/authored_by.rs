use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the primary authorship of a clinical note.
/// This edge links the documentation (ClinicalNote) back to the healthcare provider (User) who wrote it.
///
/// Edge Label: AUTHORED_BY
/// Source Vertex: ClinicalNote
/// Target Vertex: User (Doctor, NP, PA, etc.)
#[derive(Debug, Clone)]
pub struct AuthoredBy {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the ClinicalNote)
    pub source_id: Uuid,
    /// ID of the target vertex (the User/Author)
    pub target_id: Uuid,

    /// Role of the author at the time of authorship (e.g., "Attending Physician", "Medical Student").
    /// Maps to FHIR: Composition.author.role
    pub author_role: String,

    /// Whether the note was officially signed or attested to by the author.
    pub is_signed: bool,

    /// The date and time when this authorship link was created (can often be the same as note creation time).
    pub created_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for AuthoredBy {
    fn edge_label() -> &'static str {
        "AUTHORED_BY"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("author_role", true)
                .with_description("The clinical role of the author (e.g., 'Attending Physician')."),
            PropertyConstraint::new("is_signed", true)
                .with_description("Boolean indicating if the note has been electronically signed."),
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the authorship link was recorded (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for AuthoredBy {
    /// Converts the AuthoredBy struct into a generic Edge structure.
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
            "author_role".to_string(), 
            PropertyValue::String(self.author_role.clone())
        );
        e = e.with_property(
            "is_signed".to_string(), 
            PropertyValue::Boolean(self.is_signed)
        );
        e = e.with_property(
            "created_at".to_string(), 
            PropertyValue::String(self.created_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for AuthoredBy {
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
        let is_signed = match edge.properties.get("is_signed")? {
            PropertyValue::Boolean(val) => *val,
            _ => return None,
        };

        Some(AuthoredBy {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            author_role: edge.properties.get("author_role")?.as_str()?.to_string(),
            is_signed,
            created_at: parse_datetime("created_at", edge)?,
        })
    }
}
