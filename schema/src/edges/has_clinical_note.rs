use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a link to clinical documentation. This edge allows a ClinicalNote to be
/// associated with either the primary Patient record (overall history) or a specific
/// Encounter (visit-specific documentation).
///
/// Edge Label: HAS_CLINICAL_NOTE
/// Source Vertices: Patient OR Encounter
/// Target Vertex: ClinicalNote
#[derive(Debug, Clone)]
pub struct HasClinicalNote {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (either Patient or Encounter)
    pub source_id: Uuid,
    /// ID of the target vertex (ClinicalNote)
    pub target_id: Uuid,

    /// The type of clinical documentation (e.g., "History and Physical", "Progress Note", "Discharge Summary").
    /// Maps to FHIR: Composition.type
    pub note_type: String,

    /// The status of the clinical note (e.g., "final", "amended", "preliminary").
    /// Maps to FHIR: Composition.status
    pub document_status: String,

    /// The date and time when the note was authenticated or authored.
    pub authored_date: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasClinicalNote {
    fn edge_label() -> &'static str {
        "HAS_CLINICAL_NOTE"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("note_type", true)
                .with_description("The kind of documentation (e.g., 'Discharge Summary')."),
            PropertyConstraint::new("document_status", true)
                .with_description("The workflow status of the document (e.g., 'final')."),
            PropertyConstraint::new("authored_date", true)
                .with_description("Timestamp when the note was authored (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasClinicalNote {
    /// Converts the HasClinicalNote struct into a generic Edge structure.
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
            "note_type".to_string(), 
            PropertyValue::String(self.note_type.clone())
        );
        e = e.with_property(
            "document_status".to_string(), 
            PropertyValue::String(self.document_status.clone())
        );
        e = e.with_property(
            "authored_date".to_string(), 
            PropertyValue::String(self.authored_date.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasClinicalNote {
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
        Some(HasClinicalNote {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            note_type: edge.properties.get("note_type")?.as_str()?.to_string(),
            document_status: edge.properties.get("document_status")?.as_str()?.to_string(),
            authored_date: parse_datetime("authored_date", edge)?,
        })
    }
}
