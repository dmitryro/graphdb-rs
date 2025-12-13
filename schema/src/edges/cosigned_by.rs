use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents an Attending or supervising physician's co-signature (attestation) on a note,
/// often for documentation written by a resident or student.
///
/// Edge Label: COSIGNED_BY
/// Source Vertex: ClinicalNote
/// Target Vertex: User (Attending, Supervisor)
#[derive(Debug, Clone)]
pub struct CosignedBy {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the ClinicalNote being cosigned)
    pub source_id: Uuid,
    /// ID of the target vertex (the User/Attester)
    pub target_id: Uuid,

    /// The type of attestation (e.g., "Legal Authentication", "Review").
    /// Maps to FHIR: Composition.attester.mode
    pub attestation_mode: String,

    /// The date and time when the note was officially cosigned.
    /// Maps to FHIR: Composition.attester.time
    pub cosigned_at: DateTime<Utc>,

    /// The reason for the co-signature (e.g., "Required for resident documentation").
    pub reason: Option<String>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for CosignedBy {
    fn edge_label() -> &'static str {
        "COSIGNED_BY"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("attestation_mode", true)
                .with_description("The mode of attestation (e.g., 'Legal Authentication')."),
            PropertyConstraint::new("cosigned_at", true)
                .with_description("Timestamp when the co-signature was applied (RFC3339)."),
            PropertyConstraint::new("reason", false) // Optional
                .with_description("The reason for the co-signature (optional)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for CosignedBy {
    /// Converts the CosignedBy struct into a generic Edge structure.
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
            "attestation_mode".to_string(), 
            PropertyValue::String(self.attestation_mode.clone())
        );
        e = e.with_property(
            "cosigned_at".to_string(), 
            PropertyValue::String(self.cosigned_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        if let Some(reason) = &self.reason {
            e = e.with_property(
                "reason".to_string(), 
                PropertyValue::String(reason.clone())
            );
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for CosignedBy {
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
        let reason = edge.properties.get("reason")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());


        Some(CosignedBy {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            attestation_mode: edge.properties.get("attestation_mode")?.as_str()?.to_string(),
            cosigned_at: parse_datetime("cosigned_at", edge)?,
            reason,
        })
    }
}
