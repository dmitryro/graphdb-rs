use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents an identity split operation, which is the reversal of an erroneous merge.
/// A new patient record (Source) is created/re-instated from the data that was
/// incorrectly merged into the authoritative record (Target).
///
/// Edge Label: IS_SPLIT_FROM
/// Source Vertex: NewPatient (The record that was split out)
/// Target Vertex: OriginalPatient (The surviving authoritative record)
#[derive(Debug, Clone)]
pub struct IsSplitFrom {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the new or re-instated patient record)
    pub source_id: Uuid,
    /// ID of the target vertex (the original record that was incorrectly merged into)
    pub target_id: Uuid,

    /// The date and time when the identity split operation was completed.
    pub split_date: DateTime<Utc>,

    /// The reason for the split (e.g., "Erroneous merge detected", "Manual review correction").
    pub reason: String,

    /// The identifier of the user or system that initiated the split operation.
    pub split_by: String,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for IsSplitFrom {
    fn edge_label() -> &'static str {
        "IS_SPLIT_FROM"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("split_date", true)
                .with_description("Timestamp when the identity split was performed (RFC3339)."),
            PropertyConstraint::new("reason", true)
                .with_description("The rationale for undoing the merge and splitting the record."),
            PropertyConstraint::new("split_by", true)
                .with_description("Identifier of the user or system that executed the split."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for IsSplitFrom {
    /// Converts the IsSplitFrom struct into a generic Edge structure.
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
            "split_date".to_string(), 
            PropertyValue::String(self.split_date.to_rfc3339()) // DateTime to RFC3339 String
        );
        e = e.with_property(
            "reason".to_string(), 
            PropertyValue::String(self.reason.clone())
        );
        e = e.with_property(
            "split_by".to_string(), 
            PropertyValue::String(self.split_by.clone())
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for IsSplitFrom {
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
        Some(IsSplitFrom {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            split_date: parse_datetime("split_date", edge)?,
            reason: edge.properties.get("reason")?.as_str()?.to_string(),
            split_by: edge.properties.get("split_by")?.as_str()?.to_string(),
        })
    }
}
