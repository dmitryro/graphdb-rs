use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a permanent identity merge, where a SourcePatient record is retired
/// and all associated data is consolidated into the TargetPatient record.
///
/// Edge Label: IS_MERGED_INTO
/// Source Vertex: SourcePatient (Retired Record)
/// Target Vertex: TargetPatient (Authoritative Record)
#[derive(Debug, Clone)]
pub struct IsMergedInto {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the record that was retired)
    pub source_id: Uuid,
    /// ID of the target vertex (the authoritative record)
    pub target_id: Uuid,

    /// The date and time when the merge operation was completed.
    pub merge_date: DateTime<Utc>,

    /// The reason for the merge (e.g., "Duplicate record found", "Manual correction").
    pub reason: String,

    /// The identifier of the user or system that initiated the retirement/merge process.
    pub retired_by: String,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for IsMergedInto {
    fn edge_label() -> &'static str {
        "IS_MERGED_INTO"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("merge_date", true)
                .with_description("Timestamp when the permanent merge was performed (RFC3339)."),
            PropertyConstraint::new("reason", true)
                .with_description("The rationale for retiring the source record and merging."),
            PropertyConstraint::new("retired_by", true)
                .with_description("Identifier of the user or system that executed the merge."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for IsMergedInto {
    /// Converts the IsMergedInto struct into a generic Edge structure.
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
            "merge_date".to_string(), 
            PropertyValue::String(self.merge_date.to_rfc3339()) // DateTime to RFC3339 String
        );
        e = e.with_property(
            "reason".to_string(), 
            PropertyValue::String(self.reason.clone())
        );
        e = e.with_property(
            "retired_by".to_string(), 
            PropertyValue::String(self.retired_by.clone())
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for IsMergedInto {
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
        Some(IsMergedInto {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            merge_date: parse_datetime("merge_date", edge)?,
            reason: edge.properties.get("reason")?.as_str()?.to_string(),
            retired_by: edge.properties.get("retired_by")?.as_str()?.to_string(),
        })
    }
}
