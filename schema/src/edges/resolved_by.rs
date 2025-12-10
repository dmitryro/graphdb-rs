use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the assignment of responsibility for resolving a data conflict
/// (like a potential duplicate or merge conflict) to a specific user or data steward.
///
/// Edge Label: RESOLVED_BY
/// Source Vertex: MPI_Conflict (The conflict record, often a potential duplicate link)
/// Target Vertex: User (The clinician, data steward, or administrator who resolved it)
#[derive(Debug, Clone)]
pub struct ResolvedBy {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (the MPI Conflict record)
    pub source_id: Uuid,
    /// ID of the target vertex (the User/Data Steward who resolved the conflict)
    pub target_id: Uuid,

    /// The date and time when the conflict was officially resolved.
    pub resolution_date: DateTime<Utc>,

    /// The final outcome of the resolution (e.g., "Merged", "Declined (Not a Duplicate)", "Split").
    pub resolution_outcome: String,

    /// Optional notes or comments provided by the reviewer during the resolution process.
    pub reviewer_notes: Option<String>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for ResolvedBy {
    fn edge_label() -> &'static str {
        "RESOLVED_BY"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("resolution_date", true)
                .with_description("Timestamp when the conflict was resolved (RFC3339)."),
            PropertyConstraint::new("resolution_outcome", true)
                .with_description("The final decision made by the reviewer (e.g., Merged, Declined, Split)."),
            PropertyConstraint::new("reviewer_notes", false)
                .with_description("Optional textual notes from the reviewer."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for ResolvedBy {
    /// Converts the ResolvedBy struct into a generic Edge structure.
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
            "resolution_date".to_string(), 
            PropertyValue::String(self.resolution_date.to_rfc3339()) // DateTime to RFC3339 String
        );
        e = e.with_property(
            "resolution_outcome".to_string(), 
            PropertyValue::String(self.resolution_outcome.clone())
        );
        
        if let Some(notes) = &self.reviewer_notes {
            e = e.with_property(
                "reviewer_notes".to_string(), 
                PropertyValue::String(notes.clone())
            );
        }

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for ResolvedBy {
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
        let reviewer_notes = edge.properties.get("reviewer_notes")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Some(ResolvedBy {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            resolution_date: parse_datetime("resolution_date", edge)?,
            resolution_outcome: edge.properties.get("resolution_outcome")?.as_str()?.to_string(),
            reviewer_notes,
        })
    }
}
