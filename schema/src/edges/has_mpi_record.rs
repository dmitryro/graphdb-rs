use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the critical relationship (edge) between a local Patient record
/// and the definitive Master Patient Index (MPI) record, used for identity resolution.
///
/// Edge Label: HAS_MPI_RECORD
/// Source Vertex: Patient (Local Record)
/// Target Vertex: MasterPatientIndex (Golden Record)
#[derive(Debug, Clone)]
pub struct HasMpiRecord {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (Patient)
    pub source_id: Uuid,
    /// ID of the target vertex (MasterPatientIndex)
    pub target_id: Uuid,

    /// The date and time when this link was established or confirmed.
    pub link_date: DateTime<Utc>,

    /// The method by which this link was established (e.g., 'MANUAL', 'AUTOMATIC', 'PROBABILISTIC').
    pub resolution_method: String,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasMpiRecord {
    fn edge_label() -> &'static str {
        "HAS_MPI_RECORD"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("link_date", true)
                .with_description("Timestamp when the identity link was established (RFC3339)."),
            PropertyConstraint::new("resolution_method", true)
                .with_description("Method used to establish the link (e.g., MANUAL, AUTOMATIC)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasMpiRecord {
    /// Converts the HasMpiRecord struct into a generic Edge structure.
    fn to_edge(&self) -> Edge {
        // IDs are Uuids, which are directly convertible to SerializableUuid
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
            "link_date".to_string(), 
            PropertyValue::String(self.link_date.to_rfc3339()) // DateTime to RFC3339 String
        );
        e = e.with_property(
            "resolution_method".to_string(), 
            PropertyValue::String(self.resolution_method.clone())
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasMpiRecord {
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
        Some(HasMpiRecord {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            link_date: parse_datetime("link_date", edge)?,
            resolution_method: edge.properties.get("resolution_method")?.as_str()?.to_string(),
        })
    }
}
