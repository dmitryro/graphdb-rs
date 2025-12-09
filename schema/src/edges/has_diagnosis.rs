use chrono::{DateTime, Utc, NaiveDate};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue}; 
use crate::edges::relationship::EdgeSchema;

/// Represents the relationship (edge) between a Patient and a Diagnosis record,
/// signifying that the Patient has received that specific diagnosis.
///
/// Edge Label: HAS_DIAGNOSIS
/// Source Vertex: Patient
/// Target Vertex: Diagnosis
#[derive(Debug, Clone)]
pub struct HasDiagnosis {
    /// Unique ID for this relationship instance
    pub id: i32,
    /// ID of the source vertex (Patient)
    pub source_id: i32,
    /// ID of the target vertex (Diagnosis)
    pub target_id: i32,
    /// Date when this diagnosis was first recorded as applicable to the patient.
    pub recorded_date: NaiveDate,
    /// Timestamp when this relationship record was created
    pub created_at: DateTime<Utc>,
    /// Optional field to track the status of the diagnosis (e.g., ACTIVE, RESOLVED, HISTORY)
    pub status: Option<String>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasDiagnosis {
    fn edge_label() -> &'static str {
        "HAS_DIAGNOSIS"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (i32, stored as String)."),
            PropertyConstraint::new("recorded_date", true)
                .with_description("Date diagnosis was recorded (YYYY-MM-DD)."),
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp of relationship creation (RFC3339)."),
            PropertyConstraint::new("status", false)
                .with_description("Status of the diagnosis (ACTIVE, RESOLVED, HISTORY)."),
        ]
    }
}
// --- END EdgeSchema ---

impl ToEdge for HasDiagnosis {
    /// Converts the HasDiagnosis struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // Prepare IDs and Label for the new Edge::new(outbound_id, edge_type, inbound_id) signature
        let outbound_id: SerializableUuid = Uuid::parse_str(&self.source_id.to_string())
            .expect("Invalid source_id UUID format")
            .into();
        let inbound_id: SerializableUuid = Uuid::parse_str(&self.target_id.to_string())
            .expect("Invalid target_id UUID format")
            .into();
        let edge_label_identifier = Identifier::new(Self::edge_label().to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(
            outbound_id, 
            edge_label_identifier, 
            inbound_id
        );

        // Add relationship-specific properties
        e = e.with_property(
            "id".to_string(), 
            PropertyValue::String(self.id.to_string())
        );
        e = e.with_property(
            "recorded_date".to_string(), 
            PropertyValue::String(self.recorded_date.to_string()) 
        );
        e = e.with_property(
            "created_at".to_string(), 
            PropertyValue::String(self.created_at.to_rfc3339())
        );

        if let Some(ref val) = self.status {
            e = e.with_property(
                "status".to_string(), 
                PropertyValue::String(val.to_string())
            );
        }

        e
    }
}


impl FromEdge for HasDiagnosis {
    // FIX E0185: Removed '&self' to match the trait definition (now a static method)
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::edge_label() { return None; }

        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };

        let parse_naive_date = |prop_name: &str, edge: &Edge| -> Option<NaiveDate> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        };
        
        // This closure is correct for parsing an &str into an i32
        let parse_id = |id_str: &str| -> Option<i32> {
            id_str.parse().ok()
        };


        Some(HasDiagnosis {
            id: edge.properties.get("id")?.as_str()?.parse().ok()?,
            // FIX E0308: Convert SerializableUuid to String, then take &str for parse_id
            source_id: parse_id(&edge.outbound_id.to_string())?, 
            target_id: parse_id(&edge.inbound_id.to_string())?,
            recorded_date: parse_naive_date("recorded_date", edge)?,
            created_at: parse_datetime("created_at", edge)?,
            status: edge.properties.get("status").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}