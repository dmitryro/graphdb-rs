use chrono::{DateTime, Utc, NaiveDate};
use models::{Edge, ToEdge, Identifier, SerializableUuid, PropertyValue}; // Added SerializableUuid and PropertyValue
use uuid::Uuid;

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

impl ToEdge for HasDiagnosis {
    /// Converts the HasDiagnosis struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // Prepare IDs and Label for the new Edge::new(outbound_id, edge_type, inbound_id) signature
        // Parse the source_id string as a UUID, or handle the error appropriately
        let outbound_id: SerializableUuid = Uuid::parse_str(&self.source_id.to_string())
            .expect("Invalid source_id UUID format")
            .into();
        let inbound_id: SerializableUuid = Uuid::parse_str(&self.target_id.to_string())
            .expect("Invalid target_id UUID format")
            .into();
        let edge_label_identifier = Identifier::new("HAS_DIAGNOSIS".to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(
            outbound_id, 
            edge_label_identifier, 
            inbound_id
        );

        // Add relationship-specific properties using the chainable with_property method,
        // and wrapping values in PropertyValue::String
        e = e.with_property(
            "id".to_string(), 
            PropertyValue::String(self.id.to_string())
        );
        e = e.with_property(
            "recorded_date".to_string(), 
            // NaiveDate::to_string() yields a format like YYYY-MM-DD
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

impl HasDiagnosis {
    /// Attempts to construct a HasDiagnosis struct from a generic Edge representation.
    pub fn from_edge(edge: &Edge) -> Option<Self> {
        // Use as_str() for explicit string comparison
        if edge.label.as_str() != "HAS_DIAGNOSIS" { return None; }

        // Helper closure to parse DateTime<Utc> from property string
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                // Access the underlying string value from the PropertyValue enum
                .and_then(|v| v.as_str()) 
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };

        // Helper closure to parse NaiveDate from property string
        let parse_naive_date = |prop_name: &str, edge: &Edge| -> Option<NaiveDate> {
            edge.properties.get(prop_name)
                // Access the underlying string value from the PropertyValue enum
                .and_then(|v| v.as_str()) 
                // We rely on the to_string() format from the to_edge implementation (YYYY-MM-DD)
                .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()) 
        };

        Some(HasDiagnosis {
            // Retrieve string from PropertyValue before parsing to i32
            id: edge.properties.get("id")?.as_str()?.parse().ok()?,
            // Source ID (Patient) - Use the renamed 'outbound_id' field
            source_id: edge.outbound_id.to_string().parse().ok()?,
            // Target ID (Diagnosis) - Use the renamed 'inbound_id' field
            target_id: edge.inbound_id.to_string().parse().ok()?,
            // Relationship-specific properties
            recorded_date: parse_naive_date("recorded_date", edge)?,
            created_at: parse_datetime("created_at", edge)?,
            // Retrieve string from PropertyValue before mapping
            status: edge.properties.get("status").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}