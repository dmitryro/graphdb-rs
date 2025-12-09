use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue}; 
use crate::edges::relationship::EdgeSchema;

// --- Mock/External Type Definitions for Schema Context ---
pub type PersonId = Uuid;
pub type MedicationId = Uuid;

/// A trait representing a basic edge in the graph schema for declarative definition.
pub trait SchemaEdge {
    /// The unique type identifier for this edge (e.g., "HAS_MEDICATION").
    const TYPE: &'static str;
    /// The schema type name of the source node (e.g., "Person").
    const SOURCE_TYPE: &'static str;
    /// The schema type name of the target node (e.g., "Medication").
    const TARGET_TYPE: &'static str;
}

// --------------------------------------------------------

/// Represents the relationship where a Person has been prescribed or is taking a specific Medication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HasMedication {
    /// The unique ID of the edge.
    pub id: Uuid,
    /// The ID of the source node (a Person).
    pub source_id: PersonId,
    /// The ID of the target node (a Medication).
    pub target_id: MedicationId,
    
    // --- Edge Properties (Metadata about the relationship) ---
    
    /// The start date of the medication regimen.
    pub start_date: SystemTime,
    /// The prescribed dosage (e.g., "500 mg").
    pub dosage: String,
    /// The frequency of administration (e.g., "Twice daily", "PRN").
    pub frequency: String,
    /// Optional field for notes or specific instructions.
    pub notes: Option<String>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasMedication {
    fn edge_label() -> &'static str {
        Self::TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (Uuid, stored as String)."),
            PropertyConstraint::new("start_date", true)
                .with_description("Start date of medication regimen (Unix timestamp string)."),
            PropertyConstraint::new("dosage", true)
                .with_description("Prescribed dosage (e.g., '500 mg')."),
            PropertyConstraint::new("frequency", true)
                .with_description("Frequency of administration (e.g., 'Once daily')."),
            PropertyConstraint::new("notes", false)
                .with_description("Specific instructions or notes."),
        ]
    }
}
// --- END EdgeSchema ---

impl ToEdge for HasMedication {
    fn to_edge(&self) -> Edge {
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::TYPE.to_string())
            .expect("Invalid edge label identifier");

        // Convert SystemTime to Unix timestamp string (seconds since epoch)
        let start_time_str = self.start_date.duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_else(|_| "0".to_string()); // Fallback
        
        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            .with_property("id".to_string(), PropertyValue::String(self.id.to_string()))
            .with_property("start_date".to_string(), PropertyValue::String(start_time_str))
            .with_property("dosage".to_string(), PropertyValue::String(self.dosage.clone()))
            .with_property("frequency".to_string(), PropertyValue::String(self.frequency.clone()));
        
        if let Some(ref val) = self.notes {
            e = e.with_property("notes".to_string(), PropertyValue::String(val.clone()));
        }

        e
    }
}

impl FromEdge for HasMedication {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::TYPE { return None; }
        
        let parse_system_time = |prop_name: &str, edge: &Edge| -> Option<SystemTime> {
            let seconds = edge.properties.get(prop_name)?.as_str()?.parse::<u64>().ok()?;
            Some(UNIX_EPOCH + std::time::Duration::from_secs(seconds))
        };

        Some(HasMedication {
            id: edge.properties.get("id")?.as_str()?.parse().ok()?,
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?,
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,
            start_date: parse_system_time("start_date", edge)?,
            dosage: edge.properties.get("dosage")?.as_str()?.to_string(),
            frequency: edge.properties.get("frequency")?.as_str()?.to_string(),
            notes: edge.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}

impl HasMedication {
    pub fn new(
        source_id: PersonId,
        target_id: MedicationId,
        start_date: SystemTime,
        dosage: String,
        frequency: String,
        notes: Option<String>,
    ) -> Self {
        HasMedication {
            id: Uuid::new_v4(),
            source_id,
            target_id,
            start_date,
            dosage,
            frequency,
            notes,
        }
    }
}

impl SchemaEdge for HasMedication {
    const TYPE: &'static str = "HAS_MEDICATION";
    const SOURCE_TYPE: &'static str = "Person";
    const TARGET_TYPE: &'static str = "Medication";
}
