use std::time::SystemTime;
use uuid::Uuid;

// --- Mock/External Type Definitions for Schema Context ---
// These type aliases mimic types that would typically be imported from a central
// schema or ID definition module.
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
///
/// Edge Name: HAS_MEDICATION
/// Source Node: Person
/// Target Node: Medication
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

impl HasMedication {
    /// Creates a new `HasMedication` edge instance with a generated UUID.
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

// Implement the SchemaEdge trait for declarative schema definition
impl SchemaEdge for HasMedication {
    const TYPE: &'static str = "HAS_MEDICATION";
    const SOURCE_TYPE: &'static str = "Person";
    const TARGET_TYPE: &'static str = "Medication";
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn create_and_check_has_medication_edge() {
        let person_id = Uuid::new_v4();
        let med_id = Uuid::new_v4();
        let now = SystemTime::now();

        let edge = HasMedication::new(
            person_id,
            med_id,
            now,
            "10mg".to_string(),
            "Once daily".to_string(),
            Some("Take with food.".to_string()),
        );

        assert_eq!(edge.source_id, person_id);
        assert_eq!(edge.target_id, med_id);
        assert_eq!(edge.dosage, "10mg");
        assert_eq!(edge.frequency, "Once daily");
        assert_eq!(edge.notes, Some("Take with food.".to_string()));
        
        // Check schema metadata
        assert_eq!(HasMedication::TYPE, "HAS_MEDICATION");
        assert_eq!(HasMedication::SOURCE_TYPE, "Person");
        assert_eq!(HasMedication::TARGET_TYPE, "Medication");
    }
}
