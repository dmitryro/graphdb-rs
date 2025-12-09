use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{LifecycleRule, MessagingSchema};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Defines the schema for a Vitals vertex, representing a set of clinical measurements
/// taken at a specific point in time for a patient.
pub struct Vitals;

impl VertexSchema for Vitals {
    fn schema_name() -> &'static str {
        "Vitals"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Globally Unique Identifier (UUID) for the Vitals record.")
                .with_data_type(DataType::Uuid)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("The ID of the Patient vertex this record belongs to.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("weight", false)
                .with_description("Patient weight in kilograms (kg).")
                .with_data_type(DataType::Float)
                .with_constraints(vec![Constraint::Optional, Constraint::Min(0)]), // Must be non-negative

            PropertyConstraint::new("height", false)
                .with_description("Patient height in meters (m).")
                .with_data_type(DataType::Float)
                .with_constraints(vec![Constraint::Optional, Constraint::Min(0)]), // Must be non-negative

            PropertyConstraint::new("blood_pressure_systolic", false)
                .with_description("Systolic blood pressure (mmHg).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![
                    Constraint::Optional,
                    Constraint::Min(0),
                    Constraint::Max(300), // Sanity check for extremely high BP
                ]),

            PropertyConstraint::new("blood_pressure_diastolic", false)
                .with_description("Diastolic blood pressure (mmHg).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![
                    Constraint::Optional,
                    Constraint::Min(0),
                    Constraint::Max(200), // Sanity check for extremely high BP
                ]),

            PropertyConstraint::new("temperature", false)
                .with_description("Body temperature in Celsius (°C).")
                .with_data_type(DataType::Float)
                .with_constraints(vec![
                    Constraint::Optional,
                    Constraint::Min(30),  // Below 30°C is severe hypothermia
                    Constraint::Max(44),  // Above 44°C is lethal
                ]),

            PropertyConstraint::new("heart_rate", false)
                .with_description("Heart rate (beats per minute).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![
                    Constraint::Optional,
                    Constraint::Min(1),
                    Constraint::Max(300), // Sanity check for extreme tachycardia
                ]),

            PropertyConstraint::new("created_at", true)
                .with_description("The exact timestamp when the vital signs were recorded.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
        ]
    }

    fn lifecycle_rules() -> Vec<LifecycleRule> {
        // Vitals records are generally immutable once created, reflecting a historical measurement.
        vec![]
    }

    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            // Standard medical ontologies are often used to define and code vital signs.
            OntologyReference::new("LOINC", "Logical Observation Identifiers Names and Codes")
        ]
    }

    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("vitals.created".to_string()),
            // Updates are discouraged for vitals; if the data is wrong, a new corrected record should be created.
            update_topic: None, 
            deletion_topic: Some("vitals.deleted".to_string()),
            error_queue: Some("medical.data_integrity_errors".to_string()),
        }
    }
}
