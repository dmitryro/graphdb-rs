use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the PatientJourney vertex type.
///
/// This vertex serves as a chronological snapshot or event point in a patient's
/// medical history, linking relevant entities that occurred at a specific time.
pub struct PatientJourney;

impl VertexSchema for PatientJourney {
    fn schema_name() -> &'static str {
        "PatientJourney"
    }

    /// Returns the list of property constraints for the PatientJourney vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the Patient vertex this journey point tracks. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),
            
            // --- Linked Entities (Required for a journey point) ---
            PropertyConstraint::new("encounter_id", true)
                .with_description("Reference ID to the Encounter vertex this event belongs to. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("diagnosis_id", true)
                .with_description("Reference ID to the Diagnosis vertex associated with this event. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            // --- Linked Entities (Optional) ---
            PropertyConstraint::new("prescription_id", false)
                .with_description("Reference ID to a Prescription vertex initiated at this point. Optional.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Optional, Constraint::Indexable]),

            PropertyConstraint::new("vitals_id", false)
                .with_description("Reference ID to the Vitals/Observation vertex taken at this time. Optional.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Optional, Constraint::Indexable]),
            
            // --- Temporal Property ---
            PropertyConstraint::new("timestamp", true)
                .with_description("The exact time (point-in-time) of this event in the patient's journey. Required, Indexed.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            // --- Status/Lifecycle Property ---
            PropertyConstraint::new("status", true)
                .with_description("The chronological status of the journey point (ACTIVE, COMPLETED, VOIDED).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "ACTIVE".to_string(),      // Currently the latest or ongoing event
                    "COMPLETED".to_string(),   // Event is finalized and closed
                    "VOIDED".to_string(),      // Event was created in error and should be ignored
                ]))
                .with_default_value(JsonValue::String("ACTIVE".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("ACTIVE".to_string()),
                transitions: vec![
                    // 1. Completion: ACTIVE -> COMPLETED (Most common successful transition)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "COMPLETED".to_string(),
                        required_rules: vec!["require_final_data".to_string()],
                        triggers_events: vec!["journey_point.completed".to_string()],
                    },
                    // 2. Error/Retraction: * -> VOIDED (Can be voided from any non-voided state)
                    StateTransition {
                        from_state: "*".to_string(),
                        to_state: "VOIDED".to_string(),
                        required_rules: vec!["require_void_reason".to_string()],
                        triggers_events: vec!["journey_point.voided".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Ontology references are general since this is a linkage vertex, not a source of clinical data.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "FHIR_Resource_Mapping".to_string(),
                ontology_system_id: "FHIR".to_string(),
                uri: Some("http://hl7.org/fhir/R4".to_string()),
                reference_uri: None,
                description: Some("Relevant to mapping to the FHIR 'Encounter' or 'CarePlan' resources.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for PatientJourney CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("patient_journey.created".to_string()),
            update_topic: Some("patient_journey.updated".to_string()),
            deletion_topic: Some("patient_journey.deleted".to_string()),
            error_queue: Some("patient_journey.errors".to_string()),
        }
    }
}
