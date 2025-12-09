use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the Encounter vertex type.
///
/// This schema defines the structure for a patient's interaction with the healthcare system,
/// including the date, type of encounter, involved physician, and status lifecycle.
pub struct Encounter;

impl VertexSchema for Encounter {
    fn schema_name() -> &'static str {
        "Encounter"
    }

    /// Returns the list of property constraints for the Encounter vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Fields from the Model ---

            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Foreign key to the Patient vertex (i32). Required.")
                .with_data_type(DataType::Integer),

            PropertyConstraint::new("doctor_id", true)
                .with_description("Foreign key to the Doctor/Provider vertex (i32). Required.")
                .with_data_type(DataType::Integer),

            // Encounter type is constrained to common service delivery types.
            PropertyConstraint::new("encounter_type", true)
                .with_description("The category of the patient-provider interaction (String). Required.")
                .with_enum_values(EnumValues::new(vec![
                    "OUTPATIENT".to_string(),
                    "INPATIENT".to_string(),
                    "EMERGENCY".to_string(),
                    "VIRTUAL".to_string(),
                    "FOLLOW_UP".to_string(),
                ])),

            // Status field is added for state transitions
            PropertyConstraint::new("status", true)
                .with_description("The current lifecycle status of the encounter (String). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "PLANNED".to_string(),
                    "ACTIVE".to_string(),
                    "FINISHED".to_string(),
                    "CANCELED".to_string(),
                ]))
                .with_default_value(serde_json::Value::String("PLANNED".to_string())),

            PropertyConstraint::new("date", true)
                .with_description("The date and time the encounter occurred (Timestamp). Required.")
                .with_data_type(DataType::Timestamp),

            PropertyConstraint::new("notes", false)
                .with_description("Clinical or administrative notes regarding the encounter (String). Optional."),

            // --- Audit Fields ---
            PropertyConstraint::new("created_at", true)
                .with_description("Creation timestamp. Required.")
                .with_data_type(DataType::Timestamp),
            PropertyConstraint::new("updated_at", false)
                .with_description("Last update timestamp. Optional/System-managed.")
                .with_data_type(DataType::Timestamp),
        ]
    }

    /// Defines lifecycle rules using the 'status' property.
    /// This ensures encounters follow a logical progression: PLANNED -> ACTIVE -> FINISHED or CANCELED.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                // Corrected field name from `property_name` to `element`
                element: "status".to_string(),
                // Define the default state
                initial_state: Some("PLANNED".to_string()),
                // Define allowed state transitions
                transitions: vec![
                    // Start: PLANNED -> ACTIVE (Triggers the 'activated' event)
                    StateTransition {
                        from_state: "PLANNED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["encounter.activated".to_string()],
                    },
                    // Normal Completion: ACTIVE -> FINISHED (Triggers the 'finished' event)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "FINISHED".to_string(),
                        required_rules: vec!["require_discharge_summary".to_string()], // Example rule
                        triggers_events: vec!["encounter.finished".to_string()],
                    },
                    // Cancellation: PLANNED -> CANCELED (Triggers the 'canceled' event)
                    StateTransition {
                        from_state: "PLANNED".to_string(),
                        to_state: "CANCELED".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["encounter.canceled".to_string()],
                    },
                ],
                // No pre-checks defined here
                pre_action_checks: vec![],
                // No post-actions defined here (they are triggered by the events above)
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies used for encounter classification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                ontology_system_id: "SNOMED_CT".to_string(),
                name: "Systematized Nomenclature of Medicine—Clinical Terms".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                description: Some("Comprehensive terminology for clinical findings and procedures, useful for classifying encounter type and notes.".to_string()),
                reference_uri: Some("http://snomed.info/sct".to_string()),
            },
            OntologyReference {
                ontology_system_id: "CPT".to_string(),
                name: "Current Procedural Terminology".to_string(),
                uri: Some("http://www.ama-assn.org/go/cpt".to_string()),
                description: Some("Used for billing and coding medical procedures and services performed during the encounter.".to_string()),
                reference_uri: Some("http://www.ama-assn.org/go/cpt".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Encounter CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("encounter.created".to_string()),
            update_topic: Some("encounter.updated".to_string()),
            deletion_topic: Some("encounter.deleted".to_string()),
            error_queue: Some("encounter.errors".to_string()),
        }
    }
}
