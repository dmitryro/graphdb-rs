use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::rules::{SchemaRule, EnforcementLevel}; 
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the EdProcedure vertex type.
///
/// Description: Tracks a specific medical procedure performed on a patient
/// during an Emergency Department encounter, including performer, time, and outcome.
pub struct EdProcedure;

impl EdProcedure {
    /// Provides the possible status states for a procedure lifecycle.
    fn status_values() -> Vec<String> {
        vec![
            "In_Progress".to_string(), // Currently being performed or documented
            "Completed".to_string(),   // Finished successfully or with complications
            "Aborted".to_string(),     // Stopped before completion (e.g., patient refused, non-critical reason)
            "Failed".to_string(),      // Stopped due to technical failure or adverse event
        ]
    }

    /// Provides the possible procedure outcome values.
    fn outcome_values() -> Vec<String> {
        vec![
            "Successful".to_string(),
            "Complicated".to_string(),
            "Aborted_Intentional".to_string(),
            "Failed_Technical".to_string(),
            "Patient_Refusal".to_string(),
        ]
    }
}

impl VertexSchema for EdProcedure {
    fn schema_name() -> &'static str {
        "EdProcedure"
    }

    /// Returns the list of property constraints for the EdProcedure vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("encounter_id", true)
                .with_description("ID of the Encounter vertex this procedure belongs to. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("procedure_code_id", true)
                .with_description("ID of the MedicalCode (e.g., CPT) for standardization/billing. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            PropertyConstraint::new("procedure_name", true)
                .with_description("The human-readable name of the procedure (e.g., 'Central Line Placement'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),
            
            PropertyConstraint::new("performed_by_doctor_id", true)
                .with_description("ID of the Doctor who performed the procedure. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            PropertyConstraint::new("assist_nurse_id", false)
                .with_description("Optional ID of the Nurse who assisted.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),

            PropertyConstraint::new("start_time", true)
                .with_description("Timestamp when the procedure began. Required, Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("end_time", false)
                .with_description("Timestamp when the procedure ended (required for terminal statuses).")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![]),
            
            PropertyConstraint::new("status", true)
                .with_description("The current workflow status (In_Progress, Completed, Aborted, Failed).")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(EdProcedure::status_values()))
                .with_default_value(JsonValue::String("In_Progress".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("outcome", false)
                .with_description("The clinical result (Successful, Complicated, Failed_Technical, etc.).")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(EdProcedure::outcome_values()))
                .with_constraints(vec![]),

            PropertyConstraint::new("notes", false)
                .with_description("Detailed notes or documentation about the procedure.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),
        ]
    }

    /// EdProcedure lifecycle manages the status from 'In_Progress' to a terminal state.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("In_Progress".to_string()),
                transitions: vec![
                    // 1. Transition to Completed (Successful/Complicated)
                    StateTransition {
                        from_state: "In_Progress".to_string(),
                        to_state: "Completed".to_string(),
                        // Must have an end time and a formal outcome
                        required_rules: vec![
                            "require_end_time".to_string(),
                            "require_outcome".to_string(),
                        ],
                        triggers_events: vec!["procedure.completed".to_string()],
                    },
                    // 2. Transition to Aborted (Stopped before completion)
                    StateTransition {
                        from_state: "In_Progress".to_string(),
                        to_state: "Aborted".to_string(),
                        // Must have an end time and explanatory notes
                        required_rules: vec![
                            "require_end_time".to_string(),
                            "require_notes_for_aborted".to_string()
                        ],
                        triggers_events: vec!["procedure.aborted".to_string()],
                    },
                    // 3. Transition to Failed (Technical failure or adverse event)
                    StateTransition {
                        from_state: "In_Progress".to_string(),
                        to_state: "Failed".to_string(),
                        // Must have an end time, notes, and a clear failure outcome
                        required_rules: vec![
                            "require_end_time".to_string(),
                            "require_notes_for_failure".to_string(),
                            "require_failure_outcome".to_string(),
                        ],
                        triggers_events: vec!["procedure.failed".to_string()],
                    },
                ],
                // No pre-action checks here, as it's state-driven.
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for procedure classification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "CPT_HCPCS".to_string(),
                ontology_system_id: "AMA".to_string(),
                uri: Some("https://www.ama-assn.org/".to_string()),
                reference_uri: None,
                description: Some("Current Procedural Terminology (CPT) codes for services and procedures.".to_string()),
            },
            OntologyReference {
                name: "SNOMED_CT_Procedure".to_string(),
                ontology_system_id: "SNOMED".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: Some("SNOMED Concept: 71388002 |Procedure (procedure)|".to_string()),
                description: Some("SNOMED CT concepts related to clinical procedures.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for EdProcedure CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("ed_procedure.created".to_string()),
            update_topic: Some("ed_procedure.updated".to_string()), // Allowed for status/closure updates
            deletion_topic: Some("ed_procedure.deleted".to_string()),
            error_queue: Some("ed_procedure.errors".to_string()),
        }
    }
}
