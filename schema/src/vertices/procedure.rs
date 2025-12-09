use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the Procedure vertex type.
///
/// This schema defines constraints and a lifecycle for any medical, surgical,
/// or diagnostic procedure performed on a patient. It tracks the status of the procedure
/// from planning to completion.
pub struct Procedure;

impl VertexSchema for Procedure {
    fn schema_name() -> &'static str {
        "Procedure"
    }

    /// Returns the list of property constraints for the Procedure vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex on whom the procedure was performed. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("encounter_id", false)
                .with_description("ID of the Encounter vertex during which the procedure occurred. Optional.")
                .with_data_type(DataType::Integer),

            // --- Procedure Details ---
            PropertyConstraint::new("procedure_code_id", true)
                .with_description("Reference ID to a coding system (e.g., CPT, ICD-10-PCS) for billing and classification.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("procedure_name", true)
                .with_description("Human-readable name of the procedure (e.g., 'Appendectomy').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("performed_by_doctor_id", true)
                .with_description("ID of the Doctor vertex who primarily performed the procedure.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("performed_at", true)
                .with_description("Timestamp when the procedure started or was completed.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("notes", false)
                .with_description("Detailed procedural notes or summary of findings.")
                .with_data_type(DataType::String),

            // --- Lifecycle Status Field ---
            PropertyConstraint::new("status", true)
                .with_description("The current status of the procedure.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "PLANNED".to_string(),      // Initial state, scheduled
                    "IN_PROGRESS".to_string(),  // Currently being performed
                    "COMPLETED".to_string(),    // Procedure finished successfully
                    "FAILED".to_string(),       // Attempted but unsuccessful
                    "ABORTED".to_string(),      // Stopped before completion
                    "CANCELLED".to_string(),    // Never started, cancelled beforehand
                ]))
                .with_default_value(serde_json::Value::String("PLANNED".to_string())),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to manage procedure flow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PLANNED".to_string()),
                transitions: vec![
                    // 1. Procedure Starts: PLANNED -> IN_PROGRESS
                    StateTransition {
                        from_state: "PLANNED".to_string(),
                        to_state: "IN_PROGRESS".to_string(),
                        required_rules: vec!["require_pre_op_check".to_string()],
                        triggers_events: vec!["procedure.started".to_string()],
                    },
                    // 2. Procedure Success: IN_PROGRESS -> COMPLETED
                    StateTransition {
                        from_state: "IN_PROGRESS".to_string(),
                        to_state: "COMPLETED".to_string(),
                        required_rules: vec!["require_post_op_note".to_string()],
                        triggers_events: vec!["procedure.completed".to_string()],
                    },
                    // 3. Procedure Failure: IN_PROGRESS -> FAILED or ABORTED
                    StateTransition {
                        from_state: "IN_PROGRESS".to_string(),
                        to_state: "FAILED".to_string(),
                        required_rules: vec!["require_failure_reason".to_string()],
                        triggers_events: vec!["procedure.failed".to_string()],
                    },
                    StateTransition {
                        from_state: "IN_PROGRESS".to_string(),
                        to_state: "ABORTED".to_string(),
                        required_rules: vec!["require_abortion_reason".to_string()],
                        triggers_events: vec!["procedure.aborted".to_string()],
                    },
                    // 4. Cancellation (can happen anytime before IN_PROGRESS)
                    StateTransition {
                        from_state: "PLANNED".to_string(),
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["procedure.cancelled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for procedure coding.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "CPT_Codes".to_string(),
                ontology_system_id: "CPT".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Current Procedural Terminology (CPT) codes for services and procedures.".to_string()),
            },
            OntologyReference {
                name: "ICD10_PCS_Codes".to_string(),
                ontology_system_id: "ICD10-PCS".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("International Classification of Diseases, Tenth Revision, Procedure Coding System (ICD-10-PCS) for inpatient procedures.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Procedure CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("procedure.created".to_string()),
            update_topic: Some("procedure.updated".to_string()),
            deletion_topic: Some("procedure.deleted".to_string()),
            error_queue: Some("procedure.errors".to_string()),
        }
    }
}
