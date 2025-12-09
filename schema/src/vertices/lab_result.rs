use chrono::{DateTime, Utc};
use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the LabResult vertex type.
///
/// This schema defines constraints and a lifecycle for individual laboratory results,
/// tracking their status from initial receipt to final sign-off or cancellation.
pub struct LabResult;

impl VertexSchema for LabResult {
    fn schema_name() -> &'static str {
        "LabResult"
    }

    /// Returns the list of property constraints for the LabResult vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex this result belongs to. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("lab_order_id", false)
                .with_description("ID of the original LabOrder that generated this result. Optional.")
                .with_data_type(DataType::Integer),

            // --- Result Data Fields ---
            PropertyConstraint::new("test_name", true)
                .with_description("Human-readable name of the test performed.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("value", true)
                .with_description("The numeric or qualitative result value.")
                .with_data_type(DataType::String) // Stored as String for flexibility (e.g., 'Positive', '15.2')
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("unit", false)
                .with_description("Unit of measure (e.g., 'mg/dL').")
                .with_data_type(DataType::String),

            PropertyConstraint::new("reference_range", false)
                .with_description("The normal range for this test (e.g., '10-20').")
                .with_data_type(DataType::String),

            PropertyConstraint::new("abnormal_flag", false)
                .with_description("Flag indicating if the result is abnormal (H, L, W, N, etc.).")
                .with_data_type(DataType::String),

            PropertyConstraint::new("resulted_at", true)
                .with_description("Timestamp when the result was determined/entered.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("notes", false)
                .with_description("Any supplementary notes or comments from the lab technician.")
                .with_data_type(DataType::String),

            // --- Lifecycle Status Field ---
            PropertyConstraint::new("status", true)
                .with_description("The clinical status of the lab result.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "RECEIVED".to_string(),  // Initial state, data is present but not validated
                    "FINAL".to_string(),     // Validated and approved for clinical use
                    "AMENDED".to_string(),   // Previously FINAL but has been corrected
                    "CANCELED".to_string(),  // Retracted due to error
                ]))
                .with_default_value(serde_json::Value::String("RECEIVED".to_string())),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to manage result validity.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("RECEIVED".to_string()),
                transitions: vec![
                    // 1. Validation: RECEIVED -> FINAL
                    StateTransition {
                        from_state: "RECEIVED".to_string(),
                        to_state: "FINAL".to_string(),
                        required_rules: vec!["require_pathologist_signoff".to_string()],
                        triggers_events: vec!["lab_result.finalized".to_string()],
                    },
                    // 2. Correction: FINAL -> AMENDED
                    StateTransition {
                        from_state: "FINAL".to_string(),
                        to_state: "AMENDED".to_string(),
                        required_rules: vec!["require_amendment_reason_and_signoff".to_string()],
                        triggers_events: vec!["lab_result.amended".to_string()],
                    },
                    // 3. Retraction: FINAL -> CANCELED
                    StateTransition {
                        from_state: "FINAL".to_string(),
                        to_state: "CANCELED".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["lab_result.canceled".to_string()],
                    },
                    // Note: AMENDED and CANCELED are typically terminal states, not allowing transition back to FINAL.
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for laboratory tests.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "LOINC_Codes".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("https://loinc.org".to_string()),
                reference_uri: Some("https://loinc.org".to_string()),
                description: Some("Logical Observation Identifiers Names and Codes (LOINC) for test names.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for LabResult CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("lab_result.created".to_string()),
            update_topic: Some("lab_result.updated".to_string()),
            deletion_topic: Some("lab_result.deleted".to_string()),
            error_queue: Some("lab_result.errors".to_string()),
        }
    }
}
