use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue; // For default values in constraints

/// Implementation of the VertexSchema for the CarePlan vertex type.
///
/// Description: A formal plan of care for a patient, outlining goals, interventions, and responsible parties.
pub struct CarePlan;

impl CarePlan {
    /// Provides a list of standard statuses for a CarePlan.
    fn status_values() -> Vec<String> {
        vec![
            "Draft".to_string(),    // Plan is being created
            "Active".to_string(),   // Plan is currently in effect
            "OnHold".to_string(),   // Plan is temporarily suspended
            "Completed".to_string(),// Goals achieved or timeline ended
            "Cancelled".to_string(),// Plan aborted before completion
        ]
    }
}

impl VertexSchema for CarePlan {
    fn schema_name() -> &'static str {
        "CarePlan"
    }

    /// Returns the list of property constraints for the CarePlan vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex this plan is for. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("created_by_doctor_id", true)
                .with_description("ID of the Doctor vertex who created the plan. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            // --- Descriptive Properties ---
            PropertyConstraint::new("plan_name", true)
                .with_description("A brief, descriptive name for the care plan.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::MinLength(5)]),

            // --- Date Constraints ---
            PropertyConstraint::new("start_date", true)
                .with_description("The intended start date of the plan. Must be set.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("end_date", false)
                .with_description("The intended end date of the plan.")
                .with_data_type(DataType::DateTime),

            // --- Status and Content ---
            PropertyConstraint::new("status", true)
                .with_description("The current administrative status of the plan.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(CarePlan::status_values()))
                .with_default_value(JsonValue::String("Draft".to_string())),

            PropertyConstraint::new("goals", false)
                .with_description("Goals for the patient, expected to be stored as a JSON array of goal objects.")
                .with_data_type(DataType::Json),

            PropertyConstraint::new("interventions", false)
                .with_description("Interventions/activities defined by the plan, expected to be stored as a JSON array.")
                .with_data_type(DataType::Json),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to manage the care plan's state.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Draft".to_string()),
                transitions: vec![
                    // 1. Activation: Draft -> Active
                    StateTransition {
                        from_state: "Draft".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec!["require_approving_physician".to_string()],
                        triggers_events: vec!["care_plan.activated".to_string()],
                    },
                    // 2. Suspension: Active -> OnHold
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "OnHold".to_string(),
                        required_rules: vec!["require_hold_reason".to_string()],
                        triggers_events: vec!["care_plan.suspended".to_string()],
                    },
                    // 3. Resumption: OnHold -> Active
                    StateTransition {
                        from_state: "OnHold".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["care_plan.resumed".to_string()],
                    },
                    // 4. Finalization: Active -> Completed
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Completed".to_string(),
                        required_rules: vec!["require_outcome_summary".to_string()],
                        triggers_events: vec!["care_plan.completed".to_string()],
                    },
                    // 5. Termination: Active/Draft/OnHold -> Cancelled
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["care_plan.canceled".to_string()],
                    },
                    StateTransition {
                        from_state: "Draft".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["care_plan.canceled".to_string()],
                    },
                    StateTransition {
                        from_state: "OnHold".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["care_plan.canceled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for patient care plans.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "SNOMED_CT_Procedure".to_string(),
                ontology_system_id: "SNOMED".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: Some("http://snomed.info/sct".to_string()),
                description: Some("SNOMED CT codes for interventions and procedures.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for CarePlan operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("care_plan.created".to_string()),
            update_topic: Some("care_plan.updated".to_string()),
            deletion_topic: Some("care_plan.deleted".to_string()),
            error_queue: Some("care_plan.errors".to_string()),
        }
    }
}