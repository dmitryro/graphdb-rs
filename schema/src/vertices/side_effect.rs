use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the SideEffect vertex type.
///
/// This vertex captures and verifies reported adverse effects associated with a medication.
pub struct SideEffect;

impl VertexSchema for SideEffect {
    fn schema_name() -> &'static str {
        "SideEffect"
    }

    /// Returns the list of property constraints for the SideEffect vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("medication_id", true)
                .with_description("The ID of the Medication vertex that this side effect is associated with.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Descriptive Data ---
            PropertyConstraint::new("description", true)
                .with_description("A detailed, clinical description of the reported side effect/adverse event.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::MinLength(10)]),

            PropertyConstraint::new("severity", true)
                .with_description("The perceived severity of the side effect.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable])
                .with_enum_values(EnumValues::new(vec![
                    "MILD".to_string(),
                    "MODERATE".to_string(),
                    "SEVERE".to_string(),
                    "LIFE_THREATENING".to_string(),
                ])),

            PropertyConstraint::new("onset", false)
                .with_description("Description of when the side effect began (e.g., '1 hour after first dose').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("duration", false)
                .with_description("Description of how long the side effect lasted.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            // --- Status and Time ---
            PropertyConstraint::new("status", true)
                .with_description("The verification status of the reported side effect.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "REPORTED".to_string(),     // Initial submission
                    "IN_REVIEW".to_string(),    // Clinician or Pharmacovigilance reviewing
                    "VERIFIED".to_string(),     // Confirmed adverse event
                    "REJECTED".to_string(),     // Found to be unrelated or spurious
                ]))
                .with_default_value(JsonValue::String("REPORTED".to_string())),

            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the side effect report was submitted.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp when the side effect record was last modified (e.g., status change, severity update).")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, governing verification flow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("REPORTED".to_string()),
                transitions: vec![
                    // 1. REPORTED -> IN_REVIEW: Assignment to a reviewer
                    StateTransition {
                        from_state: "REPORTED".to_string(),
                        to_state: "IN_REVIEW".to_string(),
                        required_rules: vec!["require_reviewer_assignment".to_string()],
                        triggers_events: vec!["side_effect.review_started".to_string()],
                    },
                    // 2. IN_REVIEW -> VERIFIED: Confirmation of the adverse event
                    StateTransition {
                        from_state: "IN_REVIEW".to_string(),
                        to_state: "VERIFIED".to_string(),
                        required_rules: vec!["require_clinical_confirmation".to_string()],
                        triggers_events: vec!["side_effect.verified".to_string()],
                    },
                    // 3. IN_REVIEW -> REJECTED: Report deemed spurious
                    StateTransition {
                        from_state: "IN_REVIEW".to_string(),
                        to_state: "REJECTED".to_string(),
                        required_rules: vec!["require_rejection_justification".to_string()],
                        triggers_events: vec!["side_effect.rejected".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references for standardizing the description and severity.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "MedDRA".to_string(),
                ontology_system_id: "MedDRA_Adverse_Event_Terminology".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Uses the Medical Dictionary for Regulatory Activities (MedDRA) for coding adverse event descriptions.".to_string()),
            },
            OntologyReference {
                name: "SNOMED_CT_Severity".to_string(),
                ontology_system_id: "SNOMED_CT_Clinical_Terms".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("References SNOMED CT terminology for standardized severity grading.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for SideEffect lifecycle events, crucial for pharmacovigilance.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("side_effect.newly_reported".to_string()),
            update_topic: Some("side_effect.review_status_change".to_string()),
            deletion_topic: None, // Reports should be retained for auditing
            error_queue: Some("side_effect.ingestion_errors".to_string()),
        }
    }
}
