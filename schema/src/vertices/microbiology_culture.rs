use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition, SchemaAction,
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the MicrobiologyCulture vertex type.
///
/// Description: Represents the detailed result of a specific culture component, often linked
/// to a parent LabResult. Includes the finding, identified organism, and drug sensitivity data.
pub struct MicrobiologyCulture;

impl VertexSchema for MicrobiologyCulture {
    fn schema_name() -> &'static str {
        "MicrobiologyCulture"
    }

    /// Returns the property constraints for the MicrobiologyCulture vertex.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("lab_result_id", true)
                .with_description("Reference ID to the parent lab result this culture belongs to. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("culture_result", true)
                .with_description("The summary finding of the culture (e.g., Positive, Negative, Inconclusive).")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Positive".to_string(), "Negative".to_string(), "Inconclusive".to_string()
                ]))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("organism_identified", false)
                .with_description("The specific name of the organism identified, if applicable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("sensitivity", false)
                .with_description("Antibiogram or sensitivity data, often a structured JSON string.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("resulted_at", true)
                .with_description("The UTC timestamp when the culture result was finalized and reported.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Audit & Lifecycle ---
            PropertyConstraint::new("status", true)
                .with_description("The lifecycle status of the result.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Pending".to_string(), "Preliminary".to_string(), "Final".to_string(), "Corrected".to_string()
                ]))
                .with_default_value(JsonValue::String("Preliminary".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("created_at", true)
                .with_description("The UTC timestamp when the record was initially created. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("The UTC timestamp of the last modification. Automatically updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines the lifecycle for laboratory results, transitioning from preliminary to final status.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Preliminary".to_string()),
                transitions: vec![
                    // 1. Preliminary -> Final (Requires verification and sign-off by a lab director)
                    StateTransition {
                        from_state: "Preliminary".to_string(),
                        to_state: "Final".to_string(),
                        required_rules: vec!["require_verification".to_string(), "lock_resulted_data".to_string()],
                        triggers_events: vec!["culture.finalized".to_string()],
                    },
                    // 2. Final -> Corrected (Only permissible transition from Final, requires justification)
                    StateTransition {
                        from_state: "Final".to_string(),
                        to_state: "Corrected".to_string(),
                        required_rules: vec!["require_justification".to_string()],
                        triggers_events: vec!["culture.correction_started".to_string()],
                    },
                ],
                // Global checks before any status change
                pre_action_checks: vec![
                    SchemaRule::new("check_laboratory_access_rights"),
                    SchemaRule::new("audit_log_entry_required"),
                ],
                // Post-action ensures the 'updated_at' timestamp is always current.
                post_action_actions: vec![
                    // Use GraphMutation to signal the internal system action to update the timestamp.
                    SchemaAction::GraphMutation {
                        mutation_type: "update_updated_at".to_string(),
                        target_schema: Self::schema_name().to_string(),
                    },
                ],
            }
        ]
    }

    /// References to standard terminologies used for laboratory results.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "LOINCCultureResult".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("http://loinc.org/".to_string()),
                reference_uri: None,
                description: Some("LOINC codes for microbiology culture results and organisms.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics related to this schema element.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("lab.culture_created".to_string()),
            update_topic: Some("lab.culture_updated".to_string()),
            deletion_topic: None,
            error_queue: Some("lab.culture_processing_errors".to_string()),
        }
    }
}
