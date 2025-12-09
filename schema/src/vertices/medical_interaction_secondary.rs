use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the MedicalInteractionSecondary vertex type.
///
/// Description: Represents a specific, documented interaction between two distinct
/// medical entities (medications/substances), detailing the specific risk and severity.
pub struct MedicalInteractionSecondary;

impl VertexSchema for MedicalInteractionSecondary {
    fn schema_name() -> &'static str {
        "MedicalInteractionSecondary"
    }

    /// Returns the property constraints for the MedicalInteractionSecondary vertex.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("primary_medication_id", true)
                .with_description("Reference ID to the first medication/substance involved. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("secondary_medication_id", true)
                .with_description("Reference ID to the second medication/substance involved. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("severity", true)
                .with_description("The clinical severity of the specific interaction.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Contraindicated".to_string(), "High_Risk".to_string(), "Monitor_Closely".to_string(), "Minor_Effect".to_string()
                ]))
                .with_default_value(JsonValue::String("Monitor_Closely".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("description", false)
                .with_description("Detailed context and management recommendations for this interaction.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            // Status is used for the lifecycle management
            PropertyConstraint::new("status", true)
                .with_description("The review status of this specific interaction pair.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Draft".to_string(), "In_Review".to_string(), "Published".to_string(), "Superseded".to_string()
                ]))
                .with_default_value(JsonValue::String("Draft".to_string()))
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines the lifecycle for MedicalInteractionSecondary, focusing on quality control and review.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Draft".to_string()),
                transitions: vec![
                    // 1. Draft -> In_Review (Start QA process)
                    StateTransition {
                        from_state: "Draft".to_string(),
                        to_state: "In_Review".to_string(),
                        required_rules: vec!["require_data_completeness".to_string()],
                        triggers_events: vec!["secondary_interaction.review_started".to_string()],
                    },
                    // 2. In_Review -> Published (Ready for use in production systems)
                    StateTransition {
                        from_state: "In_Review".to_string(),
                        to_state: "Published".to_string(),
                        required_rules: vec!["require_peer_review_signoff".to_string()],
                        triggers_events: vec!["secondary_interaction.published".to_string()],
                    },
                    // 3. Published -> Superseded (Replaced by newer evidence/record)
                    StateTransition {
                        from_state: "Published".to_string(),
                        to_state: "Superseded".to_string(),
                        required_rules: vec!["require_replacement_reference".to_string()],
                        triggers_events: vec!["secondary_interaction.superseded".to_string()],
                    },
                    // 4. In_Review -> Draft (Reject/send back for correction)
                    StateTransition {
                        from_state: "In_Review".to_string(),
                        to_state: "Draft".to_string(),
                        required_rules: vec!["require_correction_notes".to_string()],
                        triggers_events: vec!["secondary_interaction.corrections_needed".to_string()],
                    },
                ],
                // These checks apply to any action attempting to change the status property
                pre_action_checks: vec![
                    SchemaRule::new("medication_pair_exists"),
                    SchemaRule::new("severity_is_valid"),
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to pharmacological knowledge bases.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "InteractionSeverityScale".to_string(),
                ontology_system_id: "InternalRiskMatrix".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Internal scale used to grade the risk of the specific drug combination.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics, crucial for decision support systems.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("safety.secondary_interaction_created".to_string()),
            update_topic: Some("safety.secondary_interaction_severity_updated".to_string()),
            deletion_topic: None, // Records are superseded, not deleted
            error_queue: Some("safety.secondary_interaction_alerts".to_string()),
        }
    }
}
