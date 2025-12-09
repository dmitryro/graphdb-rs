use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the MedicalInteractionPrimary vertex type.
///
/// Description: Represents a primary, high-level classification of a drug interaction,
/// typically used as foundational knowledge linked to a specific medication.
pub struct MedicalInteractionPrimary;

impl VertexSchema for MedicalInteractionPrimary {
    fn schema_name() -> &'static str {
        "MedicalInteractionPrimary"
    }

    /// Returns the property constraints for the MedicalInteractionPrimary vertex.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("medication_id", true)
                .with_description("Reference ID to the primary medication this interaction is associated with. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("interaction_name", true)
                .with_description("A concise, human-readable name for the interaction (e.g., 'QT Prolongation Risk').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("interaction_class", true)
                .with_description("The technical class of the interaction (e.g., Pharmacokinetic (PK), Pharmacodynamic (PD), Contraindication).")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Pharmacokinetic".to_string(), "Pharmacodynamic".to_string(), "Contraindication".to_string(), "Synergistic".to_string()
                ]))
                .with_default_value(JsonValue::String("Pharmacodynamic".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("description", false)
                .with_description("Detailed clinical description of the interaction.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            // Status is used for the lifecycle management
            PropertyConstraint::new("status", true)
                .with_description("The lifecycle status of this primary interaction record.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Draft".to_string(), "Active".to_string(), "Archived".to_string()
                ]))
                .with_default_value(JsonValue::String("Draft".to_string()))
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines a simple lifecycle for managing the availability of the interaction record.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Draft".to_string()),
                transitions: vec![
                    // 1. Draft -> Active (Requires admin sign-off to be used in clinical decision support)
                    StateTransition {
                        from_state: "Draft".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec!["require_admin_activation".to_string()],
                        triggers_events: vec!["interaction.activated".to_string()],
                    },
                    // 2. Active -> Archived (Requires evidence of record invalidation or replacement)
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Archived".to_string(),
                        required_rules: vec!["require_invalidation_reason".to_string()],
                        triggers_events: vec!["interaction.archived".to_string()],
                    },
                    // 3. Archived -> Active (Requires justification for restoration)
                    StateTransition {
                        from_state: "Archived".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec!["require_restoration_justification".to_string()],
                        triggers_events: vec!["interaction.restored".to_string()],
                    },
                ],
                // Basic checks before any status change
                pre_action_checks: vec![
                    SchemaRule::new("can_edit_reference_data"),
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to pharmacological knowledge bases.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "DrugInteractionClassification".to_string(),
                ontology_system_id: "InternalKnowledgeBase".to_string(),
                uri: None,
                reference_uri: Some("https://example.com/interaction/classes".to_string()),
                description: Some("Internal classification system for drug interaction types (PK/PD).".to_string()),
            },
        ]
    }

    /// Defines the messaging topics.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("safety.primary_interaction_created".to_string()),
            update_topic: Some("safety.primary_interaction_updated".to_string()),
            deletion_topic: None, // Interactions are generally archived, not deleted
            error_queue: Some("safety.primary_interaction_errors".to_string()),
        }
    }
}
