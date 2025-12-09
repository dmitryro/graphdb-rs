use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the MedicalInteraction vertex type.
///
/// Description: Represents a known drug-drug or drug-substance interaction derived from
/// clinical literature. This vertex connects two Medication or Substance vertices.
pub struct MedicalInteraction;

impl VertexSchema for MedicalInteraction {
    fn schema_name() -> &'static str {
        "MedicalInteraction"
    }

    /// Returns the property constraints for the MedicalInteraction vertex.
    /// This vertex acts as reference data, so its properties are largely stable.
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

            // --- Interaction Details ---
            PropertyConstraint::new("severity", true)
                .with_description("Clinical severity of the interaction.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Major".to_string(), "Moderate".to_string(), "Minor".to_string(), "Unknown".to_string()
                ]))
                .with_default_value(JsonValue::String("Unknown".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("mechanism", true)
                .with_description("The pharmacological mechanism causing the interaction (e.g., CYP450 inhibition).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("clinical_significance", true)
                .with_description("Detailed description of the clinical outcomes.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            // --- Status and Audit ---
            PropertyConstraint::new("status", true)
                .with_description("The verification status of this interaction record.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Verified".to_string(), "Preliminary".to_string(), "Disputed".to_string()
                ]))
                .with_default_value(JsonValue::String("Preliminary".to_string()))
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines the lifecycle for MedicalInteraction, focused on verification status.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Preliminary".to_string()),
                transitions: vec![
                    // 1. Preliminary -> Verified (Requires clinical sign-off)
                    StateTransition {
                        from_state: "Preliminary".to_string(),
                        to_state: "Verified".to_string(),
                        required_rules: vec!["require_pharmacist_review".to_string()],
                        triggers_events: vec!["medical_interaction.verified".to_string()],
                    },
                    // 2. Verified -> Disputed (Requires evidence/documentation of dispute)
                    StateTransition {
                        from_state: "Verified".to_string(),
                        to_state: "Disputed".to_string(),
                        required_rules: vec!["require_clinical_documentation".to_string()],
                        triggers_events: vec!["medical_interaction.disputed".to_string()],
                    },
                    // 3. Disputed -> Verified (Requires secondary review)
                    StateTransition {
                        from_state: "Disputed".to_string(),
                        to_state: "Verified".to_string(),
                        required_rules: vec!["require_secondary_review".to_string()],
                        triggers_events: vec!["medical_interaction.reverified".to_string()],
                    },
                    // 4. Disputed -> Preliminary (Goes back to research/preliminary state for re-evaluation)
                    StateTransition {
                        from_state: "Disputed".to_string(),
                        to_state: "Preliminary".to_string(),
                        required_rules: vec!["require_data_review".to_string()],
                        triggers_events: vec!["medical_interaction.re_evaluation".to_string()],
                    },
                ],
                // Require clinical authority to change status or edit significance/severity.
                pre_action_checks: vec![
                    SchemaRule::new("can_verify_medical_data"),
                    SchemaRule::new("medication_ids_immutable"),
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to pharmacology ontologies.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "Pharmacokinetic_Interaction".to_string(),
                ontology_system_id: "DrugInteractionKnowledgeBase".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Classification of interaction based on PK/PD mechanisms.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics, crucial for downstream systems that check for drug safety.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("safety.interaction_detected".to_string()),
            update_topic: Some("safety.interaction_severity_updated".to_string()),
            deletion_topic: None, // Interactions are permanent records
            error_queue: Some("safety.interaction_alerts".to_string()),
        }
    }
}
