use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition, SchemaAction,
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Nurse vertex type.
///
/// Description: Represents a registered nurse or clinical staff member, containing
/// professional details like license, specialty, and years of experience.
pub struct Nurse;

impl VertexSchema for Nurse {
    fn schema_name() -> &'static str {
        "Nurse"
    }

    /// Returns the property constraints for the Nurse vertex.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("user_id", true)
                .with_description("Reference ID to the core User account linked to this staff member. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            // --- Professional Data ---
            PropertyConstraint::new("license_number", true)
                .with_description("The official nursing license number. Required and Unique.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("specialty", false)
                .with_description("The nursing specialty (e.g., 'ER', 'ICU', 'Oncology').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("years_of_experience", true)
                .with_description("Total years of professional nursing experience.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            // --- Lifecycle & Audit ---
            PropertyConstraint::new("status", true)
                .with_description("The employment and license status of the nurse.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Active".to_string(), "Inactive".to_string(), "Suspended".to_string()
                ]))
                .with_default_value(JsonValue::String("Active".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("created_at", true)
                .with_description("The UTC timestamp when the nurse record was created. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("The UTC timestamp of the last modification. Automatically updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines the professional lifecycle of a nurse, tied to employment and license status.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Active".to_string()),
                transitions: vec![
                    // 1. Active -> Inactive (Resignation, retirement)
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Inactive".to_string(),
                        required_rules: vec!["require_offboarding_procedure".to_string()],
                        triggers_events: vec!["nurse.deactivated".to_string()],
                    },
                    // 2. Active -> Suspended (License issue, internal investigation)
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Suspended".to_string(),
                        required_rules: vec!["require_legal_signoff".to_string()],
                        triggers_events: vec!["nurse.suspended".to_string()],
                    },
                    // 3. Suspended -> Active (Issue resolved)
                    StateTransition {
                        from_state: "Suspended".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec!["require_reinstatement_approval".to_string()],
                        triggers_events: vec!["nurse.reinstated".to_string()],
                    },
                ],
                // Global checks before any professional data change
                pre_action_checks: vec![
                    SchemaRule::new("check_hr_management_rights"),
                    SchemaRule::new("validate_license_active"), // Check license validity against external system
                ],
                // Post-action ensures the 'updated_at' timestamp is always current.
                post_action_actions: vec![
                    SchemaAction::GraphMutation {
                        mutation_type: "update_updated_at".to_string(),
                        target_schema: Self::schema_name().to_string(),
                    },
                ],
            }
        ]
    }

    /// References to standard terminologies used for professional roles.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "NUCCProviderSpecialty".to_string(),
                ontology_system_id: "NUCC".to_string(),
                uri: Some("https://nucc.org/".to_string()),
                reference_uri: None,
                description: Some("NUCC codes for nursing specialties and job classifications.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics related to this schema element.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("staff.nurse_onboarded".to_string()),
            update_topic: Some("staff.nurse_profile_updated".to_string()),
            deletion_topic: None, // Staff records are deactivated, not deleted
            error_queue: Some("staff.hr_sync_errors".to_string()),
        }
    }
}
