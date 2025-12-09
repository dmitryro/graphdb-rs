use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the MedicalCode vertex type.
///
/// Description: Represents standardized medical codes (e.g., ICD-10, CPT, HCPCS)
/// used for billing, diagnosis, and procedures. These vertices are critical reference data.
pub struct MedicalCode;

impl VertexSchema for MedicalCode {
    fn schema_name() -> &'static str {
        "MedicalCode"
    }

    /// Returns the list of property constraints for the MedicalCode vertex.
    /// Most fields are immutable or unique, reflecting that a code definition is stable.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("code", true)
                .with_description("The actual standardized medical code string (e.g., 'E11.9'). Unique, Immutable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("code_type", true)
                .with_description("The system the code belongs to (e.g., 'ICD-10', 'CPT', 'HCPCS'). Immutable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Descriptive and Status Data ---
            PropertyConstraint::new("description", true)
                .with_description("The human-readable description of the code. Updatable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("status", true)
                .with_description("The current availability status of the code.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec!["Active".to_string(), "Inactive".to_string(), "Pending".to_string()]))
                .with_default_value(JsonValue::String("Active".to_string()))
                .with_constraints(vec![Constraint::Required]),

            // --- Audit Timestamps ---
            PropertyConstraint::new("created_at", true)
                .with_description("UTC timestamp of creation. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("UTC timestamp of last modification. Auto-managed.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines the lifecycle for a MedicalCode, allowing transition between active/inactive.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Pending".to_string()),
                transitions: vec![
                    // A code can only become Active if it's currently Pending
                    StateTransition {
                        from_state: "Pending".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["medical_code.activated".to_string()],
                    },
                    // A code can move from Active to Inactive (deactivation)
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Inactive".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["medical_code.deactivated".to_string()],
                    },
                    // An Inactive code might be reactivated
                    StateTransition {
                        from_state: "Inactive".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["medical_code.reactivated".to_string()],
                    },
                ],
                // Ensure only authorized parties can change code status or update descriptions.
                pre_action_checks: vec![
                    SchemaRule::new("can_manage_medical_codes"),
                    SchemaRule::new("code_and_type_immutable"),
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to medical and standards ontologies for classification context.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "Standard_Classification_System".to_string(),
                ontology_system_id: "MedicalCodingStandards".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Classification system (ICD, CPT, etc.) for standardized medical terms.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for updates to the code data.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("medical.code_added".to_string()),
            update_topic: Some("medical.code_updated".to_string()),
            deletion_topic: None, // Codes should generally be marked inactive, not deleted
            error_queue: Some("medical.code_management_alerts".to_string()),
        }
    }
}
