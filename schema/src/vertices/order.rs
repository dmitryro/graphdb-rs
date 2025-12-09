use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition, SchemaAction,
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Order vertex type.
///
/// Description: Represents a clinical order or instruction placed by a provider
/// for a patient (e.g., Medication, Lab, Imaging, Diet, Activity).
pub struct Order;

impl VertexSchema for Order {
    fn schema_name() -> &'static str {
        "Order"
    }

    /// Returns the property constraints for the Order vertex.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the Patient the order is for. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("encounter_id", true)
                .with_description("Reference ID to the Encounter during which the order was placed. Immutable.")
                .with_data_type(DataType::Uuid)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("ordered_by", true)
                .with_description("User ID of the provider who placed the order. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Order Specification ---
            PropertyConstraint::new("order_type", true)
                .with_description("Classification of the order (e.g., MEDICATION, LAB, DIET).")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "ADMIT".to_string(), "DIET".to_string(), "ACTIVITY".to_string(),
                    "VITALS".to_string(), "MEDICATION".to_string(), "LAB".to_string(),
                    "IMAGING".to_string(), "CONSULT".to_string(), "IV_FLUIDS".to_string()
                ]))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("order_details", true)
                .with_description("Detailed textual instruction for the order.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            // --- Specific Order Fields (Optional based on type) ---
            // Note: In a production system, these specialized fields might be structured as edges
            // or nested properties, but we include them as strings here based on the model.
            PropertyConstraint::new("diet", false).with_data_type(DataType::String),
            PropertyConstraint::new("activity", false).with_data_type(DataType::String),
            PropertyConstraint::new("vitals_frequency", false).with_data_type(DataType::String),
            PropertyConstraint::new("iv_fluids", false).with_data_type(DataType::String),
            PropertyConstraint::new("medications", false).with_data_type(DataType::String),
            PropertyConstraint::new("labs", false).with_data_type(DataType::String),
            PropertyConstraint::new("imaging", false).with_data_type(DataType::String),
            PropertyConstraint::new("notes", false).with_data_type(DataType::String),

            // --- Lifecycle & Audit ---
            PropertyConstraint::new("status", true)
                .with_description("The execution status of the order.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "ORDERED".to_string(), "HELD".to_string(), "IN_PROGRESS".to_string(),
                    "COMPLETED".to_string(), "CANCELLED".to_string(), "DISCONTINUED".to_string()
                ]))
                .with_default_value(JsonValue::String("ORDERED".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("ordered_at", true)
                .with_description("The UTC timestamp when the order was placed. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("completed_at", false)
                .with_description("The UTC timestamp when the order was completed or discontinued.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional]),

        ]
    }

    /// Defines the operational lifecycle for clinical orders.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("ORDERED".to_string()),
                transitions: vec![
                    // 1. ORDERED -> IN_PROGRESS (Staff acknowledges and starts execution)
                    StateTransition {
                        from_state: "ORDERED".to_string(),
                        to_state: "IN_PROGRESS".to_string(),
                        required_rules: vec!["require_staff_acknowledgement".to_string()],
                        triggers_events: vec!["order.started".to_string()],
                    },
                    // 2. IN_PROGRESS -> COMPLETED (Order executed)
                    StateTransition {
                        from_state: "IN_PROGRESS".to_string(),
                        to_state: "COMPLETED".to_string(),
                        required_rules: vec!["require_documentation_of_completion".to_string()],
                        triggers_events: vec!["order.completed".to_string()],
                    },
                    // 3. Any active state -> CANCELLED/DISCONTINUED (Provider decides to stop)
                    StateTransition {
                        from_state: "ORDERED".to_string(),
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec!["require_provider_signature".to_string()],
                        triggers_events: vec!["order.cancelled".to_string()],
                    },
                    StateTransition {
                        from_state: "IN_PROGRESS".to_string(),
                        to_state: "DISCONTINUED".to_string(),
                        required_rules: vec!["require_provider_signature".to_string()],
                        triggers_events: vec!["order.discontinued".to_string()],
                    },
                ],
                pre_action_checks: vec![
                    // Example: Check for drug interactions before executing a medication order
                    SchemaRule::new("check_provider_credentials"),
                    SchemaRule::new("check_for_critical_alerts"),
                ],
                post_action_actions: vec![
                    SchemaAction::FunctionCall {
                        function_name: "auto_set_completed_at".to_string(),
                        arguments: vec![],
                    },
                ],
            }
        ]
    }

    /// References to standard terminologies used for order types and procedures.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "SNOMEDCTProcedure".to_string(),
                ontology_system_id: "SNOMEDCT".to_string(),
                uri: Some("https://www.snomed.org/".to_string()),
                reference_uri: None,
                description: Some("SNOMED CT codes for standardized clinical procedures and actions.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics related to this schema element.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("clinical.order_placed".to_string()),
            update_topic: Some("clinical.order_status_update".to_string()),
            deletion_topic: None, // Orders are cancelled/discontinued, not deleted
            error_queue: Some("clinical.order_error_management".to_string()),
        }
    }
}
