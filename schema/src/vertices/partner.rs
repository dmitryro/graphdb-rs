use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Partner vertex type.
///
/// This schema defines constraints for external organizational partners (e.g., Pharmacy,
/// Insurance), manages their lifecycle status, and ensures data integrity.
pub struct Partner;

impl VertexSchema for Partner {
    fn schema_name() -> &'static str {
        "Partner"
    }

    /// Returns the list of property constraints for the Partner vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Fields from the Model ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("partner_type", true)
                .with_description("The type of partner (e.g., 'Pharmacy', 'Laboratory', 'Insurance'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "Pharmacy".to_string(),
                    "Laboratory".to_string(),
                    "Medical Supplier".to_string(),
                    "Insurance".to_string(),
                    "Clinic".to_string(),
                    "Other".to_string(),
                ])),

            PropertyConstraint::new("name", true)
                .with_description("The official name of the partner organization. Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("contact_person_user_id", false)
                .with_description("The ID of the internal User who acts as the primary contact. Optional.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Indexable]),

            PropertyConstraint::new("phone", false)
                .with_description("Partner phone number. Optional.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("email", false)
                .with_description("Partner email address. Must be unique if provided.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Unique]),

            PropertyConstraint::new("address", false)
                .with_description("Physical address of the partner. Optional.")
                .with_data_type(DataType::String),

            // --- System/Lifecycle Fields ---
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp of vertex creation. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp of last vertex update.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
                
            // --- State Management Field (Required for complex lifecycle) ---
            PropertyConstraint::new("status", true)
                .with_description("The current engagement status of the Partner (String). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "PROSPECT".to_string(), // Initial state, potential partner
                    "ACTIVE".to_string(),  // Currently engaged and active
                    "SUSPENDED".to_string(), // Temporarily halted engagement
                    "ARCHIVED".to_string(), // Relationship terminated
                ]))
                .with_default_value(JsonValue::String("PROSPECT".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, managing the engagement state.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PROSPECT".to_string()),
                transitions: vec![
                    // 1. Onboarding/Activation: PROSPECT -> ACTIVE
                    StateTransition {
                        from_state: "PROSPECT".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_contract_signed".to_string()],
                        triggers_events: vec!["partner.activated".to_string()],
                    },
                    // 2. Temporary Halt: ACTIVE -> SUSPENDED
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "SUSPENDED".to_string(),
                        required_rules: vec!["require_admin_approval".to_string()],
                        triggers_events: vec!["partner.suspended".to_string()],
                    },
                    // 3. Resumption: SUSPENDED -> ACTIVE
                    StateTransition {
                        from_state: "SUSPENDED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_legal_review".to_string()],
                        triggers_events: vec!["partner.resumed".to_string()],
                    },
                    // 4. Termination: ACTIVE/SUSPENDED -> ARCHIVED
                    StateTransition {
                        from_state: "*".to_string(), // Can transition from any state to ARCHIVED
                        to_state: "ARCHIVED".to_string(),
                        required_rules: vec!["require_final_signoff".to_string()],
                        triggers_events: vec!["partner.archived".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                // Example: Trigger an update of the 'updated_at' field on any state change.
                post_action_actions: vec![
                    SchemaAction::FunctionCall {
                        function_name: "set_timestamp_now".to_string(),
                        arguments: vec!["updated_at".to_string()],
                    }
                ],
            }
        ]
    }

    /// Defines standard organization/jurisdiction ontology references.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "DUNS_Number".to_string(),
                ontology_system_id: "DUNS".to_string(),
                uri: Some("https://www.dnb.com/duns.html".to_string()),
                reference_uri: None,
                description: Some("Global organization identifier.".to_string()),
            },
            OntologyReference {
                name: "NPI_Identifier".to_string(),
                ontology_system_id: "NPI".to_string(),
                uri: Some("https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProviderIdentifier".to_string()),
                reference_uri: None,
                description: Some("National Provider Identifier (US-specific).".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Partner CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("partner.created".to_string()),
            update_topic: Some("partner.updated".to_string()),
            deletion_topic: Some("partner.deleted".to_string()),
            error_queue: Some("partner.errors".to_string()),
        }
    }
}
