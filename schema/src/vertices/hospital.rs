use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Hospital vertex type.
///
/// Description: Represents a healthcare institution or facility. It links to other entities
/// like Address, Users (admin), and potentially other medical entities (like Departments).
pub struct Hospital;

impl Hospital {
    /// Provides the possible operational status values for the hospital.
    fn status_values() -> Vec<String> {
        vec![
            "Active".to_string(),
            "Under_Construction".to_string(),
            "Pending_Review".to_string(),
            "Closed".to_string(),
            "Decommissioned".to_string(),
        ]
    }
}

impl VertexSchema for Hospital {
    fn schema_name() -> &'static str {
        "Hospital"
    }

    /// Returns the list of property constraints for the Hospital vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("name", true)
                .with_description("Official legal or operational name of the hospital.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            // --- Relational IDs (Expected to form Edges) ---
            PropertyConstraint::new("address_id", true)
                .with_description("ID of the linked Address vertex for the primary location.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("admin_contact_user_id", false)
                .with_description("ID of the User vertex designated as the primary administrative contact.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),

            // --- Contact Information ---
            PropertyConstraint::new("phone", false)
                .with_description("Primary public contact phone number.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            PropertyConstraint::new("website", false)
                .with_description("Official website URL.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            // --- Operational Status and Timestamps ---
            PropertyConstraint::new("status", true)
                .with_description("The current operational status of the facility.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Hospital::status_values()))
                .with_default_value(JsonValue::String("Active".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp of record creation. Required, Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp of the last record update. Required, Mutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
        ]
    }

    /// Lifecycle manages the operational status of the hospital entity.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Active".to_string()),
                transitions: vec![
                    // Standard operations
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Closed".to_string(),
                        required_rules: vec!["require_closure_reason".to_string()],
                        triggers_events: vec!["hospital.closure_announced".to_string()],
                    },
                    StateTransition {
                        from_state: "Closed".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hospital.reopened".to_string()],
                    },
                    // Setup/Review flow
                    StateTransition {
                        from_state: "Under_Construction".to_string(),
                        to_state: "Pending_Review".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hospital.ready_for_review".to_string()],
                    },
                    StateTransition {
                        from_state: "Pending_Review".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hospital.activated".to_string()],
                    },
                    // Final state
                    StateTransition {
                        from_state: "*".to_string(),
                        to_state: "Decommissioned".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hospital.data_archived".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for facility classification and identification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "NAICS_Code".to_string(),
                ontology_system_id: "NAICS".to_string(),
                uri: Some("https://www.census.gov/naics/".to_string()),
                reference_uri: Some("622000".to_string()), // General Hospital code
                description: Some("North American Industry Classification System code for Hospital.".to_string()),
            },
            OntologyReference {
                name: "NPI_OrganizationTaxonomy".to_string(),
                ontology_system_id: "NPI".to_string(),
                uri: Some("https://www.cms.gov/regulations-and-guidance/administrative-simplification/national-provider-identifier/national-plan-and-provider-enumeration-system-nppes".to_string()),
                reference_uri: None,
                description: Some("Taxonomy codes used by the National Provider Identifier (NPI) for organization types.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Hospital entity management.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("hospital.created".to_string()),
            update_topic: Some("hospital.updated".to_string()),
            deletion_topic: Some("hospital.decommissioned".to_string()),
            error_queue: Some("hospital.admin_errors".to_string()),
        }
    }
}
