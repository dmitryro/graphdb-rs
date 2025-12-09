use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Insurance vertex type (Payer/Company).
///
/// Description: Represents a third-party insurance company or payer organization.
/// This vertex is generally static but tracks operational readiness (e.g., claims integration).
pub struct Insurance;

impl Insurance {
    /// Provides the possible claims integration statuses.
    fn claims_integration_status_values() -> Vec<String> {
        vec![
            "PendingSetup".to_string(),      // Initial state, configuration in progress
            "ActiveEDI".to_string(),        // Live Electronic Data Interchange (EDI) claims submission
            "ManualSubmission".to_string(), // Requires manual paper or portal submission
            "Inactive".to_string(),         // Not currently used or supported
        ]
    }
}

impl VertexSchema for Insurance {
    fn schema_name() -> &'static str {
        "Insurance"
    }

    /// Returns the list of property constraints for the Insurance vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("name", true)
                .with_description("Legal name of the insurance company or payer.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            // --- Operational Details ---
            PropertyConstraint::new("contact_info", true)
                .with_description("Primary contact information (phone, address, website).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
            
            PropertyConstraint::new("coverage_details", false)
                .with_description("General information about common plans or services covered.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Mutable]),

            // --- Workflow and Integration ---
            PropertyConstraint::new("claims_integration_status", true)
                .with_description("The current operational status of electronic claims integration.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Insurance::claims_integration_status_values()))
                .with_default_value(JsonValue::String("PendingSetup".to_string()))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
        ]
    }

    /// Lifecycle tracks the operational status of claims integration.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "claims_integration_status".to_string(),
                initial_state: Some("PendingSetup".to_string()),
                transitions: vec![
                    StateTransition {
                        from_state: "PendingSetup".to_string(),
                        to_state: "ActiveEDI".to_string(),
                        required_rules: vec!["require_edi_certificate".to_string()],
                        triggers_events: vec!["insurance.edi_activated".to_string()],
                    },
                    StateTransition {
                        from_state: "PendingSetup".to_string(),
                        to_state: "ManualSubmission".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["insurance.manual_only".to_string()],
                    },
                    StateTransition {
                        from_state: "*".to_string(), // Can be deactivated from any state
                        to_state: "Inactive".to_string(),
                        required_rules: vec!["require_inactivation_reason".to_string()],
                        triggers_events: vec!["insurance.deactivated".to_string()],
                    },
                    StateTransition {
                        from_state: "Inactive".to_string(),
                        to_state: "PendingSetup".to_string(), // Can reactivate process
                        required_rules: vec![],
                        triggers_events: vec!["insurance.reactivation_started".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for medical coding.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "NAIC_Code".to_string(),
                ontology_system_id: "NAIC".to_string(),
                uri: Some("https://www.naic.org/".to_string()),
                reference_uri: None,
                description: Some("National Association of Insurance Commissioners (NAIC) company codes.".to_string()),
            },
            OntologyReference {
                name: "NPI_Payer".to_string(),
                ontology_system_id: "NPI".to_string(),
                uri: Some("https://www.cms.gov/regulations-and-guidance/administrative-simplification/national-provider-identifier".to_string()),
                reference_uri: None,
                description: Some("Payer National Provider Identifier (NPI) when used as a billing entity.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Insurance entity management.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("insurance.registered".to_string()),
            update_topic: Some("insurance.integration_status_changed".to_string()),
            deletion_topic: None, // Typically only inactivated, not deleted
            error_queue: Some("insurance.claims_submission_errors".to_string()),
        }
    }
}
