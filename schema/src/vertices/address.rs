use serde_json::json;

use crate::definitions::VertexSchema;
use crate::properties::{
    PropertyDefinition, OntologyReference,
};
use crate::constraints::{ PropertyConstraint, Constraint, DataType, EnumValues };
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};

/// Implementation of the VertexSchema for the Address vertex type.
pub struct Address;

impl VertexSchema for Address {
    fn schema_name() -> &'static str {
        "Address"
    }

    /// Returns the list of property constraints for the Address vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // 1. Address Line 1 - Required
            PropertyConstraint::new("address_line1", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::MinLength(1)])
                .with_description("The primary street address line. Required."),

            // 2. Address Line 2 - Optional
            PropertyConstraint::new("address_line2", false)
                .with_data_type(DataType::String)
                .with_description("Optional secondary address information (e.g., apartment number)."),

            // 3. City - Required
            PropertyConstraint::new("city", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::MinLength(1)])
                .with_description("The city name. Required."),

            // 4. State/Province ID (Foreign Key to StateProvince vertex) - Required
            PropertyConstraint::new("state_province_id", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![
                    Constraint::MinLength(2),
                    Constraint::Immutable, // State reference should not change post-creation
                ])
                .with_description("Identifier of the related StateProvince vertex (Foreign Key). Required."),

            // 5. Postal Code - Required
            PropertyConstraint::new("postal_code", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::MinLength(3)])
                .with_description("The postal code or ZIP code. Required."),

            // 6. Country Code - Required
            PropertyConstraint::new("country_code", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![
                    Constraint::MinLength(2),
                    Constraint::Pattern(r"^[A-Z]{2,3}$".to_string()), // ISO country codes
                ])
                .with_description("ISO 3166-1 alpha-2 or alpha-3 country code. Required."),

            // 7. Validity Status - Required for Lifecycle Tracking
            PropertyConstraint::new("validity_status", true)
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "Current".to_string(),
                    "Historical".to_string(),
                    "Invalid".to_string(),
                ]))
                .with_description("The current operational status of the address record (e.g., Current, Historical). Defaults to 'Current'."),
        ]
    }

    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "validity_status".to_string(),
                initial_state: Some("Current".to_string()),
                transitions: vec![
                    StateTransition {
                        from_state: "Current".to_string(),
                        to_state: "Historical".to_string(),
                        required_rules: vec!["Newer_Address_Exists".to_string()],
                        triggers_events: vec!["ADDRESS_BECAME_HISTORICAL".to_string()],
                    },
                    StateTransition {
                        from_state: "Current".to_string(),
                        to_state: "Invalid".to_string(),
                        required_rules: vec!["Address_Verification_Failed".to_string()],
                        triggers_events: vec!["ADDRESS_INVALIDATED".to_string()],
                    },
                    StateTransition {
                        from_state: "Historical".to_string(),
                        to_state: "Current".to_string(),
                        required_rules: vec!["Verified_Is_Latest_Address".to_string()],
                        triggers_events: vec!["ADDRESS_REVERTED_TO_CURRENT".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![
                    SchemaAction::PublishMessage {
                        topic: "address_lifecycle_changes".to_string(),
                        payload_template: "Address {id} changed status from {old_status} to {new_status}".to_string()
                    }
                ],
            }
        ]
    }

    fn ontology_references() -> Vec<OntologyReference> {
        // References to external geographic standards (e.g., ISO for country codes)
        vec![
            OntologyReference {
                name: "ISO 3166-1".to_string(),
                ontology_system_id: "ISO3166_1".to_string(),
                // FIX: Added 'uri' to satisfy the struct definition requirement.
                uri: Some("https://www.iso.org/iso-3166-country-codes.html".to_string()),
                // The backward-compatible field
                reference_uri: Some("https://www.iso.org/iso-3166-country-codes.html".to_string()),
                description: Some("Used for validation of country_code property.".to_string()),
            },
        ]
    }

    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("address.created".to_string()),
            update_topic: Some("address.updated".to_string()),
            deletion_topic: Some("address.deleted".to_string()),
            error_queue: Some("address.errors".to_string()),
        }
    }
}