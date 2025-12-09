use serde_json::json;

use crate::definitions::VertexSchema;
use crate::properties::{
    PropertyDefinition, OntologyReference,
};
use crate::constraints::{ PropertyConstraint, Constraint, DataType, EnumValues };
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};

/// Implementation of the VertexSchema for the BillingAddress vertex type.
/// This vertex captures the financial address information associated with a patient.
pub struct BillingAddress;

impl VertexSchema for BillingAddress {
    fn schema_name() -> &'static str {
        "BillingAddress"
    }

    /// Returns the list of property constraints for the BillingAddress vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // 1. Patient ID (Foreign Key to Patient vertex) - Required
            // Maps to the 'patient_id' field in the source model.
            PropertyConstraint::new("patient_id", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![
                    Constraint::MinLength(1),
                    Constraint::Immutable, // Link to patient should not change
                ])
                .with_description("Identifier of the related Patient vertex (Foreign Key). Required."),

            // 2. Address Line 1 - Required
            // Maps to the 'address' field in the source model.
            PropertyConstraint::new("address_line1", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::MinLength(1)])
                .with_description("The primary street address line (billing address). Required."),

            // 3. City - Required
            PropertyConstraint::new("city", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::MinLength(1)])
                .with_description("The city name. Required."),

            // 4. State/Province ID (Foreign Key to StateProvince vertex) - Required
            // Maps to the 'state_province' field (using its ID).
            PropertyConstraint::new("state_province_id", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![
                    Constraint::MinLength(2),
                    Constraint::Immutable,
                ])
                .with_description("Identifier of the related StateProvince vertex (Foreign Key). Required."),

            // 5. Postal Code - Required
            // Maps to the 'postal_code' field in the source model.
            PropertyConstraint::new("postal_code", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::MinLength(3)])
                .with_description("The postal code or ZIP code. Required."),

            // 6. Country Code - Required
            // Maps to the 'country' field in the source model (using the code).
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
                .with_description("The current operational status of the billing address record. Defaults to 'Current'."),
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
                        required_rules: vec!["Newer_Billing_Address_Exists".to_string()],
                        triggers_events: vec!["BILLING_ADDRESS_BECAME_HISTORICAL".to_string()],
                    },
                    StateTransition {
                        from_state: "Current".to_string(),
                        to_state: "Invalid".to_string(),
                        required_rules: vec!["Address_Verification_Failed".to_string()],
                        triggers_events: vec!["BILLING_ADDRESS_INVALIDATED".to_string()],
                    },
                    StateTransition {
                        from_state: "Historical".to_string(),
                        to_state: "Current".to_string(),
                        required_rules: vec!["Verified_Is_Latest_Billing_Address".to_string()],
                        triggers_events: vec!["BILLING_ADDRESS_REVERTED_TO_CURRENT".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![
                    SchemaAction::PublishMessage {
                        topic: "billing_address_lifecycle_changes".to_string(),
                        payload_template: "Billing Address {id} changed status from {old_status} to {new_status}".to_string()
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
                // Include both 'uri' and 'reference_uri'
                uri: Some("https://www.iso.org/iso-3166-country-codes.html".to_string()),
                reference_uri: Some("https://www.iso.org/iso-3166-country-codes.html".to_string()),
                description: Some("Used for validation of country_code property.".to_string()),
            },
        ]
    }

    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("billing_address.created".to_string()),
            update_topic: Some("billing_address.updated".to_string()),
            deletion_topic: Some("billing_address.deleted".to_string()),
            error_queue: Some("billing_address.errors".to_string()),
        }
    }
}
