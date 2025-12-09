use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the PharmacyIntegration vertex type.
///
/// This vertex models a specific transaction or communication instance where a
/// prescription is sent to a pharmacy for fulfillment, tracking its lifecycle
/// from transmission to completion or rejection.
pub struct PharmacyIntegration;

impl VertexSchema for PharmacyIntegration {
    fn schema_name() -> &'static str {
        "PharmacyIntegration"
    }

    /// Returns the list of property constraints for the PharmacyIntegration vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("pharmacy_id", true)
                .with_description("Reference ID to the Pharmacy vertex receiving the request. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("prescription_id", true)
                .with_description("Reference ID to the Prescription vertex being fulfilled. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),
            
            // --- Status and Dates ---
            PropertyConstraint::new("status", true)
                .with_description("The current status of the prescription fulfillment transaction.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "SENT".to_string(),         // Request transmitted to pharmacy system
                    "RECEIVED".to_string(),     // Pharmacy system acknowledged receipt
                    "IN_PROGRESS".to_string(),  // Filling/processing the order
                    "REJECTED".to_string(),     // Pharmacy cannot fulfill (e.g., out of stock, invalid)
                    "FULFILLED".to_string(),    // Order ready for pickup/delivery
                    "CANCELLED".to_string(),    // Order canceled by provider or patient
                ]))
                .with_default_value(JsonValue::String("SENT".to_string())),
            
            PropertyConstraint::new("fulfillment_date", false)
                .with_description("The date and time the prescription was marked as FULFILLED.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("rejection_reason", false)
                .with_description("Textual reason if the status is REJECTED.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, modeling the transaction flow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("SENT".to_string()),
                transitions: vec![
                    // 1. Transaction Acknowledgment
                    StateTransition {
                        from_state: "SENT".to_string(),
                        to_state: "RECEIVED".to_string(),
                        required_rules: vec!["require_pharmacy_ack".to_string()],
                        triggers_events: vec!["integration.ack_received".to_string()],
                    },
                    // 2. Fulfillment Path
                    StateTransition {
                        from_state: "RECEIVED".to_string(),
                        to_state: "FULFILLED".to_string(),
                        required_rules: vec!["require_fulfillment_date".to_string()],
                        triggers_events: vec!["integration.fulfilled".to_string()],
                    },
                    // 3. Rejection Path (Can happen from SENT or RECEIVED)
                    StateTransition {
                        from_state: "*".to_string(), // SENT, RECEIVED, IN_PROGRESS
                        to_state: "REJECTED".to_string(),
                        required_rules: vec!["require_rejection_reason".to_string()],
                        triggers_events: vec!["integration.rejected".to_string()],
                    },
                    // 4. Cancellation Path (Prior to fulfillment)
                    StateTransition {
                        from_state: "*".to_string(), // SENT, RECEIVED, IN_PROGRESS
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["integration.cancelled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies related to prescription and claim processing.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "NCPDP_Surescripts_Flows".to_string(),
                ontology_system_id: "NCPDP".to_string(),
                uri: Some("https://www.ncpdp.org/".to_string()),
                reference_uri: None,
                description: Some("Standards related to electronic prescription transmission and status tracking.".to_string()),
            },
            OntologyReference {
                name: "FHIR_Task_Mapping".to_string(),
                ontology_system_id: "FHIR".to_string(),
                uri: Some("http://hl7.org/fhir/R4/task.html".to_string()),
                reference_uri: None,
                description: Some("Maps the fulfillment transaction to a FHIR Task resource.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for PharmacyIntegration lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("pharmacy_integration.sent".to_string()),
            update_topic: Some("pharmacy_integration.status_change".to_string()),
            deletion_topic: None, // Integrations should generally be voided, not deleted
            error_queue: Some("pharmacy_integration.transaction_errors".to_string()),
        }
    }
}
