use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Refill vertex type.
///
/// This vertex models a single request for a prescription refill, tracking its
/// relationship to the original prescription, the request date, and the outcome.
pub struct Refill;

impl VertexSchema for Refill {
    fn schema_name() -> &'static str {
        "Refill"
    }

    /// Returns the list of property constraints for the Refill vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("prescription_id", true)
                .with_description("Reference ID to the original Prescription vertex this refill relates to. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            // --- Status and Time ---
            PropertyConstraint::new("date_requested", true)
                .with_description("The date and time the patient or pharmacy requested the refill.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("date_fulfilled", false)
                .with_description("The date and time the refill order was dispensed or authorized by the pharmacy/provider.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("status", true)
                .with_description("The current processing status of the refill request.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "REQUESTED".to_string(), // Initial state, pending review
                    "AUTHORIZED".to_string(), // Provider approved the request
                    "DENIED".to_string(),    // Provider rejected the request (requires reason)
                    "FULFILLED".to_string(), // Pharmacy dispensed the medication
                    "CANCELLED".to_string(), // Request was withdrawn
                ]))
                .with_default_value(JsonValue::String("REQUESTED".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, modeling the refill workflow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("REQUESTED".to_string()),
                transitions: vec![
                    // 1. Provider Action: REQUESTED -> AUTHORIZED
                    StateTransition {
                        from_state: "REQUESTED".to_string(),
                        to_state: "AUTHORIZED".to_string(),
                        required_rules: vec!["check_refill_count_remaining".to_string()],
                        triggers_events: vec!["refill.authorized".to_string()],
                    },
                    // 2. Provider Action: REQUESTED -> DENIED
                    StateTransition {
                        from_state: "REQUESTED".to_string(),
                        to_state: "DENIED".to_string(),
                        required_rules: vec!["require_denial_reason".to_string()],
                        triggers_events: vec!["refill.denied".to_string()],
                    },
                    // 3. Pharmacy Action: AUTHORIZED -> FULFILLED
                    StateTransition {
                        from_state: "AUTHORIZED".to_string(),
                        to_state: "FULFILLED".to_string(),
                        required_rules: vec!["require_date_fulfilled".to_string()],
                        triggers_events: vec!["refill.fulfilled".to_string()],
                    },
                    // 4. Withdrawal: REQUESTED -> CANCELLED
                    StateTransition {
                        from_state: "REQUESTED".to_string(),
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["refill.cancelled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies related to medications and dispensing.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "RxNorm".to_string(),
                ontology_system_id: "RXNORM".to_string(),
                uri: Some("https://www.nlm.nih.gov/research/umls/rxnorm/index.html".to_string()),
                reference_uri: None,
                description: Some("Provides normalized names for clinical drugs, used for tracking drug components in the Prescription vertex.".to_string()),
            },
            OntologyReference {
                name: "NDC".to_string(),
                ontology_system_id: "NDC".to_string(),
                uri: Some("https://www.fda.gov/drugs/drug-listing/national-drug-code-ndc".to_string()),
                reference_uri: None,
                description: Some("National Drug Code, used to identify the specific commercial package dispensed upon fulfillment.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Refill lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("refill.requested".to_string()),
            update_topic: Some("refill.status_update".to_string()),
            deletion_topic: None, // Refill requests should be historically preserved.
            error_queue: Some("refill.processing_issues".to_string()),
        }
    }
}
