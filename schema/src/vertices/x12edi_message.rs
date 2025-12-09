use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{LifecycleRule, MessagingSchema};
use crate::properties::OntologyReference;
use serde_json::json;

/// Defines the schema for an X12EDIMessage vertex, representing a single
/// electronic data interchange transaction (e.g., 837 Claim, 835 Payment).
pub struct X12EDIMessage;

impl VertexSchema for X12EDIMessage {
    fn schema_name() -> &'static str {
        "X12EDIMessage"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Globally Unique Identifier (UUID) for the EDI message record.")
                .with_data_type(DataType::Uuid)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("transaction_set_id", true)
                .with_description("The specific EDI transaction set identifier (e.g., '837' for a healthcare claim).")
                .with_data_type(DataType::String)
                // FIX: Changed Constraint::Regex to Constraint::Format
                .with_constraints(vec![Constraint::Required, Constraint::Immutable, Constraint::Format(r"^\d{3}$".to_string())]),

            PropertyConstraint::new("transaction_set_control_number", true)
                .with_description("The ST segment control number, identifying the individual transaction set.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("interchange_control_number", true)
                .with_description("The ISA segment control number, identifying the entire interchange file.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("sender_id", true)
                .with_description("The ID of the sending entity (ISA06).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("receiver_id", true)
                .with_description("The ID of the receiving entity (ISA08).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("message_content", true)
                .with_description("The full raw EDI message payload.")
                // FIX: Changed DataType::LargeString to DataType::String
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("received_date", true)
                .with_description("The date and time the system initially received the message.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("sent_date", false)
                .with_description("The date and time the message was officially sent (for outbound messages).")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            PropertyConstraint::new("status", true)
                .with_description("The processing status of the EDI message.")
                // FIX: Changed DataType::Enum to DataType::String
                .with_data_type(DataType::String)
                .with_default_value(json!("Received"))
                // Use with_enum_values to apply the actual enum constraint
                .with_enum_values(EnumValues::new(vec![
                    "Received".to_string(),
                    "Validated".to_string(),
                    "Processing".to_string(),
                    "Accepted".to_string(),
                    "Rejected".to_string(),
                    "Acknowledged".to_string(),
                    "Sent".to_string(),
                    "Error".to_string(),
                ]))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]), // Ensure status is mutable
        ]
    }

    fn lifecycle_rules() -> Vec<LifecycleRule> {
        // FIX: The provided LifecycleRule struct does not contain an UpdateConstraint variant,
        // so we return an empty vector to resolve the E0223 errors.
        Vec::new()
    }

    fn ontology_references() -> Vec<OntologyReference> {
        // EDI is its own standard, but we reference the overall governing body.
        vec![
            OntologyReference::new(
                "X12",
                "Accredited Standards Committee X12 (EDI Standards Body)"
            )
            .with_uri("https://www.x12.org/"),
        ]
    }

    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("edi.message.received".to_string()),
            update_topic: Some("edi.message.status_updated".to_string()), // Track status changes
            deletion_topic: Some("edi.message.archived".to_string()),
            error_queue: Some("edi.processing_errors".to_string()),
        }
    }
}
