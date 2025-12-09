use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the HL7Message vertex type.
///
/// Description: Represents a legacy HL7 v2 message used for clinical and administrative data
/// exchange (e.g., ADT, ORM, ORU). The status tracks its processing state in the integration engine.
pub struct HL7Message;

impl HL7Message {
    /// Provides the possible processing status values for a message.
    fn status_values() -> Vec<String> {
        vec![
            "Received".to_string(),     // Message received and acknowledged
            "Validation_Error".to_string(), // Message failed basic HL7 structure validation
            "Parsing".to_string(),      // Currently being parsed into internal format
            "Integrated".to_string(),   // Data successfully mapped and saved
            "Rejected".to_string(),     // Business logic rejection after parsing
            "Sent".to_string(),         // Message successfully sent externally
        ]
    }

    /// Provides common HL7 v2 message trigger events/types (Message Type and Event Type).
    fn message_type_values() -> Vec<String> {
        vec![
            "ADT^A01".to_string(), // Admit/Visit Notification
            "ADT^A04".to_string(), // Register Patient
            "ORM^O01".to_string(), // Order Message
            "ORU^R01".to_string(), // Observation Result
            "SIU^S12".to_string(), // Schedule Information Unsolicited
            "MDM^T02".to_string(), // Document/Content Update
            "ACK".to_string(),     // General Acknowledgment
        ]
    }
}

impl VertexSchema for HL7Message {
    fn schema_name() -> &'static str {
        "HL7Message"
    }

    /// Returns the list of property constraints for the HL7Message vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Content Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("message_type", true)
                .with_description("The HL7 Message Type and Event Type (e.g., ADT^A04). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(HL7Message::message_type_values()))
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("message_content", true)
                .with_description("The raw HL7 v2 content (pipe-delimited string). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Date Fields ---
            PropertyConstraint::new("received_date", true)
                .with_description("Timestamp when the message was received by the integration engine. Required, Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("sent_date", false)
                .with_description("Timestamp when the message was sent (for outgoing messages or for tracking original sender timestamp).")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![]),

            // --- Status/Lifecycle Field ---
            PropertyConstraint::new("status", true)
                .with_description("The current processing status of the HL7 message.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(HL7Message::status_values()))
                .with_default_value(JsonValue::String("Received".to_string()))
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Lifecycle manages the processing status flow of the HL7 message.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Received".to_string()),
                transitions: vec![
                    // 1. Validation to Parsing
                    StateTransition {
                        from_state: "Received".to_string(),
                        to_state: "Parsing".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hl7.parsing_started".to_string()],
                    },
                    // 2. Successful Completion
                    StateTransition {
                        from_state: "Parsing".to_string(),
                        to_state: "Integrated".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hl7.data_integrated".to_string()],
                    },
                    // 3. Failure (Validation or Business Logic)
                    StateTransition {
                        from_state: "Received".to_string(),
                        to_state: "Validation_Error".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hl7.validation_failed".to_string()],
                    },
                    StateTransition {
                        from_state: "Parsing".to_string(),
                        to_state: "Rejected".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["hl7.business_rejected".to_string()],
                    },
                    // 4. Marking as Sent (for outgoing messages)
                    StateTransition {
                        from_state: "Integrated".to_string(), // Typically sent after successful integration
                        to_state: "Sent".to_string(),
                        required_rules: vec!["require_sent_date".to_string()],
                        triggers_events: vec!["hl7.message_sent".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for HL7 v2 messaging.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "HL7v2_MessageTypes".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: Some("http://www.hl7.org/implement/standards/product_table.cfm?ref=v2&version=v2.5".to_string()),
                reference_uri: None,
                description: Some("The official list of HL7 v2 Message Types and Trigger Events.".to_string()),
            },
            OntologyReference {
                name: "HL7v2_Segments".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: Some("http://www.hl7.org/fhir/v2/v2msg.html".to_string()),
                reference_uri: None,
                description: Some("HL7 v2 message structure and segment definitions.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for HL7Message CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("hl7_message.received".to_string()),
            update_topic: Some("hl7_message.status_changed".to_string()),
            deletion_topic: Some("hl7_message.archived".to_string()),
            error_queue: Some("hl7_message.processing_errors".to_string()),
        }
    }
}
