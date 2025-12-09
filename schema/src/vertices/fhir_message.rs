use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the FHIRMessage vertex type.
///
/// Description: Represents a structured message, typically containing a FHIR resource or Bundle,
/// used for integration and data exchange. The status tracks its processing state.
pub struct FHIRMessage;

impl FHIRMessage {
    /// Provides the possible processing status values for a message.
    fn status_values() -> Vec<String> {
        vec![
            "Pending".to_string(),      // Message received but processing hasn't started
            "Processing".to_string(),   // Currently being parsed or integrated
            "Processed".to_string(),    // Successfully integrated into the database
            "Failed".to_string(),       // Processing failed due to validation or error
            "Sent".to_string(),         // Message successfully sent externally (for outgoing messages)
        ]
    }

    /// Provides common FHIR resource/bundle types stored in the message.
    fn message_type_values() -> Vec<String> {
        vec![
            "Bundle".to_string(),
            "Patient".to_string(),
            "Observation".to_string(),
            "Encounter".to_string(),
            "ServiceRequest".to_string(),
            "DiagnosticReport".to_string(),
            "MedicationRequest".to_string(),
        ]
    }
}

impl VertexSchema for FHIRMessage {
    fn schema_name() -> &'static str {
        "FHIRMessage"
    }

    /// Returns the list of property constraints for the FHIRMessage vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Content Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("message_type", true)
                .with_description("The top-level FHIR resource type or 'Bundle'. Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(FHIRMessage::message_type_values()))
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("message_content", true)
                .with_description("The raw FHIR content (JSON/XML). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]), // Content is immutable once recorded

            // --- Date Fields ---
            PropertyConstraint::new("received_date", true)
                .with_description("Timestamp when the message was received by the system. Required, Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("sent_date", false)
                .with_description("Timestamp when the message was sent (for outgoing messages).")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![]),

            // --- Status/Lifecycle Field ---
            PropertyConstraint::new("status", true)
                .with_description("The current processing status of the message.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(FHIRMessage::status_values()))
                .with_default_value(JsonValue::String("Pending".to_string()))
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Lifecycle manages the processing status flow of the FHIR message.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Pending".to_string()),
                transitions: vec![
                    // 1. Start Processing
                    StateTransition {
                        from_state: "Pending".to_string(),
                        to_state: "Processing".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["fhir.processing_started".to_string()],
                    },
                    // 2. Successful Completion
                    StateTransition {
                        from_state: "Processing".to_string(),
                        to_state: "Processed".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["fhir.processing_successful".to_string()],
                    },
                    // 3. Failure
                    StateTransition {
                        from_state: "Processing".to_string(),
                        to_state: "Failed".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["fhir.processing_failed".to_string()],
                    },
                    // 4. Marking as Sent (for outgoing messages)
                    StateTransition {
                        from_state: "*".to_string(), // Can be marked 'Sent' from any state (usually Pending or Processed)
                        to_state: "Sent".to_string(),
                        // Requires 'sent_date' to be set when transitioning to 'Sent'
                        required_rules: vec!["require_sent_date".to_string()],
                        triggers_events: vec!["fhir.message_sent".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for medical information and messaging.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "FHIR_ResourceTypes".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: Some("http://hl7.org/fhir/R4/resourcelist.html".to_string()),
                reference_uri: None,
                description: Some("The official list of Fast Healthcare Interoperability Resources (FHIR) types.".to_string()),
            },
            OntologyReference {
                name: "MessagingEvent".to_string(),
                ontology_system_id: "Custom_Messaging".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Internal ontology defining message handling events and statuses.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for FHIRMessage CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("fhir_message.created".to_string()),
            update_topic: Some("fhir_message.status_updated".to_string()), // Status updates are the main use case
            deletion_topic: Some("fhir_message.deleted".to_string()),
            error_queue: Some("fhir_message.errors".to_string()),
        }
    }
}
