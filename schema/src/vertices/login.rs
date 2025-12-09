use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Login vertex type.
///
/// Description: Represents an immutable event log for a user's authentication event
/// (e.g., successful or failed login).
pub struct Login;

impl VertexSchema for Login {
    fn schema_name() -> &'static str {
        "Login"
    }

    /// Returns the list of property constraints for the Login vertex type.
    /// All properties are immutable as this records a historical event.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (u32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            // --- Event Details ---
            PropertyConstraint::new("username", true)
                .with_description("The username used for the login attempt. Immutable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // NOTE: In a real system, storing the plaintext password is a severe security risk. 
            // This property follows the provided model but would typically store a derived hash or result flag.
            PropertyConstraint::new("password", true) 
                .with_description("The password (or derived hash) used in the attempt. Immutable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("login_time", true)
                .with_description("The exact UTC timestamp of the login event. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            // Simple status to denote the result of the attempt
            PropertyConstraint::new("status", true)
                .with_description("Result of the login attempt (Success, Failure).")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec!["Success".to_string(), "Failure".to_string()]))
                .with_default_value(JsonValue::String("Success".to_string())) 
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
        ]
    }

    /// A login event is terminal (it represents a moment in time) and immutable.
    /// The lifecycle enforces that the record cannot be changed after creation.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Success".to_string()),
                // No transitions are possible for an immutable event log.
                transitions: vec![],
                // Enforce that this vertex can only be created and not updated later.
                pre_action_checks: vec![
                    SchemaRule::new("is_new_creation_only"),
                    SchemaRule::new("immutable_after_creation"),
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to security or audit ontologies for standardized event classification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "Authentication_Event_Type".to_string(),
                ontology_system_id: "InternalSecurityAudit".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Internal classification for authentication and access events.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Login events, primarily for audit streams.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("security.user_logged_in".to_string()),
            update_topic: None, // Events are immutable
            deletion_topic: None, // Events should not be deleted
            error_queue: Some("security.auth_audit_alerts".to_string()),
        }
    }
}
