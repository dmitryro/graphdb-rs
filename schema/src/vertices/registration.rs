use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Registration vertex type.
///
/// This vertex models the initial stage of user signup, capturing the user-provided
/// data before it is fully verified and potentially split into separate user/patient/doctor
/// vertices. It manages the verification and activation process.
pub struct Registration;

impl VertexSchema for Registration {
    fn schema_name() -> &'static str {
        "Registration"
    }

    /// Returns the list of property constraints for the Registration vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (u32). Required, Unique, and Immutable.")
                .with_data_type(DataType::UnsignedInteger)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("first", true)
                .with_description("First name provided during registration.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("last", true)
                .with_description("Last name provided during registration.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            // --- Authentication/Contact Details (Sensitive) ---
            PropertyConstraint::new("username", true)
                .with_description("Unique login username. Required, Unique.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("email", true)
                .with_description("Email address for verification and contact. Required, Unique.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::ValidateEmail]),

            PropertyConstraint::new("password", true)
                .with_description("Hashed password (MUST be stored securely).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Sensitive, Constraint::Immutable]),

            PropertyConstraint::new("phone", false)
                .with_description("Optional phone number for contact or multi-factor authentication.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("role_id", true)
                .with_description("The requested user role (e.g., Patient, Doctor, Administrator). Indexed.")
                .with_data_type(DataType::UnsignedInteger)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            // --- Status and Time ---
            PropertyConstraint::new("status", true)
                .with_description("The current verification status of the registration attempt.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "PENDING_VERIFICATION".to_string(), // Initial state: email not yet confirmed
                    "VERIFIED".to_string(),             // Email/phone confirmed
                    "ACTIVE".to_string(),               // User data successfully provisioned into main User/Patient/Doctor vertices
                    "REJECTED".to_string(),             // Failed verification or blocked registration
                ]))
                .with_default_value(JsonValue::String("PENDING_VERIFICATION".to_string())),
            
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the registration attempt was initiated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, modeling the verification and activation flow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PENDING_VERIFICATION".to_string()),
                transitions: vec![
                    // 1. User Action: PENDING_VERIFICATION -> VERIFIED
                    StateTransition {
                        from_state: "PENDING_VERIFICATION".to_string(),
                        to_state: "VERIFIED".to_string(),
                        required_rules: vec!["require_verification_token".to_string()],
                        triggers_events: vec!["registration.email_confirmed".to_string()],
                    },
                    // 2. System Action: VERIFIED -> ACTIVE (Provisioning)
                    StateTransition {
                        from_state: "VERIFIED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        // This transition implies the data is successfully mapped/copied to a permanent user vertex
                        required_rules: vec!["require_user_provisioning_success".to_string()],
                        triggers_events: vec!["registration.user_activated".to_string()],
                    },
                    // 3. Rejection/Failure: Any -> REJECTED
                    StateTransition {
                        from_state: "PENDING_VERIFICATION|VERIFIED".to_string(),
                        to_state: "REJECTED".to_string(),
                        required_rules: vec!["require_rejection_reason".to_string()],
                        triggers_events: vec!["registration.rejected".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Since this is an internal application management vertex, there are typically
    /// fewer external ontological references, but we can reference user role standards.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "User_Role_Taxonomy".to_string(),
                ontology_system_id: "Internal_Roles".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Internal system definitions for different user roles (e.g., 1=Patient, 2=Doctor, 3=Admin).".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Registration lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("registration.new_attempt".to_string()),
            update_topic: Some("registration.status_change".to_string()),
            deletion_topic: None, // Keep registration attempts for audit/logging purposes.
            error_queue: Some("registration.verification_errors".to_string()),
        }
    }
}
