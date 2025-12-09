use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the User vertex type.
///
/// This vertex represents any individual (employee, patient, administrator) who interacts
/// with the system, storing core identification and authentication status.
pub struct User;

impl VertexSchema for User {
    fn schema_name() -> &'static str {
        "User"
    }

    /// Returns the list of property constraints for the User vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identification ---
            PropertyConstraint::new("id", true)
                .with_description("Globally Unique Identifier (UUID) for the user.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("first", true)
                .with_description("User's first name.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("last", true)
                .with_description("User's last name.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("username", true)
                .with_description("Unique login identifier for the user.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("email", true)
                .with_description("Primary contact email address.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Mutable, Constraint::Format("email".to_string())]),

            // The password_hash property is necessary for the schema but should be secured
            // via database-level access control, preventing normal graph traversal access.
            PropertyConstraint::new("password_hash", true)
                .with_description("Bcrypt hash of the user's password.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Sensitive, Constraint::WriteOnly]),

            PropertyConstraint::new("role_id", true)
                .with_description("Reference ID to the Role vertex defining the user's permissions.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("phone", false)
                .with_description("User's primary phone number.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            // --- Audit Timestamps ---
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the user account was created.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp of the last successful profile update.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("last_login", false)
                .with_description("Timestamp of the user's last successful authentication.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            // --- Account Status ---
            PropertyConstraint::new("status", true)
                .with_description("The operational status of the user account.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "PENDING_ACTIVATION".to_string(), // Account created, awaiting email verification/admin approval
                    "ACTIVE".to_string(),             // Standard operational state
                    "SUSPENDED".to_string(),          // Temporarily disabled (e.g., policy violation)
                    "LOCKED".to_string(),             // Disabled due to failed login attempts
                    "DELETED".to_string(),            // Logically deleted, retained for auditing
                ]))
                .with_default_value(JsonValue::String("PENDING_ACTIVATION".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, managing account access.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PENDING_ACTIVATION".to_string()),
                transitions: vec![
                    // 1. Activation: PENDING_ACTIVATION -> ACTIVE
                    StateTransition {
                        from_state: "PENDING_ACTIVATION".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_email_verification".to_string()],
                        triggers_events: vec!["user.account_activated".to_string()],
                    },
                    // 2. Security Lock: ACTIVE/SUSPENDED -> LOCKED
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "LOCKED".to_string(),
                        required_rules: vec!["check_failed_login_attempts_limit".to_string()],
                        triggers_events: vec!["user.account_locked".to_string()],
                    },
                    // 3. Admin Suspension: ACTIVE -> SUSPENDED
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "SUSPENDED".to_string(),
                        required_rules: vec!["require_admin_approval_and_reason".to_string()],
                        triggers_events: vec!["user.account_suspended".to_string()],
                    },
                    // 4. Reactivation: LOCKED/SUSPENDED -> ACTIVE
                    StateTransition {
                        from_state: "LOCKED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_password_reset".to_string()],
                        triggers_events: vec!["user.account_reactivated".to_string()],
                    },
                    StateTransition {
                        from_state: "SUSPENDED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_admin_reinstatement".to_string()],
                        triggers_events: vec!["user.account_reactivated".to_string()],
                    },
                    // 5. Deletion: Any state -> DELETED
                    StateTransition {
                        from_state: "*".to_string(),
                        to_state: "DELETED".to_string(),
                        required_rules: vec!["require_audit_flag_set".to_string()],
                        triggers_events: vec!["user.account_deleted_logical".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references for User, primarily linking to standards for roles/permissions.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "FHIR_Practitioner_Role".to_string(),
                ontology_system_id: "FHIR_R4_PractitionerRole".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Links the user's role_id to standard healthcare roles defined by FHIR.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for User events, essential for security and system auditing.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("auth.new_user_registered".to_string()),
            update_topic: Some("auth.user_profile_updated".to_string()),
            deletion_topic: Some("auth.user_archived".to_string()),
            error_queue: Some("auth.security_alerts".to_string()),
        }
    }
}
