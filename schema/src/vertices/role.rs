use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Role vertex type.
///
/// This vertex defines the authorization roles (e.g., Patient, Doctor, Admin)
/// and their associated permissions within the system.
pub struct Role;

impl VertexSchema for Role {
    fn schema_name() -> &'static str {
        "Role"
    }

    /// Returns the list of property constraints for the Role vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (u32). Required, Unique, and Immutable.")
                .with_data_type(DataType::UnsignedInteger)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("name", true)
                .with_description("The unique, human-readable name of the role (e.g., 'Physician', 'Nurse', 'Admin').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::MinLength(3)]),

            PropertyConstraint::new("permissions", true)
                .with_description("A list of capabilities or access rights granted by this role (e.g., 'read_patient_record', 'write_prescription').")
                .with_data_type(DataType::List(Box::new(DataType::String)))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]), // Permissions can change

            // --- Status and Time ---
            PropertyConstraint::new("status", true)
                .with_description("The current operational status of the role.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "DRAFT".to_string(),    // Role is being defined/reviewed
                    "ACTIVE".to_string(),   // Role is operational and assignable
                    "RETIRED".to_string(),  // Role is deprecated and cannot be assigned to new users
                ]))
                .with_default_value(JsonValue::String("DRAFT".to_string())),

            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the role definition was created.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, managing the deployment and retirement of roles.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("DRAFT".to_string()),
                transitions: vec![
                    // 1. DRAFT -> ACTIVE: Role is approved and ready for assignment.
                    StateTransition {
                        from_state: "DRAFT".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_admin_approval".to_string()],
                        triggers_events: vec!["role.activated".to_string()],
                    },
                    // 2. ACTIVE -> RETIRED: Role is deprecated.
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "RETIRED".to_string(),
                        required_rules: vec!["ensure_no_active_assignments".to_string()],
                        triggers_events: vec!["role.retired".to_string()],
                    },
                    // 3. RETIRED -> ACTIVE: Reactivation (less common, but possible)
                    StateTransition {
                        from_state: "RETIRED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_admin_approval".to_string()],
                        triggers_events: vec!["role.reactivated".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references, linking this internal system concept to external standards.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "RBAC_Model".to_string(),
                ontology_system_id: "Access_Control".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("References the general model of Role-Based Access Control (RBAC) upon which this role structure is based.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Role lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("role.definition_created".to_string()),
            update_topic: Some("role.permissions_updated".to_string()),
            deletion_topic: None, // Roles are typically kept for audit
            error_queue: Some("role.management_errors".to_string()),
        }
    }
}
