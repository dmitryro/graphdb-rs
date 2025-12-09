use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the StaffAssignment vertex type.
///
/// This vertex captures the relationship between a medical User and their specific role
/// within a Hospital, Department, or FacilityUnit over a defined period.
pub struct StaffAssignment;

impl VertexSchema for StaffAssignment {
    fn schema_name() -> &'static str {
        "StaffAssignment"
    }

    /// Returns the list of property constraints for the StaffAssignment vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32) for the assignment record.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("user_id", true)
                .with_description("The ID of the User vertex (staff member) being assigned.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("hospital_id", true)
                .with_description("The ID of the Hospital vertex where the assignment is valid.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("department_id", false)
                .with_description("The ID of the Department vertex (e.g., Cardiology, ER). Optional.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Optional, Constraint::Immutable]),

            PropertyConstraint::new("facility_unit_id", false)
                .with_description("The ID of the specific FacilityUnit (e.g., ICU, Ward B). Optional.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Optional, Constraint::Immutable]),

            PropertyConstraint::new("assigned_role_id", true)
                .with_description("The ID of the Role vertex defining the user's function (e.g., Attending Physician, Charge Nurse).")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Assignment Tenure ---
            PropertyConstraint::new("start_date", true)
                .with_description("The date and time the assignment began.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("end_date", false)
                .with_description("The date and time the assignment ended (null if current/active).")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            PropertyConstraint::new("is_active", true)
                .with_description("Boolean flag indicating if the assignment is currently active.")
                .with_data_type(DataType::Boolean)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable])
                .with_default_value(JsonValue::Bool(true)),

            // --- Status and Audit ---
            PropertyConstraint::new("status", true)
                .with_description("The administrative status of the assignment.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "PROVISIONAL".to_string(), // Pending approval or start
                    "ACTIVE".to_string(),      // Currently in effect
                    "SUSPENDED".to_string(),   // Temporarily inactive (e.g., leave of absence)
                    "COMPLETED".to_string(),   // Ended naturally (end_date is set)
                    "TERMINATED".to_string(),  // Ended prematurely
                ]))
                .with_default_value(JsonValue::String("PROVISIONAL".to_string())),

            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp of assignment creation.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp of last update to the assignment record.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, governing HR workflow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PROVISIONAL".to_string()),
                transitions: vec![
                    // 1. PROVISIONAL -> ACTIVE: Assignment is approved and starts
                    StateTransition {
                        from_state: "PROVISIONAL".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_hr_approval".to_string()],
                        triggers_events: vec!["staff.assignment_activated".to_string()],
                    },
                    // 2. ACTIVE -> SUSPENDED: Temporary leave/inactivity
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "SUSPENDED".to_string(),
                        required_rules: vec!["require_reason_code".to_string()],
                        triggers_events: vec!["staff.assignment_suspended".to_string()],
                    },
                    // 3. SUSPENDED -> ACTIVE: Return from leave
                    StateTransition {
                        from_state: "SUSPENDED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["check_fitness_for_duty".to_string()],
                        triggers_events: vec!["staff.assignment_resumed".to_string()],
                    },
                    // 4. ACTIVE -> COMPLETED: Assignment reaches its natural end date
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "COMPLETED".to_string(),
                        required_rules: vec!["check_end_date_passed".to_string()],
                        triggers_events: vec!["staff.assignment_completed".to_string()],
                    },
                    // 5. ACTIVE/PROVISIONAL/SUSPENDED -> TERMINATED: Premature end
                    StateTransition {
                        from_state: "*".to_string(), // From any state
                        to_state: "TERMINATED".to_string(),
                        required_rules: vec!["require_termination_protocol".to_string()],
                        triggers_events: vec!["staff.assignment_terminated".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references, though less critical for internal assignments,
    /// they can link roles to external standards.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "O*NET_Roles".to_string(),
                ontology_system_id: "O*NET".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("References O*NET codes for standardized job classification and roles.".to_string()),
            },
            OntologyReference {
                name: "HR_Code_Standard".to_string(),
                ontology_system_id: "Internal_HR_System".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Links to the internal Human Resources system's role and job codes.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for StaffAssignment events, critical for access control.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("staff.new_assignment".to_string()),
            update_topic: Some("staff.assignment_status_change".to_string()),
            deletion_topic: Some("staff.assignment_archived".to_string()), // Should be archived, not deleted
            error_queue: Some("staff.assignment_sync_errors".to_string()),
        }
    }
}
