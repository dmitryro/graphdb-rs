use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Department vertex type.
///
/// Description: Represents an organizational unit or section within a Hospital,
/// such as Cardiology, Emergency Room, or Billing.
pub struct Department;

impl Department {
    /// Provides a list of organizational statuses for a department.
    fn status_values() -> Vec<String> {
        vec![
            "Active".to_string(),       // Operational and fully functioning
            "Under_Review".to_string(), // Operational, but undergoing organizational review
            "Restructuring".to_string(),// Actively being restructured (reduced services)
            "Inactive".to_string(),     // Not currently operating (e.g., closed)
        ]
    }

    /// Provides common high-level types for departments.
    fn type_values() -> Vec<String> {
        vec![
            "Clinical".to_string(),     // e.g., Surgery, Pediatrics
            "Ancillary".to_string(),    // e.g., Radiology, Laboratory
            "Administrative".to_string(),// e.g., HR, Finance
            "Support".to_string(),      // e.g., IT, Facilities
            "Emergency".to_string(),    // e.g., Emergency Department (ED)
        ]
    }
}

impl VertexSchema for Department {
    fn schema_name() -> &'static str {
        "Department"
    }

    /// Returns the list of property constraints for the Department vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("hospital_id", true)
                .with_description("ID of the parent Hospital vertex this department belongs to. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("name", true)
                .with_description("The official name of the department (e.g., 'Pediatric Cardiology'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::MinLength(3)]),

            PropertyConstraint::new("department_type", true)
                .with_description("The high-level category of the department (e.g., Clinical, Administrative). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Department::type_values()))
                .with_default_value(JsonValue::String("Clinical".to_string())),

            PropertyConstraint::new("head_of_department_user_id", false)
                .with_description("Optional ID of the User who manages the department.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),

            PropertyConstraint::new("phone", false)
                .with_description("Department's direct phone contact number.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            PropertyConstraint::new("description", false)
                .with_description("Detailed description of the department's function and services.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the department record was created.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp when the department record was last updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
            
            // --- Status Field ---
            PropertyConstraint::new("status", true)
                .with_description("The current operational status of the department.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Department::status_values()))
                .with_default_value(JsonValue::String("Active".to_string())),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to manage operational state changes.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Active".to_string()),
                transitions: vec![
                    // 1. Review: Active -> Under_Review
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Under_Review".to_string(),
                        required_rules: vec!["require_management_approval".to_string()],
                        triggers_events: vec!["department.review_initiated".to_string()],
                    },
                    // 2. Restructuring: Under_Review -> Restructuring
                    StateTransition {
                        from_state: "Under_Review".to_string(),
                        to_state: "Restructuring".to_string(),
                        required_rules: vec!["require_governance_signoff".to_string()],
                        triggers_events: vec!["department.restructuring_began".to_string()],
                    },
                    // 3. Deactivation: Restructuring/Active -> Inactive
                    StateTransition {
                        from_state: "Restructuring".to_string(),
                        to_state: "Inactive".to_string(),
                        required_rules: vec!["require_resource_transfer".to_string()],
                        triggers_events: vec!["department.closed".to_string()],
                    },
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Inactive".to_string(),
                        required_rules: vec!["require_resource_transfer".to_string()],
                        triggers_events: vec!["department.closed".to_string()],
                    },
                    // 4. Re-Activation: Inactive -> Active
                    StateTransition {
                        from_state: "Inactive".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec!["require_budget_allocation".to_string()],
                        triggers_events: vec!["department.reopened".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for organizational classification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "Organizational_Type".to_string(),
                ontology_system_id: "HOSPITAL_ORG".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Internal classification system for the type of department (e.g., inpatient, outpatient, administrative).".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Department CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("department.created".to_string()),
            update_topic: Some("department.updated".to_string()),
            deletion_topic: Some("department.deleted".to_string()),
            error_queue: Some("department.errors".to_string()),
        }
    }
}
