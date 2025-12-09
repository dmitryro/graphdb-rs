use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the MasterPatientIndex (MPI) vertex type.
///
/// This schema manages the identity resolution process, storing core demographic data
/// and tracking the matching status to ensure all records correctly link to a canonical
/// patient entity.
pub struct MasterPatientIndex;

impl VertexSchema for MasterPatientIndex {
    fn schema_name() -> &'static str {
        "MasterPatientIndex"
    }

    /// Returns the list of property constraints for the MPI vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32) of the MPI record. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("patient_id", false)
                .with_description("The ID of the canonical Patient vertex this MPI record is linked to (if confirmed matched).")
                .with_data_type(DataType::Integer),

            // --- Demographic Data (Often optional due to varying source systems) ---
            PropertyConstraint::new("first_name", false)
                .with_description("Patient's first name.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("last_name", false)
                .with_description("Patient's last name.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("date_of_birth", false)
                .with_description("Patient's date of birth.")
                .with_data_type(DataType::DateTime),

            PropertyConstraint::new("gender", false)
                .with_description("Patient's gender.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "Male".to_string(),
                    "Female".to_string(),
                    "Other".to_string(),
                    "Unknown".to_string(),
                ])),

            PropertyConstraint::new("address", false)
                .with_description("Patient's primary address information (stored as serialized JSON).")
                .with_data_type(DataType::Json),

            PropertyConstraint::new("contact_number", false)
                .with_description("Patient's primary contact phone number.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("email", false)
                .with_description("Patient's email address.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("ssn", false)
                .with_description("Social Security Number or equivalent national identifier. Stored as 'ssn' in the vertex map.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Encrypted]), // Should be encrypted for sensitive data

            // --- Matching Metadata ---
            PropertyConstraint::new("match_score", false)
                .with_description("The confidence score (0.0 to 1.0) of the last automatic matching attempt.")
                .with_data_type(DataType::Float),

            PropertyConstraint::new("match_date", false)
                .with_description("Timestamp of the last successful match attempt.")
                .with_data_type(DataType::DateTime),

            // --- Audit Timestamps ---
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the MPI record was created.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp when the MPI record was last updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            // --- Lifecycle Status Field ---
            PropertyConstraint::new("match_status", true)
                .with_description("The current identity resolution status of the record.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "UNMATCHED".to_string(),            // Default state
                    "POTENTIAL_MATCH".to_string(),      // High match score found
                    "PENDING_MERGE".to_string(),        // Match reviewed, awaiting linking
                    "MATCHED_CONFIRMED".to_string(),    // Linked to a canonical Patient vertex
                    "OVERRIDE_UNMATCHED".to_string(),   // Manually confirmed as unique
                ]))
                .with_default_value(serde_json::Value::String("UNMATCHED".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the identity matching status.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "match_status".to_string(),
                initial_state: Some("UNMATCHED".to_string()),
                transitions: vec![
                    // 1. Automatic Matcher Finds a strong candidate
                    StateTransition {
                        from_state: "UNMATCHED".to_string(),
                        to_state: "POTENTIAL_MATCH".to_string(),
                        required_rules: vec!["match_score_above_threshold".to_string()],
                        triggers_events: vec!["mpi.potential_match_found".to_string()],
                    },
                    // 2. Manual Reviewer Approves Potential Match
                    StateTransition {
                        from_state: "POTENTIAL_MATCH".to_string(),
                        to_state: "PENDING_MERGE".to_string(),
                        required_rules: vec!["require_reviewer_approval".to_string()],
                        triggers_events: vec!["mpi.ready_for_merge".to_string()],
                    },
                    // 3. System Links to Canonical Patient Record
                    StateTransition {
                        from_state: "PENDING_MERGE".to_string(),
                        to_state: "MATCHED_CONFIRMED".to_string(),
                        required_rules: vec!["require_patient_id_set".to_string(), "lock_core_demographics".to_string()],
                        triggers_events: vec!["mpi.match_confirmed".to_string()],
                    },
                    // 4. Manual Reviewer overrides high score and confirms uniqueness
                    StateTransition {
                        from_state: "POTENTIAL_MATCH".to_string(),
                        to_state: "OVERRIDE_UNMATCHED".to_string(),
                        required_rules: vec!["require_reviewer_justification".to_string()],
                        triggers_events: vec!["mpi.override_unmatched".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// The MPI does not typically reference external clinical ontologies directly but focuses on identity standards.
    fn ontology_references() -> Vec<OntologyReference> {
        // No specific ontology references needed for identity matching, as it relies on
        // internal demographic data consistency and matching algorithms.
        vec![]
    }

    /// Defines the messaging topics for MPI CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("mpi.record_created".to_string()),
            update_topic: Some("mpi.record_updated".to_string()),
            deletion_topic: Some("mpi.record_deleted".to_string()),
            error_queue: Some("mpi.errors".to_string()),
        }
    }
}
