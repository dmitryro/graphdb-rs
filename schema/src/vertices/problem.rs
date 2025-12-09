use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Problem vertex type.
///
/// This vertex represents a specific medical diagnosis or health concern identified
/// for a patient, including its clinical status, severity, and temporal tracking.
pub struct Problem;

impl VertexSchema for Problem {
    fn schema_name() -> &'static str {
        "Problem"
    }

    /// Returns the list of property constraints for the Problem vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the Patient this problem belongs to. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            // --- Clinical Details ---
            PropertyConstraint::new("problem", true)
                .with_description("Human-readable name of the health problem or diagnosis.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("icd10_code", false)
                .with_description("Standardized ICD-10 code for the problem/diagnosis.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("status", true)
                .with_description("The current clinical state of the problem.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "ACTIVE".to_string(),
                    "RESOLVED".to_string(),
                    "IMPROVING".to_string(),
                    "WORSENING".to_string(),
                    "STABLE".to_string(),
                    "CHRONIC".to_string(),
                ]))
                .with_default_value(JsonValue::String("ACTIVE".to_string())),
            
            PropertyConstraint::new("severity", false)
                .with_description("Clinical severity of the problem.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional])
                .with_enum_values(EnumValues::new(vec![
                    "MILD".to_string(),
                    "MODERATE".to_string(),
                    "SEVERE".to_string(),
                ])),

            PropertyConstraint::new("notes", false)
                .with_description("Clinical notes or additional context regarding the problem.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            // --- Date Tracking ---
            PropertyConstraint::new("onset_date", false)
                .with_description("Date when the problem was first identified or started.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional]),
            
            PropertyConstraint::new("resolved_date", false)
                .with_description("Date when the problem was clinically resolved or cured.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional]),
            
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the record was created.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp when the record was last updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, modeling the clinical progression.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("ACTIVE".to_string()),
                transitions: vec![
                    // 1. Resolution Path
                    StateTransition {
                        from_state: "*".to_string(), // Can be RESOLVED from any active state
                        to_state: "RESOLVED".to_string(),
                        required_rules: vec!["require_resolved_date".to_string()],
                        triggers_events: vec!["problem.resolved".to_string()],
                    },
                    // 2. Progression Tracking (Clinical assessment change)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "IMPROVING".to_string(),
                        required_rules: vec!["require_clinician_update".to_string()],
                        triggers_events: vec!["problem.status_change".to_string()],
                    },
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "WORSENING".to_string(),
                        required_rules: vec!["require_clinician_update".to_string()],
                        triggers_events: vec!["problem.status_change".to_string()],
                    },
                    // 3. Chronic Status (Active -> Chronic, implies long-term management)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "CHRONIC".to_string(),
                        required_rules: vec!["require_clinician_assessment".to_string()],
                        triggers_events: vec!["problem.marked_chronic".to_string()],
                    },
                    // 4. Any clinical status back to general ACTIVE
                    StateTransition {
                        from_state: "IMPROVING|WORSENING|STABLE|CHRONIC".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_clinician_reassessment".to_string()],
                        triggers_events: vec!["problem.status_change".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies related to medical coding.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "ICD-10-CM".to_string(),
                ontology_system_id: "ICD10".to_string(),
                uri: Some("https://www.cdc.gov/nchs/icd/icd10cm.htm".to_string()),
                reference_uri: None,
                description: Some("International Classification of Diseases, 10th Revision, Clinical Modification.".to_string()),
            },
            OntologyReference {
                name: "SNOMED_CT_Problem_List".to_string(),
                ontology_system_id: "SNOMEDCT".to_string(),
                uri: Some("https://www.snomed.org/snomed-ct".to_string()),
                reference_uri: None,
                description: Some("Used for formal representation of clinical findings and diagnoses.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Problem lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("problem.newly_recorded".to_string()),
            update_topic: Some("problem.status_or_detail_updated".to_string()),
            deletion_topic: None, // Problems should only be RESOLVED, not deleted
            error_queue: Some("problem.coding_or_system_errors".to_string()),
        }
    }
}
