use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the ClinicalNote vertex type.
///
/// This schema defines constraints and a lifecycle for structured medical documentation,
/// ensuring notes are properly signed and managed through revisions.
pub struct ClinicalNote;

impl VertexSchema for ClinicalNote {
    fn schema_name() -> &'static str {
        "ClinicalNote"
    }

    /// Returns the list of property constraints for the ClinicalNote vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient this note pertains to. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("doctor_id", true)
                .with_description("ID of the Doctor who authored or signed the note. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            // --- Note Content and Metadata ---
            PropertyConstraint::new("note_text", true)
                .with_description("The full text content of the clinical note.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::MinLength(10)]),

            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the note was first created.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp when the note was last updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            // --- Lifecycle Status Field ---
            PropertyConstraint::new("status", true)
                .with_description("The current integrity status of the note (DRAFT, SIGNED, AMENDED).")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "DRAFT".to_string(),      // Being written or reviewed
                    "SIGNED".to_string(),     // Finalized and legally signed/authenticated
                    "AMENDED".to_string(),    // Original note modified or an addendum added after signing
                    "WITHDRAWN".to_string(),  // Note removed from the chart (rare)
                ]))
                .with_default_value(serde_json::Value::String("DRAFT".to_string())),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to enforce clinical documentation integrity.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("DRAFT".to_string()),
                transitions: vec![
                    // 1. Signature: DRAFT -> SIGNED (Requires authentication)
                    StateTransition {
                        from_state: "DRAFT".to_string(),
                        to_state: "SIGNED".to_string(),
                        required_rules: vec!["require_author_signature".to_string(), "lock_note_text".to_string()],
                        triggers_events: vec!["note.signed".to_string()],
                    },
                    // 2. Amendment/Addendum: SIGNED -> AMENDED (Original content is preserved, new content is tracked)
                    StateTransition {
                        from_state: "SIGNED".to_string(),
                        to_state: "AMENDED".to_string(),
                        required_rules: vec!["require_addendum_note".to_string()],
                        triggers_events: vec!["note.amended".to_string()],
                    },
                    // 3. Withdrawal: SIGNED -> WITHDRAWN (Requires high privilege and reason)
                    StateTransition {
                        from_state: "SIGNED".to_string(),
                        to_state: "WITHDRAWN".to_string(),
                        required_rules: vec!["require_withdrawal_reason".to_string(), "require_admin_privilege".to_string()],
                        triggers_events: vec!["note.withdrawn".to_string()],
                    },
                    // 4. Draft to Canceled: DRAFT -> WITHDRAWN (If the note is abandoned)
                    StateTransition {
                        from_state: "DRAFT".to_string(),
                        to_state: "WITHDRAWN".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["note.abandoned".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for note formatting (e.g., SOAP).
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "Note_Type_SOAP".to_string(),
                ontology_system_id: "Internal".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Commonly used Subjective, Objective, Assessment, and Plan note format.".to_string()),
            },
            OntologyReference {
                name: "Note_Type_H&P".to_string(),
                ontology_system_id: "Internal".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("History and Physical note format used for initial assessments.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for ClinicalNote CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("clinical_note.created".to_string()),
            update_topic: Some("clinical_note.updated".to_string()),
            deletion_topic: Some("clinical_note.deleted".to_string()),
            error_queue: Some("clinical_note.errors".to_string()),
        }
    }
}
