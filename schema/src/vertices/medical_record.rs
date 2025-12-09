use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition, SchemaAction,
};
use crate::rules::{SchemaRule};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the MedicalRecord vertex type.
///
/// Description: Represents a single, auditable clinical document or entry linked to a patient
/// and a certifying doctor. Data governance requires a strict lifecycle for clinical records.
pub struct MedicalRecord;

impl VertexSchema for MedicalRecord {
    fn schema_name() -> &'static str {
        "MedicalRecord"
    }

    /// Returns the property constraints for the MedicalRecord vertex.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the patient this record belongs to. Immutable (critical for data linkage).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("doctor_id", true)
                .with_description("Reference ID to the doctor responsible for creating/signing the record. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("record_type", false)
                .with_description("The type of clinical record (e.g., 'ProgressNote', 'DischargeSummary', 'LabResult').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("record_data", false)
                .with_description("The primary content of the record, typically stored as structured text (JSON, FHIR Bundle, or similar).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("created_at", true)
                .with_description("The UTC timestamp when the record was initially created. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("The UTC timestamp of the last modification. Automatically updated by the system.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            // Status is used for the lifecycle management
            PropertyConstraint::new("status", true)
                .with_description("The official clinical status of the medical record.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Draft".to_string(), "Signed".to_string(), "Amended".to_string()
                ]))
                .with_default_value(JsonValue::String("Draft".to_string()))
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines a rigorous lifecycle for medical records based on clinical documentation standards.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Draft".to_string()),
                transitions: vec![
                    // 1. Draft -> Signed (Requires a digital signature, making it a legal record)
                    StateTransition {
                        from_state: "Draft".to_string(),
                        to_state: "Signed".to_string(),
                        required_rules: vec!["require_digital_signature".to_string(), "lock_record_data_fields".to_string()],
                        triggers_events: vec!["medical_record.signed".to_string()],
                    },
                    // 2. Signed -> Amended (Only permissible transition from Signed, requires justification)
                    StateTransition {
                        from_state: "Signed".to_string(),
                        to_state: "Amended".to_string(),
                        required_rules: vec!["require_amendment_reason".to_string(), "require_practitioner_id".to_string()],
                        triggers_events: vec!["medical_record.amendment_started".to_string()],
                    },
                    // 3. Amended -> Signed (Re-signing of the amended record)
                    StateTransition {
                        from_state: "Amended".to_string(),
                        to_state: "Signed".to_string(),
                        required_rules: vec!["require_digital_signature".to_string()],
                        triggers_events: vec!["medical_record.resigned".to_string()],
                    },
                ],
                // Global checks before any status change
                pre_action_checks: vec![
                    SchemaRule::new("check_patient_access_rights"),
                    SchemaRule::new("audit_trail_entry_required"),
                ],
                // Post-action ensures immutability of signed data
                post_action_actions: vec![
                    // FIX: Using GraphMutation, where the mutation_type is the intended internal action name.
                    SchemaAction::GraphMutation {
                        mutation_type: "update_updated_at".to_string(),
                        target_schema: "MedicalRecord".to_string(),
                    },
                ],
            }
        ]
    }

    /// References to standard terminologies used for medical records (e.g., FHIR, HL7).
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "HL7DocumentType".to_string(),
                ontology_system_id: "HL7v2".to_string(),
                uri: Some("http://www.hl7.org/".to_string()),
                reference_uri: None,
                description: Some("References for the clinical document type codes.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics, critical for audit logs and system notifications.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("clinical.record_created".to_string()),
            update_topic: Some("clinical.record_updated".to_string()),
            deletion_topic: None, // Records are never deleted, only marked as obsolete if error
            error_queue: Some("clinical.record_processing_errors".to_string()),
        }
    }
}