use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Triage vertex type.
///
/// This vertex records the initial severity assessment and primary complaints for a patient
/// at the beginning of an Encounter (typically in the Emergency Department or Urgent Care).
pub struct Triage;

impl VertexSchema for Triage {
    fn schema_name() -> &'static str {
        "Triage"
    }

    /// Returns the list of property constraints for the Triage vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32) for the triage record.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("encounter_id", true)
                .with_description("The ID of the Encounter vertex this triage assessment belongs to.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("The ID of the Patient vertex.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("triage_nurse_id", true)
                .with_description("The ID of the User vertex (Nurse) who performed the triage assessment.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Clinical Data ---
            PropertyConstraint::new("triage_level", true)
                .with_description("The urgency level assigned (e.g., ESI 1-5, CTAS 1-5, or institution-specific levels like 'Urgent', 'Emergent').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("chief_complaint", true)
                .with_description("The patient's main reason for seeking care (text description).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("presenting_symptoms", false)
                .with_description("Detailed list or description of symptoms.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            PropertyConstraint::new("pain_score", false)
                .with_description("Patient's self-reported pain score (0-10).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Optional, Constraint::Min(0), Constraint::Max(10), Constraint::Mutable]),

            PropertyConstraint::new("triage_notes", false)
                .with_description("Free-text notes from the triage provider.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            PropertyConstraint::new("assessed_at", true)
                .with_description("Timestamp when the assessment was completed.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Status ---
            PropertyConstraint::new("status", true)
                .with_description("The current status of the triage record.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "INITIAL".to_string(),      // First assessment taken
                    "REVISED".to_string(),      // Updated/re-evaluated due to changes in patient condition
                    "FINAL".to_string(),        // Locked assessment (used for historical/billing data)
                    "CANCELED".to_string(),     // Assessment invalidated
                ]))
                .with_default_value(JsonValue::String("INITIAL".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, ensuring clinical data integrity.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("INITIAL".to_string()),
                transitions: vec![
                    // 1. INITIAL -> REVISED: Patient condition changed, requires re-assessment.
                    StateTransition {
                        from_state: "INITIAL".to_string(),
                        to_state: "REVISED".to_string(),
                        required_rules: vec!["require_nurse_signature".to_string()],
                        triggers_events: vec!["triage.reassessment_required".to_string()],
                    },
                    // 2. REVISED -> REVISED: Can be revised multiple times before finalization.
                    StateTransition {
                        from_state: "REVISED".to_string(),
                        to_state: "REVISED".to_string(),
                        required_rules: vec!["require_time_stamp".to_string()],
                        triggers_events: vec!["triage.data_updated".to_string()],
                    },
                    // 3. INITIAL/REVISED -> FINAL: Assessment is locked for clinical/billing use.
                    StateTransition {
                        from_state: "INITIAL".to_string(),
                        to_state: "FINAL".to_string(),
                        required_rules: vec!["require_physician_signoff".to_string()],
                        triggers_events: vec!["triage.assessment_finalized".to_string()],
                    },
                    StateTransition {
                        from_state: "REVISED".to_string(),
                        to_state: "FINAL".to_string(),
                        required_rules: vec!["require_physician_signoff".to_string()],
                        triggers_events: vec!["triage.assessment_finalized".to_string()],
                    },
                    // 4. Any -> CANCELED: Assessment was recorded in error.
                    StateTransition {
                        from_state: "*".to_string(),
                        to_state: "CANCELED".to_string(),
                        required_rules: vec!["require_supervisor_reason".to_string()],
                        triggers_events: vec!["triage.record_canceled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references for linking triage levels and complaints to standardized codes.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "ESI_Triage_Scale".to_string(),
                ontology_system_id: "ESI".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Links the 'triage_level' to the Emergency Severity Index (ESI) scale 1-5. ".to_string()),
            },
            OntologyReference {
                name: "CTAS_Triage_Scale".to_string(),
                ontology_system_id: "CTAS".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Links the 'triage_level' to the Canadian Triage and Acuity Scale (CTAS).".to_string()),
            },
            OntologyReference {
                name: "ICD_10_CM".to_string(),
                ontology_system_id: "ICD_10_CM".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Used to map the 'chief_complaint' to standardized diagnostic codes.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Triage events, crucial for triggering downstream workflow.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("triage.new_assessment".to_string()),
            update_topic: Some("triage.level_updated".to_string()),
            deletion_topic: Some("triage.assessment_archived".to_string()), // Retain data for legal/auditing
            error_queue: Some("triage.data_entry_errors".to_string()),
        }
    }
}
