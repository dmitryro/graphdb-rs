use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the RadiologyReport vertex type.
///
/// This vertex stores the formal clinical document produced by a radiologist
/// analyzing an imaging study. It is characterized by critical fields like findings,
/// impression, and its signatory status.
pub struct RadiologyReport;

impl VertexSchema for RadiologyReport {
    fn schema_name() -> &'static str {
        "RadiologyReport"
    }

    /// Returns the list of property constraints for the RadiologyReport vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("imaging_study_id", true)
                .with_description("Reference ID to the ImagingStudy vertex this report belongs to. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("radiologist_id", true)
                .with_description("Reference ID to the Doctor (Radiologist) who authored the report. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            // --- Report Content ---
            PropertyConstraint::new("findings", true)
                .with_description("Detailed observational text describing what was seen in the images. Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("impression", true)
                .with_description("The radiologist's summarized diagnostic conclusion. Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("recommendation", false)
                .with_description("Suggested follow-up actions (e.g., 'Recommend CT scan in 6 weeks'). Optional.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            // --- Status and Time ---
            PropertyConstraint::new("reported_at", true)
                .with_description("The date and time the report was finalized/transcribed.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
            
            PropertyConstraint::new("status", true)
                .with_description("The legal/clinical status of the report (e.g., preliminary, signed, amended).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "PRELIMINARY".to_string(), // Initial draft, pending review/signing
                    "FINAL".to_string(),       // Signed and legally complete
                    "AMENDED".to_string(),     // Final report modified after signing
                    "CANCELLED".to_string(),   // Report withdrawn or replaced
                ]))
                .with_default_value(JsonValue::String("PRELIMINARY".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, focusing on documentation integrity.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PRELIMINARY".to_string()),
                transitions: vec![
                    // 1. Finalization/Signing: PRELIMINARY -> FINAL
                    StateTransition {
                        from_state: "PRELIMINARY".to_string(),
                        to_state: "FINAL".to_string(),
                        required_rules: vec!["require_radiologist_signature".to_string()],
                        triggers_events: vec!["report.finalized".to_string()],
                    },
                    // 2. Amendment: FINAL -> AMENDED
                    StateTransition {
                        from_state: "FINAL".to_string(),
                        to_state: "AMENDED".to_string(),
                        required_rules: vec!["require_amendment_reason".to_string(), "require_radiologist_signature".to_string()],
                        triggers_events: vec!["report.amended".to_string()],
                    },
                    // 3. Reversion to FINAL (after amendment) - less common, but possible if amendment is retracted
                    StateTransition {
                        from_state: "AMENDED".to_string(),
                        to_state: "FINAL".to_string(),
                        required_rules: vec!["require_amendment_retraction_note".to_string()],
                        triggers_events: vec!["report.reverted_to_final".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies related to imaging procedures and findings.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "RadLex".to_string(),
                ontology_system_id: "RADLEX".to_string(),
                uri: Some("http://www.radlex.org/".to_string()),
                reference_uri: None,
                description: Some("Standardized terminology for radiology reporting and indexing.".to_string()),
            },
            OntologyReference {
                name: "LOINC_Reporting".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("https://loinc.org/".to_string()),
                reference_uri: None,
                description: Some("Used for coding the type of report and its sections (e.g., 'Impression').".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for RadiologyReport lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("report.created".to_string()),
            update_topic: Some("report.updated".to_string()),
            deletion_topic: None, // Reports should always be preserved, potentially marked as CANCELLED.
            error_queue: Some("report.processing_errors".to_string()),
        }
    }
}
