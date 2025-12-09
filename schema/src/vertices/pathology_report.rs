use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the PathologyReport vertex type.
///
/// This schema defines constraints for laboratory reports from pathology, including
/// gross and microscopic findings, molecular results, and the report lifecycle (Draft, Final).
pub struct PathologyReport;

impl VertexSchema for PathologyReport {
    fn schema_name() -> &'static str {
        "PathologyReport"
    }

    /// Returns the list of property constraints for the PathologyReport vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the Patient vertex the report belongs to. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("pathologist_id", true)
                .with_description("Reference ID to the User (Pathologist) who signed the report. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),
            
            // --- Report Content ---
            PropertyConstraint::new("gross_description", false)
                .with_description("The macro description of the specimen. Optional, can be long text.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("microscopic_description", false)
                .with_description("The description of the tissue/cells under the microscope. Optional, can be long text.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("immunohistochemistry", false)
                .with_description("Results from immunohistochemistry stains (e.g., JSON payload). Optional.")
                .with_data_type(DataType::String), // Using String to match the model, but expects parsable JSON.

            PropertyConstraint::new("molecular_test", false)
                .with_description("Results from molecular diagnostics or genetic testing (e.g., JSON payload). Optional.")
                .with_data_type(DataType::String), // Using String to match the model, but expects parsable JSON.
            
            // --- Dates and Status ---
            PropertyConstraint::new("reported_at", true)
                .with_description("The date and time the report was finalized. Required.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("status", true)
                .with_description("The current clinical status of the report (DRAFT, FINAL, AMENDED, RETRACTED). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "DRAFT".to_string(), // Initial state, not yet signed
                    "FINAL".to_string(), // Signed and released
                    "AMENDED".to_string(), // A corrected version of a FINAL report
                    "RETRACTED".to_string(), // Withdrawn from clinical use
                ]))
                .with_default_value(JsonValue::String("DRAFT".to_string())),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to manage the report's validity.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("DRAFT".to_string()),
                transitions: vec![
                    // 1. Finalization: DRAFT -> FINAL
                    StateTransition {
                        from_state: "DRAFT".to_string(),
                        to_state: "FINAL".to_string(),
                        required_rules: vec!["require_pathologist_signature".to_string()],
                        triggers_events: vec!["report.finalized".to_string()],
                    },
                    // 2. Correction: FINAL -> AMENDED
                    StateTransition {
                        from_state: "FINAL".to_string(),
                        to_state: "AMENDED".to_string(),
                        required_rules: vec!["require_amendment_reason".to_string()],
                        triggers_events: vec!["report.amended".to_string()],
                    },
                    // 3. New Final Version after Amendment: AMENDED -> FINAL
                    StateTransition {
                        from_state: "AMENDED".to_string(),
                        to_state: "FINAL".to_string(),
                        required_rules: vec!["require_pathologist_signature".to_string()],
                        triggers_events: vec!["report.finalized_amendment".to_string()],
                    },
                    // 4. Withdrawal/Error: FINAL/AMENDED -> RETRACTED
                    StateTransition {
                        from_state: "*".to_string(),
                        to_state: "RETRACTED".to_string(),
                        required_rules: vec!["require_medical_director_approval".to_string()],
                        triggers_events: vec!["report.retracted".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for pathology findings and procedures.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "SNOMED_CT_Morphology".to_string(),
                ontology_system_id: "SNOMED".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: None,
                description: Some("Standardized nomenclature for anatomical sites and morphology (findings).".to_string()),
            },
            OntologyReference {
                name: "LOINC_MolecularTests".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("http://loinc.org".to_string()),
                reference_uri: None,
                description: Some("Identifiers for laboratory and clinical observations, specifically molecular tests.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for PathologyReport CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("report.pathology.created".to_string()),
            update_topic: Some("report.pathology.updated".to_string()),
            deletion_topic: Some("report.pathology.deleted".to_string()),
            error_queue: Some("report.pathology.errors".to_string()),
        }
    }
}
