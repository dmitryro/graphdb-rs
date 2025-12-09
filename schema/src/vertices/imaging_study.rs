use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the ImagingStudy vertex type.
///
/// This schema defines constraints and a lifecycle for a radiological study performed on a patient,
/// such as an X-ray, MRI, or CT scan. It tracks the progress of the study.
pub struct ImagingStudy;

impl VertexSchema for ImagingStudy {
    fn schema_name() -> &'static str {
        "ImagingStudy"
    }

    /// Returns the list of property constraints for the ImagingStudy vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex this study was performed on. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("imaging_order_id", false)
                .with_description("ID of the original ImagingOrder that initiated this study. Optional.")
                .with_data_type(DataType::Integer),

            // --- Study Details ---
            PropertyConstraint::new("modality", true)
                .with_description("Type of imaging used (e.g., 'CT', 'MRI', 'XRAY', 'US').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "CT".to_string(),
                    "MR".to_string(),
                    "XA".to_string(),
                    "US".to_string(),
                    "PET".to_string(),
                ])),

            PropertyConstraint::new("protocol", false)
                .with_description("Specific sequence or technical instructions used for the study.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("body_part", true)
                .with_description("The anatomical region studied (e.g., 'Chest', 'Head', 'Abdomen').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("contrast", false)
                .with_description("Indicates if contrast media was used (e.g., 'With IV Contrast').")
                .with_data_type(DataType::String),

            PropertyConstraint::new("performed_at", true)
                .with_description("Timestamp when the imaging procedure was completed.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            // --- Lifecycle Status Field ---
            PropertyConstraint::new("status", true)
                .with_description("The current progress status of the imaging study.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "SCHEDULED".to_string(), // Initial state, awaiting performance
                    "IN_PROGRESS".to_string(), // The study is being performed
                    "READY_FOR_INTERPRETATION".to_string(), // Images are captured, awaiting radiologist
                    "FINALIZED".to_string(), // Interpretation report is complete and signed off
                    "CANCELED".to_string(), // Study was stopped or retracted
                ]))
                .with_default_value(serde_json::Value::String("SCHEDULED".to_string())),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to manage study progress.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("SCHEDULED".to_string()),
                transitions: vec![
                    // 1. Study Started: SCHEDULED -> IN_PROGRESS
                    StateTransition {
                        from_state: "SCHEDULED".to_string(),
                        to_state: "IN_PROGRESS".to_string(),
                        required_rules: vec!["require_technologist_checkin".to_string()],
                        triggers_events: vec!["imaging.started".to_string()],
                    },
                    // 2. Imaging Done: IN_PROGRESS -> READY_FOR_INTERPRETATION
                    StateTransition {
                        from_state: "IN_PROGRESS".to_string(),
                        to_state: "READY_FOR_INTERPRETATION".to_string(),
                        required_rules: vec!["require_image_upload_complete".to_string()],
                        triggers_events: vec!["imaging.ready_for_review".to_string()],
                    },
                    // 3. Review Complete: READY_FOR_INTERPRETATION -> FINALIZED
                    StateTransition {
                        from_state: "READY_FOR_INTERPRETATION".to_string(),
                        to_state: "FINALIZED".to_string(),
                        required_rules: vec!["require_radiologist_signoff".to_string()],
                        triggers_events: vec!["imaging.finalized".to_string()],
                    },
                    // 4. Study Cancelled (can happen anytime before FINALIZED)
                    StateTransition {
                        from_state: "*".to_string(), // '*' allows transition from any state
                        to_state: "CANCELED".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["imaging.canceled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for imaging studies.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "DICOM_Modality_Codes".to_string(),
                ontology_system_id: "DICOM".to_string(),
                uri: Some("http://dicom.nema.org/medical/dicom/current/output/html/part16.html".to_string()),
                reference_uri: Some("http://dicom.nema.org/medical/dicom/current/output/html/part16.html".to_string()),
                description: Some("Digital Imaging and Communications in Medicine (DICOM) standard codes.".to_string()),
            },
            OntologyReference {
                name: "SNOMEDCT_Body_Parts".to_string(),
                ontology_system_id: "SNOMED".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("SNOMED CT terms for anatomical body parts.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for ImagingStudy CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("imaging_study.created".to_string()),
            update_topic: Some("imaging_study.updated".to_string()),
            deletion_topic: Some("imaging_study.deleted".to_string()),
            error_queue: Some("imaging_study.errors".to_string()),
        }
    }
}
