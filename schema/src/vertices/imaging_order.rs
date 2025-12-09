use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the ImagingOrder vertex type.
///
/// Description: Represents a request for a diagnostic imaging procedure for a patient.
/// This vertex links Patient, Doctor, and potentially an Encounter.
pub struct ImagingOrder;

impl ImagingOrder {
    /// Provides the possible modalities (imaging types).
    fn modality_values() -> Vec<String> {
        vec![
            "CT".to_string(), "MRI".to_string(), "XRAY".to_string(), "US".to_string(), 
            "PET".to_string(), "NM".to_string(), "FLUORO".to_string(), // Common Modalities
        ]
    }

    /// Provides the possible order priority levels.
    fn priority_values() -> Vec<String> {
        vec![
            "STAT".to_string(), // Immediately
            "ASAP".to_string(), // As Soon As Possible
            "Urgent".to_string(), 
            "Routine".to_string(),
        ]
    }

    /// Provides the possible statuses for the imaging order workflow.
    fn status_values() -> Vec<String> {
        vec![
            "Requested".to_string(), 
            "Scheduled".to_string(),
            "InProgress".to_string(), 
            "Completed".to_string(),
            "Canceled".to_string(),
            "Rejected".to_string(),
        ]
    }
}

impl VertexSchema for ImagingOrder {
    fn schema_name() -> &'static str {
        "ImagingOrder"
    }

    /// Returns the list of property constraints for the ImagingOrder vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            // --- Relational IDs (Edges) ---
            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex the order is for.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),
            
            PropertyConstraint::new("ordered_by_doctor_id", true)
                .with_description("ID of the User/Doctor who placed the order.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),
            
            PropertyConstraint::new("encounter_id", false)
                .with_description("ID of the clinical Encounter this order is linked to.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Mutable]),

            // --- Order Details ---
            PropertyConstraint::new("modality", true)
                .with_description("The type of imaging procedure (e.g., CT, MRI).")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(ImagingOrder::modality_values()))
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("body_part", true)
                .with_description("The anatomical site being imaged.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]), // Can be modified/clarified

            PropertyConstraint::new("protocol", false)
                .with_description("Specific instructions or sequence of the scan.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Mutable]),

            PropertyConstraint::new("priority", true)
                .with_description("Urgency level of the order.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(ImagingOrder::priority_values()))
                .with_default_value(JsonValue::String("Routine".to_string()))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
            
            PropertyConstraint::new("notes", false)
                .with_description("Free text notes or clinical rationale for the order.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Mutable]),

            // --- Workflow and Time ---
            PropertyConstraint::new("ordered_at", true)
                .with_description("Timestamp when the order was placed. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("status", true)
                .with_description("Current state in the imaging workflow.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(ImagingOrder::status_values()))
                .with_default_value(JsonValue::String("Requested".to_string()))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
        ]
    }

    /// Lifecycle tracks the progress of the imaging order from request to completion.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Requested".to_string()),
                transitions: vec![
                    StateTransition {
                        from_state: "Requested".to_string(),
                        to_state: "Scheduled".to_string(),
                        required_rules: vec!["require_appointment_link".to_string()],
                        triggers_events: vec!["imaging_order.scheduled".to_string()],
                    },
                    StateTransition {
                        from_state: "Scheduled".to_string(),
                        to_state: "InProgress".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["imaging_order.patient_checked_in".to_string()],
                    },
                    StateTransition {
                        from_state: "InProgress".to_string(),
                        to_state: "Completed".to_string(),
                        required_rules: vec!["require_image_link".to_string()], // Must link to Image/Result vertex
                        triggers_events: vec!["imaging_order.completed".to_string()],
                    },
                    // Termination paths
                    StateTransition {
                        from_state: "Requested".to_string(),
                        to_state: "Rejected".to_string(),
                        required_rules: vec!["require_rejection_reason".to_string()],
                        triggers_events: vec!["imaging_order.rejected".to_string()],
                    },
                    StateTransition {
                        from_state: "*".to_string(), // Can cancel from any state before 'Completed'
                        to_state: "Canceled".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["imaging_order.canceled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for classification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "LOINC_Procedure".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("https://loinc.org/".to_string()),
                reference_uri: None,
                description: Some("Used to classify the specific imaging procedure (Modality + Body Part).".to_string()),
            },
            OntologyReference {
                name: "SNOMED_BodySite".to_string(),
                ontology_system_id: "SNOMED CT".to_string(),
                uri: Some("https://www.snomed.org/snomed-ct/".to_string()),
                reference_uri: None,
                description: Some("Used to code the 'body_part' property (e.g., finding the correct concept for 'Right Knee').".to_string()),
            },
            OntologyReference {
                name: "DICOM_Modality".to_string(),
                ontology_system_id: "DICOM".to_string(),
                uri: Some("https://www.dicomstandard.org/".to_string()),
                reference_uri: None,
                description: Some("Provides standardized codes for imaging modality.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for ImagingOrder management.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("imaging_order.created".to_string()),
            update_topic: Some("imaging_order.updated".to_string()),
            deletion_topic: Some("imaging_order.canceled_or_rejected".to_string()),
            error_queue: Some("imaging_order.validation_errors".to_string()),
        }
    }
}
