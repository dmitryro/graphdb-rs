use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Specimen vertex type.
///
/// This vertex captures details about a physical sample (e.g., blood, tissue) collected for testing,
/// tracking its journey from collection to lab processing.
pub struct Specimen;

impl VertexSchema for Specimen {
    fn schema_name() -> &'static str {
        "Specimen"
    }

    /// Returns the list of property constraints for the Specimen vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32) for the specimen. Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("lab_result_id", true)
                .with_description("The ID of the LabResult vertex this specimen is associated with.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Specimen Details ---
            PropertyConstraint::new("type", true)
                .with_description("The type of the biological specimen (e.g., Blood, Urine, Tissue).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable])
                .with_enum_values(EnumValues::new(vec![
                    "BLOOD".to_string(),
                    "URINE".to_string(),
                    "TISSUE".to_string(),
                    "SWAB".to_string(),
                    "CSF".to_string(), // Cerebrospinal Fluid
                ])),

            PropertyConstraint::new("collection_method", false)
                .with_description("The method used to collect the specimen (e.g., Venipuncture, Catheter, Biopsy).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Immutable]),

            PropertyConstraint::new("collection_site", false)
                .with_description("The anatomical site from which the specimen was collected (e.g., 'Left Arm', 'Lungs').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Immutable]),

            PropertyConstraint::new("collected_at", true)
                .with_description("Timestamp when the specimen was collected from the patient.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Status and Workflow ---
            PropertyConstraint::new("status", true)
                .with_description("The current status of the specimen in the lab workflow.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "COLLECTED".to_string(),    // Sample obtained from patient
                    "IN_TRANSIT".to_string(),   // Moving to laboratory
                    "RECEIVED".to_string(),     // Arrived at the lab and logged
                    "ANALYZED".to_string(),     // Analysis complete
                    "REJECTED".to_string(),     // Unsuitable for testing (e.g., hemolysis, wrong container)
                ]))
                .with_default_value(JsonValue::String("COLLECTED".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, governing specimen handling.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("COLLECTED".to_string()),
                transitions: vec![
                    // 1. COLLECTED -> IN_TRANSIT: Specimen leaves collection point
                    StateTransition {
                        from_state: "COLLECTED".to_string(),
                        to_state: "IN_TRANSIT".to_string(),
                        required_rules: vec!["require_courier_manifest".to_string()],
                        triggers_events: vec!["specimen.shipping_started".to_string()],
                    },
                    // 2. IN_TRANSIT -> RECEIVED: Specimen arrives at the lab
                    StateTransition {
                        from_state: "IN_TRANSIT".to_string(),
                        to_state: "RECEIVED".to_string(),
                        required_rules: vec!["require_lab_checkin".to_string()],
                        triggers_events: vec!["specimen.lab_received".to_string()],
                    },
                    // 3. RECEIVED -> ANALYZED: Analysis is complete (usually tied to LabResult creation)
                    StateTransition {
                        from_state: "RECEIVED".to_string(),
                        to_state: "ANALYZED".to_string(),
                        required_rules: vec!["require_result_linkage".to_string()],
                        triggers_events: vec!["specimen.analysis_complete".to_string()],
                    },
                    // 4. COLLECTED/IN_TRANSIT/RECEIVED -> REJECTED: Sample unusable
                    StateTransition {
                        from_state: "COLLECTED".to_string(),
                        to_state: "REJECTED".to_string(),
                        required_rules: vec!["require_rejection_reason".to_string()],
                        triggers_events: vec!["specimen.rejected".to_string()],
                    },
                    StateTransition {
                        from_state: "IN_TRANSIT".to_string(),
                        to_state: "REJECTED".to_string(),
                        required_rules: vec!["require_rejection_reason".to_string()],
                        triggers_events: vec!["specimen.rejected".to_string()],
                    },
                    StateTransition {
                        from_state: "RECEIVED".to_string(),
                        to_state: "REJECTED".to_string(),
                        required_rules: vec!["require_rejection_reason".to_string()],
                        triggers_events: vec!["specimen.rejected".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references for standardizing specimen type and methods.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "LOINC_Specimen".to_string(),
                ontology_system_id: "LOINC_Codes".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Uses LOINC codes to standardize specimen type and source (e.g., 'SER/PLASMA' for serum/plasma).".to_string()),
            },
            OntologyReference {
                name: "SNOMED_CT_Procedure".to_string(),
                ontology_system_id: "SNOMED_CT_Clinical_Terms".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("References SNOMED CT for collection methods (e.g., 'Venipuncture procedure').".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Specimen lifecycle events, vital for lab integration.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("specimen.collected".to_string()),
            update_topic: Some("specimen.status_update".to_string()),
            deletion_topic: None, // Specimen records must be retained
            error_queue: Some("specimen.processing_errors".to_string()),
        }
    }
}
