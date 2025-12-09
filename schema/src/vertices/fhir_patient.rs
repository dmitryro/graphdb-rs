// schema/src/vertices/fhir_patient.rs
use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the FHIR_Patient vertex type.
///
/// This schema defines constraints for FHIR-mapped patient data, including FHIR ID, names,
/// birth date, gender, and addresses. It serves as a mapping node for interoperability.
pub struct FHIR_Patient;

impl VertexSchema for FHIR_Patient {
    fn schema_name() -> &'static str {
        "FHIR_Patient"
    }

    /// Returns the list of property constraints for the FHIR_Patient vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Fields from the Model ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (Uuid). Required."),
            PropertyConstraint::new("fhir_id", true)
                .with_description("FHIR resource ID (e.g., 'Patient/12345'). Required."),
            PropertyConstraint::new("name", true)
                .with_description("Array of structured names (JSON). Required."),
            PropertyConstraint::new("birth_date", false)
                .with_description("Birth date (Timestamp). Optional."),
            PropertyConstraint::new("gender", false)
                .with_description("Administrative gender (e.g., 'male', 'female'). Optional.")
                .with_constraints(vec![Constraint::Enum(vec!["male".to_string(), "female".to_string(), "other".to_string(), "unknown".to_string()])]),
            PropertyConstraint::new("address", false)
                .with_description("Array of structured addresses (JSON). Optional."),
            // Add more FHIR fields as properties if expanded (e.g., telecom, identifier)
        ]
    }

    /// Defines lifecycle rules for FHIR synchronization (e.g., Imported -> Validated -> Synced).
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Imported".to_string()),
                transitions: vec![
                    StateTransition {
                        from_state: "Imported".to_string(),
                        to_state: "Validated".to_string(),
                        required_rules: vec!["FHIR_Validation_Passed".to_string()],
                        triggers_events: vec!["FHIR_PATIENT_VALIDATED".to_string()],
                    },
                    StateTransition {
                        from_state: "Validated".to_string(),
                        to_state: "Synced".to_string(),
                        required_rules: vec!["FHIR_Sync_Complete".to_string()],
                        triggers_events: vec!["FHIR_PATIENT_SYNCED".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![
                    SchemaAction::PublishMessage {
                        topic: "fhir_patient_status_changes".to_string(),
                        payload_template: "{id} transitioned from {old_status} to {new_status}".to_string()
                    }
                ],
            }
        ]
    }

    /// References to FHIR standards for patient resources.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "FHIR_Patient_Resource".to_string(),
                ontology_system_id: "FHIR_R4".to_string(),
                uri: Some("http://hl7.org/fhir/R4/patient.html".to_string()),
                reference_uri: Some("http://hl7.org/fhir/R4/patient.html".to_string()),
                description: Some("FHIR Patient resource definition.".to_string()),
            },
            OntologyReference {
                name: "FHIR_AdministrativeGender".to_string(),
                ontology_system_id: "FHIR_ValueSet".to_string(),
                uri: Some("http://hl7.org/fhir/ValueSet/administrative-gender".to_string()),
                reference_uri: Some("http://hl7.org/fhir/ValueSet/administrative-gender".to_string()),
                description: Some("FHIR codes for administrative gender.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for FHIR_Patient CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("fhir_patient.created".to_string()),
            update_topic: Some("fhir_patient.updated".to_string()),
            deletion_topic: Some("fhir_patient.deleted".to_string()),
            error_queue: Some("fhir_patient.errors".to_string()),
        }
    }
}
