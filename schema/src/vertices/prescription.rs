use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Prescription vertex type.
///
/// This schema represents the authoritative record of a medication order issued
/// by a clinician to a patient. It includes essential details about the drug,
/// regimen, and validity period.
pub struct Prescription;

impl VertexSchema for Prescription {
    fn schema_name() -> &'static str {
        "Prescription"
    }

    /// Returns the list of property constraints for the Prescription vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the Patient receiving the medication. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("doctor_id", true)
                .with_description("Reference ID to the Doctor who issued the prescription. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            // --- Medication Details ---
            PropertyConstraint::new("medication_name", true)
                .with_description("The name of the prescribed medication (e.g., brand or generic). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("dose", true)
                .with_description("The quantity or strength of the medication per administration (e.g., '10mg', '5mL'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("frequency", true)
                .with_description("How often the medication should be taken (e.g., 'Once Daily', 'TID'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            // --- Validity and Status ---
            PropertyConstraint::new("start_date", true)
                .with_description("The date the prescription was issued and became valid. Required.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("end_date", false)
                .with_description("The date the prescription is no longer valid or refills expire. Optional.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Optional]),
            
            PropertyConstraint::new("status", true)
                .with_description("The current legal and fulfillment status of the prescription.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "ACTIVE".to_string(),       // Currently valid and can be filled
                    "EXPIRED".to_string(),      // End date reached
                    "CANCELLED".to_string(),    // Officially revoked by the doctor
                    "PENDING_REVIEW".to_string(), // Requires further clinician attention
                    "COMPLETED".to_string(),    // Final refill filled
                ]))
                .with_default_value(JsonValue::String("ACTIVE".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, modeling the prescription's validity.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("ACTIVE".to_string()),
                transitions: vec![
                    // 1. Cancellation: ACTIVE -> CANCELLED (Doctor revokes)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec!["require_doctor_auth".to_string()],
                        triggers_events: vec!["prescription.cancelled".to_string()],
                    },
                    // 2. Expiration (Time-based transition, typically handled by system monitoring)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "EXPIRED".to_string(),
                        required_rules: vec!["check_end_date_passed".to_string()],
                        triggers_events: vec!["prescription.expired".to_string()],
                    },
                    // 3. Completion (Final refill has been dispensed)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "COMPLETED".to_string(),
                        required_rules: vec!["check_refills_exhausted".to_string()],
                        triggers_events: vec!["prescription.completed".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies related to drug classification and coding.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "RxNorm".to_string(),
                ontology_system_id: "RXNORM".to_string(),
                uri: Some("https://www.nlm.nih.gov/research/umls/rxnorm/index.html".to_string()),
                reference_uri: None,
                description: Some("Standardized nomenclature for clinical drugs.".to_string()),
            },
            OntologyReference {
                name: "SNOMED_CT_Dosage".to_string(),
                ontology_system_id: "SNOMEDCT".to_string(),
                uri: Some("https://www.snomed.org/snomed-ct".to_string()),
                reference_uri: None,
                description: Some("Used for standardized coding of dose and frequency concepts.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Prescription lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("prescription.issued".to_string()),
            update_topic: Some("prescription.updated".to_string()),
            deletion_topic: None, // Prescriptions should generally be marked as CANCELLED or EXPIRED, not deleted
            error_queue: Some("prescription.errors".to_string()),
        }
    }
}
