use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};
use crate::rules::{SchemaRule, EnforcementLevel}; 
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Disposition vertex type.
///
/// Description: Represents the formal record of what happened to the patient
/// at the conclusion of an Encounter (e.g., discharged, admitted, transferred).
pub struct Disposition;

impl Disposition {
    /// Provides a list of common, high-level disposition outcomes.
    fn type_values() -> Vec<String> {
        vec![
            "Discharged_Home".to_string(),
            "Discharged_Home_Health".to_string(), // Discharged with formal home healthcare
            "Admitted_Inpatient".to_string(),     // Admitted to a bed within the same facility
            "Transfer_Facility".to_string(),      // Transferred to another hospital or facility
            "Expired".to_string(),                // Death occurred during the encounter
            "Left_Against_Medical_Advice".to_string(), // AMA
            "Other".to_string(),
        ]
    }
}

impl VertexSchema for Disposition {
    fn schema_name() -> &'static str {
        "Disposition"
    }

    /// Returns the list of property constraints for the Disposition vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("encounter_id", true)
                .with_description("ID of the Encounter vertex this disposition concludes. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex. Redundant but useful for indexing. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("disposition_type", true)
                .with_description("The outcome category of the encounter termination (e.g., Admitted, Discharged). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Disposition::type_values()))
                .with_constraints(vec![Constraint::Required]),

            // --- Conditional/Optional Fields ---
            PropertyConstraint::new("admitting_service", false)
                .with_description("The service/department the patient was admitted to (if disposition_type is Admitted_Inpatient).")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            PropertyConstraint::new("admitting_doctor_id", false)
                .with_description("ID of the Doctor responsible for the admission (if admitted).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),

            PropertyConstraint::new("transfer_facility_id", false)
                .with_description("ID of the external facility or partner transferred to (if disposition_type is Transfer_Facility).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),

            PropertyConstraint::new("discharge_instructions", false)
                .with_description("Text of the instructions given to the patient upon discharge.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            PropertyConstraint::new("disposed_at", true)
                .with_description("Timestamp when the disposition event officially occurred. Required.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            // --- Verification State (Simple Lifecycle) ---
            PropertyConstraint::new("verification_status", true)
                .with_description("Indicates if the disposition has been verified and finalized by the responsible party.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "Initial".to_string(), // Immediately recorded upon disposition
                    "Verified".to_string(), // Confirmed by the clinician and/or utilized by billing
                    "Error".to_string(),    // Identified as incorrect and pending correction
                ]))
                .with_default_value(JsonValue::String("Initial".to_string())),
        ]
    }

    /// Disposition has a simple lifecycle: Initial -> Verified. Once Verified, it is immutable.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "verification_status".to_string(),
                initial_state: Some("Initial".to_string()),
                transitions: vec![
                    // 1. Verification: Initial -> Verified
                    StateTransition {
                        from_state: "Initial".to_string(),
                        to_state: "Verified".to_string(),
                        required_rules: vec!["require_clinician_signoff".to_string()],
                        triggers_events: vec!["disposition.verified".to_string()],
                    },
                    // 2. Correction: Initial -> Error
                    StateTransition {
                        from_state: "Initial".to_string(),
                        to_state: "Error".to_string(),
                        required_rules: vec!["require_error_reason".to_string()],
                        triggers_events: vec!["disposition.error".to_string()],
                    },
                ],
                // Fixed: Include all required fields for SchemaRule
                pre_action_checks: vec![
                    SchemaRule {
                        name: "enforce_immutability_on_verified".to_string(),
                        description: "Prevents modifications to disposition records once they are verified".to_string(),
                        condition_expression: "verification_status != 'Verified'".to_string(),
                        enforcement_level: EnforcementLevel::HardStop,
                    }
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for disposition classification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "SNOMED_CT_Disposition".to_string(),
                ontology_system_id: "SNOMED".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: Some("SNOMED Concept: 419099009 |Disposition (record artefact)|".to_string()),
                description: Some("SNOMED CT concepts for standardized patient disposition outcomes.".to_string()),
            },
            OntologyReference {
                name: "HL7_Discharge_Codes".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Relevant HL7 administrative codes for discharge/disposition status.".to_string()),
            },
            OntologyReference {
                name: "FHIR_DischargeDisposition".to_string(),
                ontology_system_id: "FHIR".to_string(),
                uri: Some("http://hl7.org/fhir/R4".to_string()),
                reference_uri: Some("http://hl7.org/fhir/valueset-discharge-disposition.html".to_string()),
                description: Some("Maps to the Encounter.hospitalization.dischargeDisposition element in the FHIR standard.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Disposition CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("disposition.created".to_string()),
            update_topic: Some("disposition.updated".to_string()), // Should only allow updates in 'Initial' or 'Error' state
            deletion_topic: Some("disposition.deleted".to_string()),
            error_queue: Some("disposition.errors".to_string()),
        }
    }
}