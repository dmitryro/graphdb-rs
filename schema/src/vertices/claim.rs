use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Claim vertex type.
///
/// Description: A formal request by a healthcare provider to a payer (like an insurance company)
/// for payment for services rendered to a patient.
pub struct Claim;

impl Claim {
    /// Provides a list of standard processing statuses for an insurance claim.
    fn status_values() -> Vec<String> {
        vec![
            "Submitted".to_string(),    // Initial state upon submission
            "Processing".to_string(),   // Under review by the payer
            "Approved".to_string(),     // Payment authorized, pending disbursement
            "Denied".to_string(),       // Payment refused
            "Paid".to_string(),         // Payment has been successfully disbursed
            "Cancelled".to_string(),    // Claim withdrawn or invalidated
        ]
    }
}

impl VertexSchema for Claim {
    fn schema_name() -> &'static str {
        "Claim"
    }

    /// Returns the list of property constraints for the Claim vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex the services were for. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("insurance_id", true)
                .with_description("ID of the InsurancePlan/Payer vertex being billed. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            // --- Financial & Date Properties ---
            PropertyConstraint::new("date_of_service", true)
                .with_description("The date the medical services were rendered.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            // FIX: Removed Constraint::Min(0.0) since it expects i64, not f64.
            PropertyConstraint::new("amount_billed", true)
                .with_description("The total monetary amount requested from the payer.")
                .with_data_type(DataType::Float)
                .with_constraints(vec![Constraint::Required]), 

            // FIX: Removed Constraint::Min(0.0) since it expects i64, not f64.
            PropertyConstraint::new("amount_covered", true)
                .with_description("The amount of the claim approved for coverage/payment.")
                .with_data_type(DataType::Float)
                .with_constraints(vec![Constraint::Required]),

            // --- Status Field ---
            PropertyConstraint::new("status", true)
                .with_description("The current processing status of the claim.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Claim::status_values()))
                .with_default_value(JsonValue::String("Submitted".to_string())),

            PropertyConstraint::new("service_lines", false)
                .with_description("Detailed breakdown of services and procedures, stored as a JSON array.")
                .with_data_type(DataType::Json),
        ]
    }

    /// Defines lifecycle rules using the 'status' property to manage the claim processing flow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Submitted".to_string()),
                transitions: vec![
                    // 1. Initial Review: Submitted -> Processing
                    StateTransition {
                        from_state: "Submitted".to_string(),
                        to_state: "Processing".to_string(),
                        required_rules: vec!["require_initial_validation".to_string()],
                        triggers_events: vec!["claim.begun_processing".to_string()],
                    },
                    // 2. Adjudication: Processing -> Approved
                    StateTransition {
                        from_state: "Processing".to_string(),
                        to_state: "Approved".to_string(),
                        required_rules: vec!["require_adjudication_signoff".to_string()],
                        triggers_events: vec!["claim.approved".to_string()],
                    },
                    // 3. Adjudication: Processing -> Denied
                    StateTransition {
                        from_state: "Processing".to_string(),
                        to_state: "Denied".to_string(),
                        required_rules: vec!["require_denial_reason".to_string()],
                        triggers_events: vec!["claim.denied".to_string()],
                    },
                    // 4. Payment: Approved -> Paid
                    StateTransition {
                        from_state: "Approved".to_string(),
                        to_state: "Paid".to_string(),
                        required_rules: vec!["require_payment_reference".to_string()],
                        triggers_events: vec!["claim.paid".to_string()],
                    },
                    // 5. Termination: Submitted/Processing -> Cancelled
                    StateTransition {
                        from_state: "Submitted".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["claim.canceled".to_string()],
                    },
                    StateTransition {
                        from_state: "Processing".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["claim.canceled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for medical coding systems used in claims.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "CPT_HCPCS".to_string(),
                ontology_system_id: "CPT".to_string(),
                uri: Some("https://www.ama-assn.org/about/cpt".to_string()),
                reference_uri: Some("https://www.cms.gov/medicare/coding/hcpcsanccmsmenac/overview".to_string()),
                description: Some("Current Procedural Terminology (CPT) and Healthcare Common Procedure Coding System (HCPCS) codes for services.".to_string()),
            },
            OntologyReference {
                name: "ICD_10_CM".to_string(),
                ontology_system_id: "ICD-10-CM".to_string(),
                uri: Some("https://www.cdc.gov/nchs/icd/icd10cm.htm".to_string()),
                reference_uri: Some("https://www.cdc.gov/nchs/icd/icd10cm.htm".to_string()),
                description: Some("International Classification of Diseases, Tenth Revision, Clinical Modification (ICD-10-CM) codes for diagnoses.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Claim CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("claim.created".to_string()),
            update_topic: Some("claim.updated".to_string()),
            deletion_topic: Some("claim.deleted".to_string()),
            error_queue: Some("claim.errors".to_string()),
        }
    }
}