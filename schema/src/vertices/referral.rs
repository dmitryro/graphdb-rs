use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Referral vertex type.
///
/// This vertex models the coordination activity of sending a patient from a referring
/// physician to a referred physician, often a specialist, tracking its status and reasons.
pub struct Referral;

impl VertexSchema for Referral {
    fn schema_name() -> &'static str {
        "Referral"
    }

    /// Returns the list of property constraints for the Referral vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the Patient being referred. Required, Indexed.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("encounter_id", true)
                .with_description("Reference ID to the originating Encounter vertex. Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("referring_doctor_id", true)
                .with_description("ID of the Doctor initiating the referral.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("referred_doctor_id", true)
                .with_description("ID of the Doctor or service the patient is being referred to.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            // --- Referral Details ---
            PropertyConstraint::new("specialty", true)
                .with_description("The specialty or service requested (e.g., 'Cardiology', 'Physical Therapy'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),

            PropertyConstraint::new("reason", true)
                .with_description("Clinical reason for the referral.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            // --- Status and Time ---
            PropertyConstraint::new("status", true)
                .with_description("The current status of the referral process.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "PENDING".to_string(),   // Issued, awaiting recipient action
                    "ACCEPTED".to_string(),  // Recipient has agreed to see the patient
                    "DECLINED".to_string(),  // Recipient has refused (requires reason)
                    "COMPLETED".to_string(), // Patient was seen and consultation finished
                    "CANCELLED".to_string(), // Referring doctor withdrew the request
                ]))
                .with_default_value(JsonValue::String("PENDING".to_string())),
            
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the referral was created.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp when the referral was last updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, modeling the coordination flow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PENDING".to_string()),
                transitions: vec![
                    // 1. Recipient Action: PENDING -> ACCEPTED
                    StateTransition {
                        from_state: "PENDING".to_string(),
                        to_state: "ACCEPTED".to_string(),
                        required_rules: vec!["require_recipient_confirmation".to_string()],
                        triggers_events: vec!["referral.accepted".to_string()],
                    },
                    // 2. Recipient Action: PENDING -> DECLINED
                    StateTransition {
                        from_state: "PENDING".to_string(),
                        to_state: "DECLINED".to_string(),
                        required_rules: vec!["require_declination_reason".to_string()],
                        triggers_events: vec!["referral.declined".to_string()],
                    },
                    // 3. Service Completion: ACCEPTED -> COMPLETED
                    StateTransition {
                        from_state: "ACCEPTED".to_string(),
                        to_state: "COMPLETED".to_string(),
                        required_rules: vec!["require_consultation_report".to_string()],
                        triggers_events: vec!["referral.completed".to_string()],
                    },
                    // 4. Withdrawal by Referring Provider
                    StateTransition {
                        from_state: "PENDING|ACCEPTED".to_string(),
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["referral.cancelled".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies related to medical specialties.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "Health_Care_Provider_Taxonomy".to_string(),
                ontology_system_id: "HCPCS".to_string(),
                uri: Some("https://www.cms.gov/Medicare/Coding/HCPCSCoding/HCPCS_Overview".to_string()),
                reference_uri: None,
                description: Some("Taxonomy codes often used for billing and identifying specialties/services.".to_string()),
            },
            OntologyReference {
                name: "Service_Type_Codes".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: Some("http://www.hl7.org/".to_string()),
                reference_uri: None,
                description: Some("HL7 codes for different types of services or specialties.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Referral lifecycle events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("referral.created_new".to_string()),
            update_topic: Some("referral.status_change".to_string()),
            deletion_topic: None, // Referrals should be tracked as CANCELLED or DECLINED, not deleted.
            error_queue: Some("referral.coordination_errors".to_string()),
        }
    }
}
