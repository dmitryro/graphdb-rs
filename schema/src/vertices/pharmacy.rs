use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Pharmacy vertex type.
///
/// This schema defines constraints for essential details about a medical pharmacy,
/// including contact information, location, and operational status.
pub struct Pharmacy;

impl VertexSchema for Pharmacy {
    fn schema_name() -> &'static str {
        "Pharmacy"
    }

    /// Returns the list of property constraints for the Pharmacy vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("name", true)
                .with_description("The official name of the pharmacy. Required, Indexed.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Indexable]),
            
            // --- Contact and Location ---
            PropertyConstraint::new("address", false)
                .with_description("The physical street address of the pharmacy. Optional, but highly recommended.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Indexable]),

            PropertyConstraint::new("contact_number", false)
                .with_description("The primary phone number. Optional.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("email", false)
                .with_description("The primary email address for professional inquiries. Optional.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Unique]),
            
            // --- Categorization ---
            PropertyConstraint::new("pharmacy_type", false)
                .with_description("The type of pharmacy (e.g., Retail, Hospital, Compounding, Mail Order). Optional.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "Retail".to_string(),
                    "Hospital".to_string(),
                    "Compounding".to_string(),
                    "Mail Order".to_string(),
                    "Clinic".to_string(),
                ])),
            
            // --- Status/Lifecycle Property ---
            PropertyConstraint::new("status", true)
                .with_description("The operational status of the pharmacy (ACTIVE, PENDING, CLOSED). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "ACTIVE".to_string(),   // Currently operating and accepting prescriptions
                    "PENDING".to_string(),  // Registration or verification in progress
                    "CLOSED".to_string(),   // Permanently or temporarily out of service
                ]))
                .with_default_value(JsonValue::String("ACTIVE".to_string())),
        ]
    }

    /// Defines lifecycle rules based on the operational 'status' property.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PENDING".to_string()),
                transitions: vec![
                    // 1. Activation: PENDING -> ACTIVE (After regulatory approval/verification)
                    StateTransition {
                        from_state: "PENDING".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_license_verification".to_string()],
                        triggers_events: vec!["pharmacy.activated".to_string()],
                    },
                    // 2. Closure: ACTIVE -> CLOSED (Business ceases operation)
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "CLOSED".to_string(),
                        required_rules: vec!["require_director_approval".to_string()],
                        triggers_events: vec!["pharmacy.closed".to_string()],
                    },
                    // 3. From PENDING to CLOSED (Registration fails)
                    StateTransition {
                        from_state: "PENDING".to_string(),
                        to_state: "CLOSED".to_string(),
                        required_rules: vec!["require_rejection_reason".to_string()],
                        triggers_events: vec!["pharmacy.registration_failed".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies related to healthcare facilities and types.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "NCPDP_Terminology".to_string(),
                ontology_system_id: "NCPDP".to_string(),
                uri: Some("https://www.ncpdp.org/".to_string()),
                reference_uri: None,
                description: Some("Standards related to pharmacy services, including IDs and transaction codes.".to_string()),
            },
            OntologyReference {
                name: "HCPCS_Drug_Codes".to_string(),
                ontology_system_id: "HCPCS".to_string(),
                uri: Some("https://www.cms.gov/Medicare/Coding/HCPCS-Coded-Listings".to_string()),
                reference_uri: None,
                description: Some("Healthcare Common Procedure Coding System for drugs and supplies.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Pharmacy CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("pharmacy.created".to_string()),
            update_topic: Some("pharmacy.updated".to_string()),
            deletion_topic: Some("pharmacy.deleted".to_string()),
            error_queue: Some("pharmacy.errors".to_string()),
        }
    }
}
