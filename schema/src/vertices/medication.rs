use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the Medication vertex type.
///
/// This schema defines constraints for drug definitions, including names, class, and
/// references to standard pharmaceutical ontologies. It includes a lifecycle to manage
/// the active status of the drug within the system's formulary.
pub struct Medication;

impl VertexSchema for Medication {
    fn schema_name() -> &'static str {
        "Medication"
    }

    /// Returns the list of property constraints for the Medication vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Fields from the Model ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("name", true)
                .with_description("The canonical full name of the medication (e.g., 'Amoxicillin 250 mg Capsule'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("brand_name", false)
                .with_description("Commercial brand name, if applicable (e.g., 'Amoxil'). Optional.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("generic_name", false)
                .with_description("The active ingredient or generic substance name (e.g., 'Amoxicillin'). Optional.")
                .with_data_type(DataType::String),

            PropertyConstraint::new("medication_class", true)
                .with_description("The therapeutic or chemical class of the drug (e.g., 'Antibiotic', 'Beta-Blocker'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),
            
            // --- Lifecycle Status Field ---
            PropertyConstraint::new("status", true)
                .with_description("The formulary status of the medication (String). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "PENDING".to_string(), // Initial state, awaiting review
                    "ACTIVE".to_string(),  // Approved for use
                    "DEPRECATED".to_string(), // Replaced by newer drug, kept for history
                    "INACTIVE".to_string(), // Recalled or permanently removed
                ]))
                .with_default_value(serde_json::Value::String("PENDING".to_string())),
        ]
    }

    /// Defines lifecycle rules using the 'status' property.
    /// This manages the formulary status of the medication within the system.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("PENDING".to_string()),
                transitions: vec![
                    // 1. Review/Activation: PENDING -> ACTIVE
                    StateTransition {
                        from_state: "PENDING".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_pharmacy_approval".to_string()], // Example rule
                        triggers_events: vec!["medication.activated".to_string()],
                    },
                    // 2. Standard Change: ACTIVE -> DEPRECATED
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "DEPRECATED".to_string(),
                        required_rules: vec!["require_formulary_review".to_string()],
                        triggers_events: vec!["medication.deprecated".to_string()],
                    },
                    // 3. Recall/Permanent Removal: ACTIVE -> INACTIVE
                    StateTransition {
                        from_state: "ACTIVE".to_string(),
                        to_state: "INACTIVE".to_string(),
                        required_rules: vec!["require_safety_officer_signoff".to_string()],
                        triggers_events: vec!["medication.recalled".to_string()],
                    },
                    // 4. Deprecation Reversal (Rare): DEPRECATED -> ACTIVE
                    StateTransition {
                        from_state: "DEPRECATED".to_string(),
                        to_state: "ACTIVE".to_string(),
                        required_rules: vec!["require_pharmacy_approval".to_string()],
                        triggers_events: vec!["medication.reactivated".to_string()],
                    },
                ],
                // No pre-checks defined here
                pre_action_checks: vec![],
                // No post-actions defined here
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard drug and pharmaceutical terminologies.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "RxNorm_Core".to_string(),
                ontology_system_id: "RXNORM".to_string(),
                uri: Some("http://www.nlm.nih.gov/research/umls/rxnorm".to_string()),
                reference_uri: Some("http://www.nlm.nih.gov/research/umls/rxnorm".to_string()),
                description: Some("Standardized nomenclature for clinical drugs.".to_string()),
            },
            OntologyReference {
                name: "NDC_Identifiers".to_string(),
                ontology_system_id: "NDC".to_string(),
                uri: Some("https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-ndc".to_string()),
                reference_uri: Some("https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-ndc".to_string()),
                description: Some("Regulatory identifiers for all drugs manufactured and distributed in the US.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Medication CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("medication.created".to_string()),
            update_topic: Some("medication.updated".to_string()),
            deletion_topic: Some("medication.deleted".to_string()),
            error_queue: Some("medication.errors".to_string()),
        }
    }
}
