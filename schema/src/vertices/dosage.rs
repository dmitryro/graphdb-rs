use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::rules::{SchemaRule, EnforcementLevel}; 
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Dosage vertex type.
///
/// Description: Represents the specific instructions for administering a medication, 
/// linking to the Medication vertex for the drug substance itself.
pub struct Dosage;

impl Dosage {
    /// Provides the possible status states for a medication dosage order.
    fn status_values() -> Vec<String> {
        vec![
            "Active".to_string(),       // Currently being administered
            "OnHold".to_string(),       // Temporarily paused
            "Discontinued".to_string(), // Permanently stopped
        ]
    }
}

impl VertexSchema for Dosage {
    fn schema_name() -> &'static str {
        "Dosage"
    }

    /// Returns the list of property constraints for the Dosage vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique]),

            PropertyConstraint::new("medication_id", true)
                .with_description("ID of the Medication vertex (the drug substance). Required.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("dosage_amount", true)
                .with_description("The specific quantity to be administered (e.g., '10 mg', '2 tablets'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("dosage_frequency", true)
                .with_description("The schedule of administration (e.g., 'TID', 'q6h', 'once daily'). Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("route", false)
                .with_description("The method of delivery (e.g., 'Oral', 'IV', 'Topical').")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            PropertyConstraint::new("status", true)
                .with_description("The current status of the dosage order (Active, OnHold, Discontinued).")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Dosage::status_values()))
                .with_default_value(JsonValue::String("Active".to_string())),
            
            // --- Reason Fields (Conditional) ---
            PropertyConstraint::new("discontinuation_reason", false)
                .with_description("The reason why the dosage was Discontinued or OnHold.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            // --- Audit Timestamps ---
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the dosage order was created. Required.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("updated_at", false)
                .with_description("Timestamp of the last update or status change.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![]),
        ]
    }

    /// Dosage lifecycle manages the status of the order (Active -> OnHold/Discontinued).
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Active".to_string()),
                transitions: vec![
                    // 1. Pause: Active -> OnHold
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "OnHold".to_string(),
                        // Requires a reason for pausing the medication
                        required_rules: vec!["require_onhold_reason".to_string()],
                        triggers_events: vec!["dosage.on_hold".to_string()],
                    },
                    // 2. Resume: OnHold -> Active
                    StateTransition {
                        from_state: "OnHold".to_string(),
                        to_state: "Active".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["dosage.resumed".to_string()],
                    },
                    // 3. Stop: Active/OnHold -> Discontinued
                    StateTransition {
                        from_state: "Active".to_string(),
                        to_state: "Discontinued".to_string(),
                        // Requires a reason for stopping the medication
                        required_rules: vec!["require_discontinuation_reason".to_string()],
                        triggers_events: vec!["dosage.discontinued".to_string()],
                    },
                    StateTransition {
                        from_state: "OnHold".to_string(),
                        to_state: "Discontinued".to_string(),
                        // Requires a reason for stopping the medication
                        required_rules: vec!["require_discontinuation_reason".to_string()],
                        triggers_events: vec!["dosage.discontinued".to_string()],
                    },
                ],
                // Any update must update the 'updated_at' timestamp
                pre_action_checks: vec![
                    SchemaRule {
                        name: "update_timestamp_on_edit".to_string(),
                        description: "Ensures the updated_at timestamp is set on any status change".to_string(),
                        condition_expression: "updated_at IS NOT NULL".to_string(),
                        enforcement_level: EnforcementLevel::HardStop,
                    }
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for dosage and frequency classification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "RxNorm_Dosage".to_string(),
                ontology_system_id: "RxNorm".to_string(),
                uri: Some("https://www.nlm.nih.gov/research/umls/rxnorm".to_string()),
                reference_uri: None,
                description: Some("Concepts for medication dosing, strength, and forms.".to_string()),
            },
            OntologyReference {
                name: "FHIR_Dosage".to_string(),
                ontology_system_id: "FHIR".to_string(),
                uri: Some("http://hl7.org/fhir/R4".to_string()),
                reference_uri: Some("http://hl7.org/fhir/datatypes-definitions.html#Dosage".to_string()),
                description: Some("Maps to the complex 'Dosage' datatype in the FHIR standard.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Dosage CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("dosage.created".to_string()),
            update_topic: Some("dosage.updated".to_string()), 
            deletion_topic: Some("dosage.deleted".to_string()),
            error_queue: Some("dosage.errors".to_string()),
        }
    }
}