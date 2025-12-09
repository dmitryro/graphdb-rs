use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Immunization vertex type.
///
/// Description: Represents a historical fact about a vaccination administered to a patient.
/// These records are typically immutable facts, except for administrative status/notes.
pub struct Immunization;

impl Immunization {
    /// Provides the possible administrative statuses for the immunization record.
    fn record_status_values() -> Vec<String> {
        vec![
            "Valid".to_string(),
            "Entered_in_Error".to_string(), // Used when the record was created mistakenly
            "Withdrawn".to_string(),       // Record is correct, but functionally withdrawn (e.g., patient moved)
        ]
    }
}

impl VertexSchema for Immunization {
    fn schema_name() -> &'static str {
        "Immunization"
    }

    /// Returns the list of property constraints for the Immunization vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex who received the immunization. Immutable link.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Fact Properties (Immutable) ---
            PropertyConstraint::new("vaccine_name", true)
                .with_description("The name or trade name of the vaccine administered.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            PropertyConstraint::new("administration_date", true)
                .with_description("The exact timestamp (or date) the vaccine was administered. Immutable fact.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Relational / Optional Properties ---
            PropertyConstraint::new("administered_by", false)
                .with_description("ID of the User/Practitioner who administered the dose.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),

            PropertyConstraint::new("notes", false)
                .with_description("Free text notes related to the administration (e.g., site, reaction, lot number).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Mutable]),

            // --- Administrative Status (Mutable Lifecycle Element) ---
            PropertyConstraint::new("record_status", true)
                .with_description("Administrative status of the record itself, tracking data entry quality.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Immunization::record_status_values()))
                .with_default_value(JsonValue::String("Valid".to_string()))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
        ]
    }

    /// Lifecycle manages the administrative status of the immunization record.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "record_status".to_string(),
                initial_state: Some("Valid".to_string()),
                transitions: vec![
                    // Transition to mark a record as an error
                    StateTransition {
                        from_state: "Valid".to_string(),
                        to_state: "Entered_in_Error".to_string(),
                        required_rules: vec!["require_error_justification".to_string()],
                        triggers_events: vec!["immunization.error_corrected".to_string()],
                    },
                    // Transition to administratively withdraw a valid record
                    StateTransition {
                        from_state: "Valid".to_string(),
                        to_state: "Withdrawn".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["immunization.withdrawn".to_string()],
                    },
                    // Cannot return to Valid once marked as error or withdrawn
                    StateTransition {
                        from_state: "Entered_in_Error".to_string(),
                        to_state: "Entered_in_Error".to_string(), // Self-loop to signify finality
                        required_rules: vec![],
                        triggers_events: vec![],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for medical coding.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "CVX_Code".to_string(),
                ontology_system_id: "CVX".to_string(),
                uri: Some("https://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=cvx".to_string()),
                reference_uri: None,
                description: Some("CDC's Vaccine Administered (CVX) code set for identifying the specific vaccine product.".to_string()),
            },
            OntologyReference {
                name: "CPT_Code".to_string(),
                ontology_system_id: "CPT".to_string(),
                uri: Some("https://www.ama-assn.org/about/cpt-current-procedural-terminology".to_string()),
                reference_uri: None,
                description: Some("Current Procedural Terminology code for the administration procedure.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Immunization entity management.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("immunization.recorded".to_string()),
            update_topic: Some("immunization.status_updated".to_string()),
            deletion_topic: None, // Historical facts are generally not deleted, only status changed
            error_queue: Some("immunization.data_quality_alerts".to_string()),
        }
    }
}
