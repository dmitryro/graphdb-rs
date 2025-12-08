use serde_json::json;

// Import PropertyConstraint from constraints module, not properties module
use crate::definitions::VertexSchema;
use crate::properties::{
    PropertyDefinition, OntologyReference, DataType,
};
use crate::constraints::PropertyConstraint; // Changed: import from constraints
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};

/// Implementation of the VertexSchema for the Patient vertex type.
pub struct Patient;

impl VertexSchema for Patient {
    fn schema_name() -> &'static str {
        "Patient"
    }

    /// Returns the list of property constraints for the Patient vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // 1. MRN (Medical Record Number)
            PropertyConstraint {
                name: "mrn".to_string(), // Medical Record Number
                data_type: crate::constraints::DataType::String,
                constraints: vec![
                    crate::constraints::Constraint::Required,
                    crate::constraints::Constraint::Unique,
                ],
                default_value: None,
            },
            
            // 2. Date of Birth
            PropertyConstraint {
                name: "date_of_birth".to_string(),
                data_type: crate::constraints::DataType::Timestamp,
                constraints: vec![
                    crate::constraints::Constraint::Required,
                ],
                default_value: None,
            },

            // 3. Gender Code (with constraints)
            PropertyConstraint {
                name: "gender_code".to_string(),
                data_type: crate::constraints::DataType::String,
                constraints: vec![
                    crate::constraints::Constraint::Required,
                ],
                default_value: Some(json!("U")),
            },
            
            // 4. Status
            PropertyConstraint {
                name: "status".to_string(),
                data_type: crate::constraints::DataType::String,
                constraints: vec![
                    crate::constraints::Constraint::Required,
                ],
                default_value: Some(json!("Admitted")),
            },

            // 5. Is Deceased
            PropertyConstraint {
                name: "is_deceased".to_string(),
                data_type: crate::constraints::DataType::Boolean,
                constraints: vec![
                    crate::constraints::Constraint::Required,
                ],
                default_value: Some(json!(false)),
            },

            // 6. Preferred Language
            PropertyConstraint {
                name: "preferred_language".to_string(),
                data_type: crate::constraints::DataType::String,
                constraints: vec![],
                default_value: None,
            },
        ]
    }

    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Pre-Registered".to_string()),
                transitions: vec![
                    StateTransition {
                        from_state: "Pre-Registered".to_string(),
                        to_state: "Admitted".to_string(),
                        required_rules: vec!["Has_Billing_Address".to_string()],
                        triggers_events: vec!["PATIENT_ADMITTED".to_string()],
                    },
                    StateTransition {
                        from_state: "Admitted".to_string(),
                        to_state: "Discharged".to_string(),
                        required_rules: vec!["Has_Discharge_Summary".to_string()],
                        triggers_events: vec!["PATIENT_DISCHARGED".to_string()],
                    },
                    StateTransition {
                        from_state: "Discharged".to_string(),
                        to_state: "Archived".to_string(),
                        required_rules: vec!["Years_After_Discharge_Min_7".to_string()],
                        triggers_events: vec![],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![
                    SchemaAction::PublishMessage {
                        topic: "patient_status_changes".to_string(),
                        payload_template: "{id} transitioned from {old_status} to {new_status}".to_string()
                    }
                ],
            }
        ]
    }

    fn ontology_references() -> Vec<OntologyReference> {
        vec![]
    }
    
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("patient.created".to_string()),
            update_topic: Some("patient.updated".to_string()),
            deletion_topic: Some("patient.deleted".to_string()),
            error_queue: Some("patient.errors".to_string()),
        }
    }
}