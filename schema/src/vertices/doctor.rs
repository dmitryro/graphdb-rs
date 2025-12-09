use serde_json::json;

// Import PropertyConstraint from constraints module, not properties module
use crate::definitions::VertexSchema;
use crate::properties::{
    PropertyDefinition, OntologyReference, 
};
use crate::constraints::{ PropertyConstraint, DataType }; // Changed: import from constraints
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};

/// Implementation of the VertexSchema for the Patient vertex type.
pub struct Patient;

// Note on PropertyConstraint Usage:
// We use the new `PropertyConstraint::new(name, required)` constructor 
// and the fluent setter `.with_description(description)` defined in constraints.rs.
// The `data_type`, `constraints`, and `default_value` fields, which are not
// part of the PropertyConstraint struct, are now implicitly defined by the
// constraints applied (e.g., `Required` implies `required: true`).

impl VertexSchema for Patient {
    fn schema_name() -> &'static str {
        "Patient"
    }

    /// Returns the list of property constraints for the Patient vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // 1. MRN (Medical Record Number) - Required & Unique
            PropertyConstraint::new("mrn", true)
                .with_description("Medical Record Number. Required and unique identifier."),

            // 2. Date of Birth - Required
            PropertyConstraint::new("date_of_birth", true)
                .with_description("Patient's date of birth (Timestamp). Required."),

            // 3. Gender Code - Required
            PropertyConstraint::new("gender_code", true)
                .with_description("Gender code (e.g., 'M', 'F', 'U'). Required. Defaults to 'U' (Unknown)."),
            
            // 4. Status - Required
            PropertyConstraint::new("status", true)
                .with_description("The current patient status (e.g., 'Admitted', 'Discharged'). Required. Defaults to 'Admitted'."),

            // 5. Is Deceased - Required
            PropertyConstraint::new("is_deceased", true)
                .with_description("Boolean flag indicating if the patient is deceased. Required. Defaults to false."),

            // 6. Preferred Language - Optional
            PropertyConstraint::new("preferred_language", false)
                .with_description("The patient's preferred communication language (optional)."),
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
