use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema, StateTransition, SchemaAction,
};
use crate::rules::SchemaRule;
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the Observation vertex type.
///
/// Description: Represents a single, factual observation recorded about a patient,
/// such as a vital sign measurement, a clinical finding, or a physical exam result.
pub struct Observation;

impl VertexSchema for Observation {
    fn schema_name() -> &'static str {
        "Observation"
    }

    /// Returns the property constraints for the Observation vertex.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Internal primary ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("Reference ID to the Patient this observation is for. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("encounter_id", true)
                .with_description("Reference ID to the Encounter during which the observation was made. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Observation Data (Generally Immutable) ---
            PropertyConstraint::new("observation_type", true)
                .with_description("The type/code defining what was observed (e.g., Heart Rate, Temperature). Should use LOINC.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("value", true)
                .with_description("The recorded value, stored as a string to handle numeric and descriptive data.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("unit", false)
                .with_description("The unit of measure for the value (e.g., 'bpm', 'kg', 'C'). Should use UCUM.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional]),

            PropertyConstraint::new("observed_at", true)
                .with_description("The UTC timestamp when the observation was physically made. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("observed_by_user_id", true)
                .with_description("The User ID of the staff member who recorded the observation. Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Lifecycle & Audit ---
            PropertyConstraint::new("status", true)
                .with_description("The verification status of the observation data.")
                .with_data_type(DataType::String)
                .with_enum_values(crate::constraints::EnumValues::new(vec![
                    "Entered".to_string(), "Verified".to_string(), "Amended".to_string(), "Cancelled".to_string()
                ]))
                .with_default_value(JsonValue::String("Entered".to_string()))
                .with_constraints(vec![Constraint::Required]),

            PropertyConstraint::new("created_at", true)
                .with_description("The UTC timestamp when the record was initially created. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("The UTC timestamp of the last modification. Automatically updated.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Defines the lifecycle for observation data, primarily focusing on verification.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Entered".to_string()),
                transitions: vec![
                    // 1. Entered -> Verified (Requires clinical review/sign-off)
                    StateTransition {
                        from_state: "Entered".to_string(),
                        to_state: "Verified".to_string(),
                        required_rules: vec!["require_clinical_signoff".to_string()],
                        triggers_events: vec!["observation.verified".to_string()],
                    },
                    // 2. Verified -> Amended (Correction required; often means deprecating and creating a new linked observation)
                    StateTransition {
                        from_state: "Verified".to_string(),
                        to_state: "Amended".to_string(),
                        required_rules: vec!["require_justification".to_string(), "create_new_observation_link".to_string()],
                        triggers_events: vec!["observation.amended".to_string()],
                    },
                    // 3. Any state -> Cancelled (Removal due to error or duplicate)
                    StateTransition {
                        from_state: "*".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec!["require_audit_trail".to_string()],
                        triggers_events: vec!["observation.cancelled".to_string()],
                    },
                ],
                pre_action_checks: vec![
                    SchemaRule::new("check_write_permission_on_patient_record"),
                ],
                post_action_actions: vec![
                    SchemaAction::GraphMutation {
                        mutation_type: "update_updated_at".to_string(),
                        target_schema: Self::schema_name().to_string(),
                    },
                ],
            }
        ]
    }

    /// References to standard terminologies used for clinical observation data.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "LOINCObservationType".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("http://loinc.org/".to_string()),
                reference_uri: None,
                description: Some("LOINC codes for defining the type of observation (e.g., Heart Rate).".to_string()),
            },
            OntologyReference {
                name: "UCUMUnitOfMeasure".to_string(),
                ontology_system_id: "UCUM".to_string(),
                uri: Some("http://unitsofmeasure.org/".to_string()),
                reference_uri: None,
                description: Some("UCUM codes for standardizing measurement units (e.g., '{beats}/min').".to_string()),
            },
        ]
    }

    /// Defines the messaging topics related to this schema element.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("clinical.observation_entered".to_string()),
            update_topic: Some("clinical.observation_status_changed".to_string()),
            deletion_topic: None, // Observations are cancelled, not deleted
            error_queue: Some("clinical.observation_data_errors".to_string()),
        }
    }
}
