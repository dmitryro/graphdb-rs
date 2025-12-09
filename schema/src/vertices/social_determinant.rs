use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the SocialDeterminant vertex type.
///
/// This vertex captures data points related to a patient's Social Determinants of Health (SDOH),
/// which are critical non-clinical factors impacting health outcomes.
pub struct SocialDeterminant;

impl VertexSchema for SocialDeterminant {
    fn schema_name() -> &'static str {
        "SocialDeterminant"
    }

    /// Returns the list of property constraints for the SocialDeterminant vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, and Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("The ID of the Patient vertex this determinant relates to.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- SDOH Classification ---
            PropertyConstraint::new("factor_type", true)
                .with_description("The category of the social determinant factor (e.g., Housing, Food, Transportation).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable])
                .with_enum_values(EnumValues::new(vec![
                    "ECONOMIC_STABILITY".to_string(),
                    "NEIGHBORHOOD_AND_ENVIRONMENT".to_string(),
                    "EDUCATION_ACCESS_AND_QUALITY".to_string(),
                    "SOCIAL_AND_COMMUNITY_CONTEXT".to_string(),
                    "HEALTH_CARE_ACCESS_AND_QUALITY".to_string(),
                    "FOOD_INSECURITY".to_string(),
                    "TRANSPORTATION".to_string(),
                    "HOUSING_INSTABILITY".to_string(),
                ])),

            PropertyConstraint::new("details", false)
                .with_description("Specific details or notes on the observed determinant.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            PropertyConstraint::new("recorded_by", false)
                .with_description("The ID of the user or practitioner who recorded the determinant.")
                .with_data_type(DataType::Relationship)
                .with_constraints(vec![Constraint::Optional, Constraint::Immutable]),

            // --- Status and Time ---
            PropertyConstraint::new("status", true)
                .with_description("The processing status of the SDOH data point.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required])
                .with_enum_values(EnumValues::new(vec![
                    "COLLECTED".to_string(),    // Initially recorded/documented
                    "SCREENED".to_string(),     // Reviewed and validated by staff
                    "ACTIONABLE".to_string(),   // Identified as a priority requiring intervention
                    "RESOLVED".to_string(),     // Intervention completed or issue mitigated
                ]))
                .with_default_value(JsonValue::String("COLLECTED".to_string())),

            PropertyConstraint::new("recorded_at", true)
                .with_description("Timestamp when the social determinant was recorded.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
        ]
    }

    /// Defines lifecycle rules based on the 'status' property, governing intervention workflow.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("COLLECTED".to_string()),
                transitions: vec![
                    // 1. COLLECTED -> SCREENED: Staff validation
                    StateTransition {
                        from_state: "COLLECTED".to_string(),
                        to_state: "SCREENED".to_string(),
                        required_rules: vec!["require_staff_review".to_string()],
                        triggers_events: vec!["sdoh.review_complete".to_string()],
                    },
                    // 2. SCREENED -> ACTIONABLE: Identified as high priority
                    StateTransition {
                        from_state: "SCREENED".to_string(),
                        to_state: "ACTIONABLE".to_string(),
                        required_rules: vec!["require_intervention_plan".to_string()],
                        triggers_events: vec!["sdoh.action_required".to_string()],
                    },
                    // 3. ACTIONABLE -> RESOLVED: Intervention completed
                    StateTransition {
                        from_state: "ACTIONABLE".to_string(),
                        to_state: "RESOLVED".to_string(),
                        required_rules: vec!["require_resolution_notes".to_string()],
                        triggers_events: vec!["sdoh.issue_resolved".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references for standardizing SDOH data points.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "ICD10_SDOH".to_string(),
                ontology_system_id: "ICD-10-CM_Z_Codes".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("References ICD-10-CM Z-Codes (Factors influencing health status and contact with health services) for coding SDOH factors.".to_string()),
            },
            OntologyReference {
                name: "LOINC_SDOH".to_string(),
                ontology_system_id: "LOINC_Observations".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Uses LOINC codes for standardized questions and observations related to social determinants.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Social Determinant events.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("sdoh.new_data_point".to_string()),
            update_topic: Some("sdoh.status_updated".to_string()),
            deletion_topic: None, // SDOH data points are usually retained
            error_queue: Some("sdoh.ingestion_errors".to_string()),
        }
    }
}
