use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the StateProvince vertex type.
///
/// This vertex captures static geographical data for states, provinces, or equivalent
/// first-level administrative divisions, crucial for location and jurisdictional data.
pub struct StateProvince;

impl VertexSchema for StateProvince {
    fn schema_name() -> &'static str {
        "StateProvince"
    }

    /// Returns the list of property constraints for the StateProvince vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32) for the state/province record.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("name", true)
                .with_description("The full official name of the state or province.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("code", true)
                .with_description("The standardized, two- or three-letter/digit code for the state/province (e.g., 'CA', 'NY', 'BC').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("country", false)
                .with_description("The country this state/province belongs to (ISO 3166-1 alpha-3 code is recommended, e.g., 'USA', 'CAN').")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Immutable]),
        ]
    }

    /// Defines lifecycle rules. Since geographical data is static, the rules mainly
    /// relate to verification and deprecation, rather than workflow transitions.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "data_accuracy_status".to_string(),
                initial_state: Some("VERIFIED".to_string()),
                transitions: vec![
                    // VERIFIED -> PENDING_UPDATE: If the name or boundaries change.
                    StateTransition {
                        from_state: "VERIFIED".to_string(),
                        to_state: "PENDING_UPDATE".to_string(),
                        required_rules: vec!["require_gov_source_documentation".to_string()],
                        triggers_events: vec!["geo_data.update_required".to_string()],
                    },
                    // VERIFIED/PENDING_UPDATE -> DEPRECATED: If the entity ceases to exist (e.g., political boundary change).
                    StateTransition {
                        from_state: "*".to_string(),
                        to_state: "DEPRECATED".to_string(),
                        required_rules: vec!["require_historical_flag".to_string()],
                        triggers_events: vec!["geo_data.entity_deprecated".to_string()],
                    },
                    // PENDING_UPDATE -> VERIFIED: Update completed and verified.
                    StateTransition {
                        from_state: "PENDING_UPDATE".to_string(),
                        to_state: "VERIFIED".to_string(),
                        required_rules: vec!["require_audit_trail_complete".to_string()],
                        triggers_events: vec!["geo_data.verified".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![],
            }
        ]
    }

    /// Defines ontological references for linking codes to international standards.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "ISO_3166_2_Codes".to_string(),
                ontology_system_id: "ISO_3166_2".to_string(),
                uri: Some("https://www.iso.org/iso-3166-country-codes.html".to_string()),
                reference_uri: None,
                description: Some("Uses ISO 3166-2 codes for standardized subdivision names and codes.".to_string()),
            },
            OntologyReference {
                name: "FIPS_Codes".to_string(),
                ontology_system_id: "FIPS_Geographic_Codes".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("References FIPS codes for US states and territories, if applicable.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for data synchronization and auditing.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("geo_data.new_state_province".to_string()),
            update_topic: Some("geo_data.state_province_updated".to_string()),
            deletion_topic: None, // Historical records should be kept and marked DEPRECATED
            error_queue: Some("geo_data.sync_errors".to_string()),
        }
    }
}
