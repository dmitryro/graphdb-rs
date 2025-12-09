use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::rules::{ SchemaRule };
// Removed: use crate::rules::SchemaRule; // No longer needed as all rules/events are stored as Strings
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the LabPanel vertex type.
///
/// Description: Represents a grouping of related lab results (e.g., a "Complete Blood Count")
/// that are part of a larger LabOrder. It tracks the status of the panel results.
pub struct LabPanel;

impl LabPanel {
    /// Provides the possible result status values for the lab panel.
    fn status_values() -> Vec<String> {
        vec![
            "Pending".to_string(),      // Initial state: Panel created, waiting for results.
            "Preliminary".to_string(),  // Some or all results are available, but not final.
            "Final".to_string(),        // All results are finalized, signed, and reported.
            "Corrected".to_string(),    // Final results were corrected after being reported.
            "Cancelled".to_string(),    // Panel was cancelled before results were finalized.
        ]
    }
}

impl VertexSchema for LabPanel {
    fn schema_name() -> &'static str {
        "LabPanel"
    }

    /// Returns the list of property constraints for the LabPanel vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("lab_order_id", true)
                .with_description("ID of the parent LabOrder vertex. Immutable link.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Panel Details (Immutable intent) ---
            PropertyConstraint::new("panel_name", true)
                .with_description("The human-readable name of the panel (e.g., 'CBC', 'CMP'). Immutable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            // --- Workflow and State Properties (Mutable) ---
            PropertyConstraint::new("status", true)
                .with_description("The current result status of the lab panel.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(LabPanel::status_values()))
                .with_default_value(JsonValue::String("Pending".to_string()))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
            
            PropertyConstraint::new("resulted_at", false)
                .with_description("The timestamp when the final results were reported/finalized.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Mutable]),

            PropertyConstraint::new("finalized_by_user_id", false)
                .with_description("ID of the user (e.g., pathologist) who finalized the results.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Mutable]),
        ]
    }

    /// Lifecycle manages the result status of the lab panel.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Pending".to_string()),
                transitions: vec![
                    // --- Standard Flow ---
                    StateTransition {
                        from_state: "Pending".to_string(),
                        to_state: "Preliminary".to_string(),
                        // A preliminary result only requires a timestamp, not yet a final signature
                        required_rules: vec!["require_resulted_at_timestamp".to_string()], // Fixed type mismatch
                        triggers_events: vec!["lab_panel.preliminary_results".to_string()], // Fixed type mismatch
                    },
                    StateTransition {
                        from_state: "Preliminary".to_string(),
                        to_state: "Final".to_string(),
                        // Final results require both a timestamp and the finalizing user's ID
                        required_rules: vec![
                            "require_resulted_at_timestamp".to_string(), // Fixed type mismatch
                            "require_finalized_by_user_id".to_string(),
                        ],
                        triggers_events: vec!["lab_panel.final_results".to_string()], // Fixed type mismatch
                    },
                    // --- Correction Flow ---
                    StateTransition {
                        from_state: "Final".to_string(),
                        to_state: "Corrected".to_string(),
                        // Requires a reason for correction and re-finalization details
                        required_rules: vec![
                            "require_correction_reason".to_string(), // Fixed type mismatch
                            "require_finalized_by_user_id".to_string(),
                        ],
                        triggers_events: vec!["lab_panel.corrected_results".to_string()], // Fixed type mismatch
                    },
                    // --- Cancellation Flow (can happen anytime before Final) ---
                    StateTransition {
                        from_state: "Pending".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()], // Fixed type mismatch
                        triggers_events: vec!["lab_panel.cancelled".to_string()], // Fixed type mismatch
                    },
                    StateTransition {
                        from_state: "Preliminary".to_string(),
                        to_state: "Cancelled".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()], // Fixed type mismatch
                        triggers_events: vec!["lab_panel.cancelled".to_string()], // Fixed type mismatch
                    },
                ],
                // Cannot transition out of Final or Cancelled (except for correction)
                pre_action_checks: vec![
                    SchemaRule::new("is_not_cancelled"),
                    SchemaRule::new("is_not_corrected_except_for_correction"),
                ], 
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for panel identification.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "LOINC_Panel_Code".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("https://loinc.org/".to_string()),
                reference_uri: None,
                description: Some("LOINC code specifically identifying the lab panel group.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for LabPanel status updates.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("lab_panel.created".to_string()),
            update_topic: Some("lab_panel.status_updated".to_string()),
            deletion_topic: None, // Panels are generally part of a persistent record
            error_queue: Some("lab_panel.critical_alerts".to_string()),
        }
    }
}
