use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::rules::{SchemaRule, EnforcementLevel}; 
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the LabOrder vertex type.
///
/// Description: Represents a clinical request for laboratory testing for a patient.
/// The lifecycle tracks the order from creation to the return of results.
pub struct LabOrder;

impl LabOrder {
    /// Provides the possible workflow statuses for the lab order.
    fn status_values() -> Vec<String> {
        vec![
            "Ordered".to_string(),          // Initial state: Request has been placed.
            "AwaitingCollection".to_string(), // Patient is waiting for sample collection.
            "SampleCollected".to_string(),  // Sample has been taken from the patient.
            "InTransit".to_string(),        // Sample is being shipped to the lab.
            "Processing".to_string(),       // Sample is at the lab and testing is underway.
            "Completed".to_string(),        // Results are finalized and available.
            "Canceled".to_string(),         // Order was canceled before completion.
        ]
    }

    /// Provides the possible priority values.
    fn priority_values() -> Vec<String> {
        vec![
            "Routine".to_string(),
            "Urgent".to_string(),
            "STAT".to_string(), // Emergency
        ]
    }
}

impl VertexSchema for LabOrder {
    fn schema_name() -> &'static str {
        "LabOrder"
    }

    /// Returns the list of property constraints for the LabOrder vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex the order is for. Immutable link.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("encounter_id", false)
                .with_description("Optional ID of the Encounter vertex this order originated from. Immutable link.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Immutable]),
            
            PropertyConstraint::new("ordered_by_doctor_id", true)
                .with_description("ID of the Practitioner/Doctor vertex who placed the order. Immutable link.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Order Details (Immutable intent) ---
            PropertyConstraint::new("test_requested", true)
                .with_description("The description or code of the lab test requested. Immutable after order placement.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            PropertyConstraint::new("ordered_at", true)
                .with_description("The timestamp when the order was placed. Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            // --- Workflow and State Properties (Mutable) ---
            PropertyConstraint::new("priority", true)
                .with_description("The urgency level of the order.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(LabOrder::priority_values()))
                .with_default_value(JsonValue::String("Routine".to_string()))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("status", true)
                .with_description("The current workflow status of the lab order.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(LabOrder::status_values()))
                .with_default_value(JsonValue::String("Ordered".to_string()))
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),
            
            PropertyConstraint::new("notes", false)
                .with_description("Any additional clinical notes or instructions regarding the order.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Mutable]),
        ]
    }

    /// Lifecycle manages the status of the lab order workflow.
    /// This defines the valid path of a lab order from request to completion.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Ordered".to_string()),
                transitions: vec![
                    // Standard flow
                    StateTransition {
                        from_state: "Ordered".to_string(),
                        to_state: "AwaitingCollection".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["lab_order.activated".to_string()],
                    },
                    StateTransition {
                        from_state: "AwaitingCollection".to_string(),
                        to_state: "SampleCollected".to_string(),
                        required_rules: vec!["require_collector_id".to_string()],
                        triggers_events: vec!["lab_order.sample_collected".to_string()],
                    },
                    StateTransition {
                        from_state: "SampleCollected".to_string(),
                        to_state: "InTransit".to_string(),
                        required_rules: vec![],
                        triggers_events: vec![],
                    },
                    StateTransition {
                        from_state: "InTransit".to_string(),
                        to_state: "Processing".to_string(),
                        required_rules: vec!["require_lab_receipt_timestamp".to_string()],
                        triggers_events: vec!["lab_order.received_by_lab".to_string()],
                    },
                    StateTransition {
                        from_state: "Processing".to_string(),
                        to_state: "Completed".to_string(),
                        required_rules: vec!["require_result_link".to_string()], // Requires link to LabResult vertex
                        triggers_events: vec!["lab_order.results_ready".to_string()],
                    },

                    // Cancellation flow (can be cancelled until processing starts)
                    StateTransition {
                        from_state: "Ordered".to_string(),
                        to_state: "Canceled".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["lab_order.canceled".to_string()],
                    },
                    StateTransition {
                        from_state: "AwaitingCollection".to_string(),
                        to_state: "Canceled".to_string(),
                        required_rules: vec!["require_cancellation_reason".to_string()],
                        triggers_events: vec!["lab_order.canceled".to_string()],
                    },
                    StateTransition {
                        from_state: "SampleCollected".to_string(),
                        to_state: "Canceled".to_string(),
                        required_rules: vec![
                            "require_cancellation_reason".to_string(),
                            "sample_disposal_log".to_string()
                        ],
                        triggers_events: vec!["lab_order.canceled_post_collection".to_string()],
                    },
                ],
                pre_action_checks: vec![
                    SchemaRule {
                        name: "is_not_completed_or_canceled".to_string(),
                        description: "Prevents modifications to completed or canceled orders".to_string(),
                        condition_expression: "status NOT IN ('Completed', 'Canceled')".to_string(),
                        enforcement_level: EnforcementLevel::HardStop,
                    }
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for lab test coding and billing.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "LOINC_Code".to_string(),
                ontology_system_id: "LOINC".to_string(),
                uri: Some("https://loinc.org/".to_string()),
                reference_uri: None,
                description: Some("Logical Observation Identifiers Names and Codes for test identification.".to_string()),
            },
            OntologyReference {
                name: "CPT_Code".to_string(),
                ontology_system_id: "CPT".to_string(),
                uri: Some("https://www.ama-assn.org/about/cpt-current-procedural-terminology".to_string()),
                reference_uri: None,
                description: Some("Current Procedural Terminology code for laboratory services billing.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for LabOrder workflow management.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("lab_order.created".to_string()),
            update_topic: Some("lab_order.status_updated".to_string()),
            deletion_topic: None, // Orders should be canceled, not deleted
            error_queue: Some("lab_order.critical_alerts".to_string()),
        }
    }
}
