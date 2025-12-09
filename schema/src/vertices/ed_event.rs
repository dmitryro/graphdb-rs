use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::rules::{SchemaRule, EnforcementLevel}; 
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the EdEvent vertex type.
///
/// Description: A specialized event record used to track granular, time-sensitive
/// actions and occurrences specific to an emergency department encounter.
pub struct EdEvent;

impl EdEvent {
    /// Provides a list of typical high-level event types in an Emergency Department.
    fn event_type_values() -> Vec<String> {
        vec![
            "TriageCompleted".to_string(),
            "MedicationAdministered".to_string(),
            "ProcedurePerformed".to_string(),
            "ImagingOrdered".to_string(),
            "ConsultRequested".to_string(),
            "CriticalLabResultReceived".to_string(),
            "PatientDeterioration".to_string(),
            "PhysicianExamStarted".to_string(),
            "PatientDischarged".to_string(),
        ]
    }
}

impl VertexSchema for EdEvent {
    fn schema_name() -> &'static str {
        "EdEvent"
    }

    /// Returns the list of property constraints for the EdEvent vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("encounter_id", true)
                .with_description("ID of the Encounter vertex this event belongs to. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("event_type", true)
                .with_description("The category of the event (e.g., TriageCompleted, MedicationAdministered). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(EdEvent::event_type_values()))
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("occurred_at", true)
                .with_description("Timestamp when the event physically occurred. Required, Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("recorded_by_user_id", true)
                .with_description("ID of the User who recorded the event. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            // --- Optional/Descriptive Fields ---
            PropertyConstraint::new("event_description", false)
                .with_description("A detailed, unstructured description of the event.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),

            PropertyConstraint::new("associated_entity_id", false)
                .with_description("Optional ID linking this event to another vertex (e.g., a Procedure, LabResult, or Medication).")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),
        ]
    }

    /// EdEvent records are typically immutable historical facts. The lifecycle prevents updates after creation.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                // This rule governs the overall event record integrity, by using the 'id'
                element: "id".to_string(), 
                initial_state: None, // No formal state machine for 'id'
                transitions: vec![], // No state transitions allowed
                
                // FIX: Changed Vec<String> to Vec<SchemaRule> to resolve E0308 error.
                pre_action_checks: vec![
                    SchemaRule {
                        name: "prevent_modification".to_string(),
                        description: "Ensures no properties of an existing EdEvent are modified after creation.".to_string(),
                        // A placeholder expression that ensures modification is only allowed if it's the creation action.
                        // In a real system, this checks if the action is an update and if so, fails.
                        condition_expression: "ACTION_TYPE == 'CREATE' || (ACTION_TYPE == 'UPDATE' && NO_CHANGES_DETECTED)".to_string(),
                        enforcement_level: EnforcementLevel::HardStop,
                    }
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for ED-specific actions and events.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "SNOMED_CT_Event".to_string(),
                ontology_system_id: "SNOMED".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: Some("SNOMED Concept: 272379006 |Event (event)|".to_string()),
                description: Some("SNOMED CT concepts for clinical actions and occurrences.".to_string()),
            },
            OntologyReference {
                name: "HL7_EventCodes".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: None,
                reference_uri: None,
                description: Some("Relevant HL7 administrative codes for event types.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for EdEvent CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("ed_event.created".to_string()),
            update_topic: None, // Updates are generally not allowed for historical events
            deletion_topic: Some("ed_event.deleted".to_string()), // Deletion allowed for correction/error
            error_queue: Some("ed_event.errors".to_string()),
        }
    }
}