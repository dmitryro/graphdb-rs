use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, MessagingSchema
};
use crate::rules::{SchemaRule, EnforcementLevel}; 
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the generic Event vertex type.
///
/// Description: Represents a generic, immutable historical event associated with a patient,
/// such as a significant administrative action, a major diagnosis, or an administrative status change.
pub struct Event;

impl Event {
    /// Provides a list of generic event types.
    fn event_type_values() -> Vec<String> {
        vec![
            "PatientAdmission".to_string(),
            "PatientTransfer".to_string(),
            "MajorDiagnosisAdded".to_string(),
            "AllergyReported".to_string(),
            "ConsultationRequested".to_string(),
            "PatientDischarged".to_string(),
            "VitalsRecorded".to_string(),
        ]
    }
}

impl VertexSchema for Event {
    fn schema_name() -> &'static str {
        "Event"
    }

    /// Returns the list of property constraints for the Event vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Relationship Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("patient_id", true)
                .with_description("ID of the Patient vertex this event is linked to. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("event_type", true)
                .with_description("The category of the event (e.g., Admission, DiagnosisAdded). Required.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(Event::event_type_values()))
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("event_date", true)
                .with_description("Timestamp when the event physically occurred. Required, Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("description", true)
                .with_description("A detailed, unstructured description of the event. Required.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Event records are historical facts and must be immutable after creation.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                // Rule governs the overall event record integrity.
                element: "id".to_string(), 
                initial_state: None, 
                transitions: vec![],
                
                // FIX: SchemaRule required here to prevent E0308.
                pre_action_checks: vec![
                    SchemaRule {
                        name: "enforce_immutability_after_creation".to_string(),
                        description: "Ensures no properties of an existing Event are modified after creation.".to_string(),
                        // Enforce immutability using a check that prevents updates.
                        condition_expression: "ACTION_TYPE == 'CREATE' || (ACTION_TYPE == 'UPDATE' && NO_CHANGES_DETECTED)".to_string(),
                        enforcement_level: EnforcementLevel::HardStop,
                    }
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard terminologies for general clinical events.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "SNOMED_CT_GeneralEvent".to_string(),
                ontology_system_id: "SNOMED".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: Some("SNOMED Concept: 272379006 |Event (event)|".to_string()),
                description: Some("SNOMED CT concepts for clinical actions and occurrences.".to_string()),
            },
            OntologyReference {
                name: "FHIR_AuditEvent".to_string(),
                ontology_system_id: "FHIR".to_string(),
                uri: Some("http://hl7.org/fhir/R4".to_string()),
                reference_uri: Some("http://hl7.org/fhir/auditevent.html".to_string()),
                description: Some("Maps to the FHIR AuditEvent for tracking key actions.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Event CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("event.created".to_string()),
            update_topic: None, // Updates are not allowed for immutable historical events
            deletion_topic: Some("event.deleted".to_string()), // Deletion allowed for correction/error
            error_queue: Some("event.errors".to_string()),
        }
    }
}
