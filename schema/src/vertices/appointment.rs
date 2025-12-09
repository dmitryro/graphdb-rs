use serde_json::json;

use crate::definitions::VertexSchema;
use crate::properties::{
    PropertyDefinition, OntologyReference,
};
use crate::constraints::{ PropertyConstraint, Constraint, DataType, EnumValues };
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};

/// Implementation of the VertexSchema for the Appointment vertex type.
/// This vertex captures scheduled interactions between a patient and a provider.
pub struct Appointment;

impl VertexSchema for Appointment {
    fn schema_name() -> &'static str {
        "Appointment"
    }

    /// Returns the list of property constraints for the Appointment vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // 1. Patient ID (Foreign Key to Patient vertex) - Required
            PropertyConstraint::new("patient_id", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![
                    Constraint::MinLength(1),
                    Constraint::Immutable, // Link to patient should not change
                ])
                .with_description("Identifier of the related Patient vertex. Required."),

            // 2. Provider ID (Foreign Key to Provider/User vertex) - Required
            PropertyConstraint::new("provider_id", true)
                .with_data_type(DataType::String)
                .with_constraints(vec![
                    Constraint::MinLength(1),
                ])
                .with_description("Identifier of the service provider (e.g., Physician, Nurse). Required."),

            // 3. Appointment Type - Required
            PropertyConstraint::new("appointment_type", true)
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "NEW_PATIENT".to_string(),
                    "FOLLOW_UP".to_string(),
                    "URGENT".to_string(),
                    "PROCEDURE".to_string(),
                    "TELEHEALTH".to_string(),
                ]))
                .with_description("The category of the appointment."),

            // 4. Scheduled Time - Required
            PropertyConstraint::new("scheduled_time", true)
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![
                    Constraint::FutureTime, // Scheduled time must be in the future (at creation)
                ])
                .with_description("The planned start date and time of the appointment (UTC)."),

            // 5. Duration Minutes - Required
            PropertyConstraint::new("duration_minutes", true)
                .with_data_type(DataType::Integer)
                .with_constraints(vec![
                    Constraint::Min(1),    // Updated to use Constraint::Min(i64)
                    Constraint::Max(180),  // Updated to use Constraint::Max(i64)
                ])
                .with_description("Expected length of the appointment in minutes."),

            // 6. Reason (for visit) - Optional
            PropertyConstraint::new("reason", false)
                .with_data_type(DataType::String)
                .with_description("The patient's stated reason for the visit."),

            // 7. Status - Required (Drives Lifecycle)
            PropertyConstraint::new("status", true)
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(vec![
                    "SCHEDULED".to_string(),
                    "CONFIRMED".to_string(),
                    "CHECKED_IN".to_string(),
                    "COMPLETED".to_string(),
                    "CANCELLED".to_string(),
                    "NO_SHOW".to_string(),
                ]))
                .with_description("The current attendance status of the appointment."),

            // 8. Location ID (Foreign Key to Location vertex) - Optional
            PropertyConstraint::new("location_id", false)
                .with_data_type(DataType::String)
                .with_description("Identifier of the clinic or facility where the appointment is held."),

            // 9. Notes - Optional
            PropertyConstraint::new("notes", false)
                .with_data_type(DataType::String)
                .with_description("Internal scheduling or clinical notes."),
        ]
    }

    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("SCHEDULED".to_string()),
                transitions: vec![
                    // Flow to completion
                    StateTransition {
                        from_state: "SCHEDULED".to_string(),
                        to_state: "CONFIRMED".to_string(),
                        required_rules: vec!["Confirmation_Received".to_string()],
                        triggers_events: vec!["APPOINTMENT_CONFIRMED".to_string()],
                    },
                    StateTransition {
                        from_state: "CONFIRMED".to_string(),
                        to_state: "CHECKED_IN".to_string(),
                        required_rules: vec!["Check_In_Time_Recorded".to_string()],
                        triggers_events: vec!["PATIENT_CHECKED_IN".to_string()],
                    },
                    StateTransition {
                        from_state: "CHECKED_IN".to_string(),
                        to_state: "COMPLETED".to_string(),
                        required_rules: vec!["Encounter_Note_Signed".to_string()],
                        triggers_events: vec!["APPOINTMENT_COMPLETED".to_string()],
                    },
                    // Cancellation paths
                    StateTransition {
                        from_state: "SCHEDULED".to_string(),
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec!["Cancellation_Reason_Required".to_string()],
                        triggers_events: vec!["APPOINTMENT_CANCELLED".to_string()],
                    },
                    StateTransition {
                        from_state: "CONFIRMED".to_string(),
                        to_state: "CANCELLED".to_string(),
                        required_rules: vec!["Cancellation_Reason_Required".to_string()],
                        triggers_events: vec!["APPOINTMENT_CANCELLED".to_string()],
                    },
                    // No-Show paths
                    StateTransition {
                        from_state: "SCHEDULED".to_string(),
                        to_state: "NO_SHOW".to_string(),
                        required_rules: vec!["Time_Elapsed_Past_Scheduled".to_string()],
                        triggers_events: vec!["APPOINTMENT_NO_SHOW".to_string()],
                    },
                    StateTransition {
                        from_state: "CONFIRMED".to_string(),
                        to_state: "NO_SHOW".to_string(),
                        required_rules: vec!["Time_Elapsed_Past_Scheduled".to_string()],
                        triggers_events: vec!["APPOINTMENT_NO_SHOW".to_string()],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![
                    SchemaAction::PublishMessage {
                        topic: "appointment_status_changes".to_string(),
                        payload_template: "Appointment {id} status changed from {old_status} to {new_status}".to_string()
                    }
                ],
            }
        ]
    }

    fn ontology_references() -> Vec<OntologyReference> {
        // Reference to common procedural and diagnostic code standards used in healthcare
        vec![
            OntologyReference {
                name: "CPT (Current Procedural Terminology)".to_string(),
                ontology_system_id: "CPT".to_string(),
                // Include both 'uri' and 'reference_uri'
                uri: Some("https://www.ama-assn.org/about/cpt-current-procedural-terminology".to_string()),
                reference_uri: Some("https://www.ama-assn.org/about/cpt-current-procedural-terminology".to_string()),
                description: Some("The standard for describing medical procedures and services provided during an encounter.".to_string()),
            },
            OntologyReference {
                name: "ICD-10 (International Classification of Diseases)".to_string(),
                ontology_system_id: "ICD10".to_string(),
                // Include both 'uri' and 'reference_uri'
                uri: Some("https://www.who.int/standards/classifications/classification-of-diseases".to_string()),
                reference_uri: Some("https://www.who.int/standards/classifications/classification-of-diseases".to_string()),
                description: Some("Used for classifying and coding diagnoses, symptoms, and procedures associated with the appointment.".to_string()),
            },
        ]
    }

    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("appointment.scheduled".to_string()),
            update_topic: Some("appointment.updated".to_string()),
            deletion_topic: Some("appointment.cancelled".to_string()),
            error_queue: Some("appointment.errors".to_string()),
        }
    }
}