use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents the relationship between a PatientJourney and an Appointment.
/// This edge links a patient's journey to their scheduled future or past visits.
/// 
/// Edge Label: HAS_APPOINTMENT
/// Source Vertex: PatientJourney
/// Target Vertex: Appointment
/// 
/// Clinical Purpose: Scheduled future or past visits
/// FHIR Mapping: Appointment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasAppointment {
    /// The unique identifier for this specific edge instance (stored as String in the graph).
    pub id: String,
    
    /// ID of the source vertex (PatientJourney)
    pub source_id: Uuid,
    /// ID of the target vertex (Appointment)
    pub target_id: Uuid,

    /// The scheduled date and time for the appointment.
    pub scheduled_time: DateTime<Utc>,

    /// The actual start time of the appointment. If null, the appointment hasn't started yet.
    pub actual_start_time: Option<DateTime<Utc>>,

    /// The actual end time of the appointment. If null, the appointment hasn't concluded yet.
    pub actual_end_time: Option<DateTime<Utc>>,

    /// The duration of the appointment in minutes. Calculated from actual start and end times.
    pub duration_minutes: Option<i64>,

    /// Current status of the appointment (e.g., "Scheduled", "Confirmed", "Arrived", "InProgress", "Completed", "Cancelled", "NoShow").
    pub status: String,

    /// The type of appointment (e.g., "Routine", "FollowUp", "Emergency", "WalkIn").
    pub appointment_type: String,

    /// The reason for the appointment.
    pub reason: String,

    /// Optional cancellation reason if status is "Cancelled" or "NoShow".
    pub cancellation_reason: Option<String>,

    /// The date and time when this appointment link was created.
    pub created_at: DateTime<Utc>,
    
    /// A map for any additional, non-standard properties stored directly on the edge.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

// --- Domain-Specific Methods and Constants ---
impl HasAppointment {
    pub const RELATIONSHIP_TYPE: &'static str = "HAS_APPOINTMENT";

    /// Creates a new, minimal HasAppointment instance.
    pub fn new(
        id: String,
        source_id: Uuid,
        target_id: Uuid,
        scheduled_time: DateTime<Utc>,
        appointment_type: String,
        reason: String,
    ) -> Self {
        Self {
            id,
            source_id,
            target_id,
            scheduled_time,
            actual_start_time: None,
            actual_end_time: None,
            duration_minutes: None,
            status: "Scheduled".to_string(),
            appointment_type,
            reason,
            cancellation_reason: None,
            created_at: Utc::now(),
            additional_properties: HashMap::new(),
        }
    }

    /// Marks the appointment as started with the actual start time.
    pub fn start_appointment(&mut self, start_time: DateTime<Utc>) -> Result<(), String> {
        if self.actual_start_time.is_some() {
            return Err("Appointment has already been started.".to_string());
        }
        
        self.actual_start_time = Some(start_time);
        self.status = "InProgress".to_string();
        Ok(())
    }

    /// Marks the appointment as completed with the actual end time and calculates duration.
    pub fn complete_appointment(&mut self, end_time: DateTime<Utc>) -> Result<(), String> {
        let start_time = self.actual_start_time
            .ok_or("Cannot complete appointment: no start time recorded.")?;
        
        if end_time < start_time {
            return Err("End time cannot be before start time.".to_string());
        }
        
        // Calculate duration and store in minutes (i64)
        let duration: Duration = end_time.signed_duration_since(start_time);
        self.duration_minutes = Some(duration.num_minutes());
        self.actual_end_time = Some(end_time);
        self.status = "Completed".to_string();
        Ok(())
    }

    /// Cancels the appointment with a reason.
    pub fn cancel_appointment(&mut self, reason: String) -> Result<(), String> {
        if self.status == "Completed" {
            return Err("Cannot cancel a completed appointment.".to_string());
        }
        
        self.status = "Cancelled".to_string();
        self.cancellation_reason = Some(reason);
        Ok(())
    }

    /// Marks the appointment as a no-show.
    pub fn mark_no_show(&mut self, reason: Option<String>) {
        self.status = "NoShow".to_string();
        self.cancellation_reason = reason;
    }

    /// Updates the status of the appointment.
    pub fn update_status(&mut self, new_status: String) {
        self.status = new_status;
    }

    /// Confirms the appointment.
    pub fn confirm_appointment(&mut self) {
        self.status = "Confirmed".to_string();
    }

    /// Marks patient as arrived.
    pub fn mark_arrived(&mut self) {
        self.status = "Arrived".to_string();
    }
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasAppointment {
    fn edge_label() -> &'static str {
        Self::RELATIONSHIP_TYPE
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("scheduled_time", true)
                .with_description("Scheduled date and time for the appointment (RFC3339)."),
            PropertyConstraint::new("actual_start_time", false)
                .with_description("Actual start time of the appointment (RFC3339)."),
            PropertyConstraint::new("actual_end_time", false)
                .with_description("Actual end time of the appointment (RFC3339)."),
            PropertyConstraint::new("duration_minutes", false)
                .with_description("Duration in minutes (i64, stored as String)."),
            PropertyConstraint::new("status", true)
                .with_description("Current status (Scheduled, Confirmed, Arrived, InProgress, Completed, Cancelled, NoShow)."),
            PropertyConstraint::new("appointment_type", true)
                .with_description("Type of appointment (Routine, FollowUp, Emergency, WalkIn)."),
            PropertyConstraint::new("reason", true)
                .with_description("Reason for the appointment."),
            PropertyConstraint::new("cancellation_reason", false)
                .with_description("Reason for cancellation or no-show."),
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the appointment link was created (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---

// --- ToEdge Implementation: Struct -> Generic Edge ---
impl ToEdge for HasAppointment {
    /// Converts the HasAppointment struct into a generic Edge structure for the graph database.
    fn to_edge(&self) -> Edge {
        // 1. Convert IDs to SerializableUuid
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::RELATIONSHIP_TYPE.to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id)
            // Mandatory properties wrapped in PropertyValue::String
            .with_property("id".to_string(), PropertyValue::String(self.id.clone()))
            .with_property("scheduled_time".to_string(), PropertyValue::String(self.scheduled_time.to_rfc3339()))
            .with_property("status".to_string(), PropertyValue::String(self.status.clone()))
            .with_property("appointment_type".to_string(), PropertyValue::String(self.appointment_type.clone()))
            .with_property("reason".to_string(), PropertyValue::String(self.reason.clone()))
            .with_property("created_at".to_string(), PropertyValue::String(self.created_at.to_rfc3339()));

        // Optional properties
        if let Some(val) = self.actual_start_time {
            e = e.with_property("actual_start_time".to_string(), PropertyValue::String(val.to_rfc3339()));
        }
        if let Some(val) = self.actual_end_time {
            e = e.with_property("actual_end_time".to_string(), PropertyValue::String(val.to_rfc3339()));
        }
        if let Some(val) = self.duration_minutes {
            // i64 converted to String
            e = e.with_property("duration_minutes".to_string(), PropertyValue::String(val.to_string()));
        }
        if let Some(ref val) = self.cancellation_reason {
            e = e.with_property("cancellation_reason".to_string(), PropertyValue::String(val.clone()));
        }

        // Additional/Dynamic Properties: Iterate and add all items from the map
        for (key, val) in &self.additional_properties {
            e = e.with_property(key.clone(), PropertyValue::String(val.clone()));
        }

        e
    }
}

// --- FromEdge Implementation: Generic Edge -> Struct ---
impl FromEdge for HasAppointment {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::RELATIONSHIP_TYPE { return None; }
        
        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };

        // Helper to parse string property into i64
        let parse_i64 = |prop_name: &str, edge: &Edge| -> Option<i64> {
            edge.properties.get(prop_name).and_then(|v| v.as_str()?.parse().ok())
        };

        // 1. Collect Additional Properties (must run before returning the struct)
        let mut additional_properties = HashMap::new();
        // Define all standard keys explicitly to exclude them from the dynamic map
        let known_keys: &[&str] = &[
            "id", 
            "scheduled_time", 
            "actual_start_time", 
            "actual_end_time", 
            "duration_minutes", 
            "status", 
            "appointment_type", 
            "reason", 
            "cancellation_reason", 
            "created_at"
        ];

        for (key, val) in &edge.properties {
            if !known_keys.contains(&key.as_str()) {
                // Ensure the value is a string before inserting it into the HashMap<String, String>
                if let PropertyValue::String(s) = val {
                    additional_properties.insert(key.to_string(), s.clone());
                }
            }
        }
        
        // 2. Map the extracted and parsed data to the HasAppointment struct
        Some(HasAppointment {
            // Mandatory fields use '?' for early exit on failure
            id: edge.properties.get("id")?.as_str()?.to_string(),
            scheduled_time: parse_datetime("scheduled_time", edge)?,
            status: edge.properties.get("status")?.as_str()?.to_string(),
            appointment_type: edge.properties.get("appointment_type")?.as_str()?.to_string(),
            reason: edge.properties.get("reason")?.as_str()?.to_string(),
            created_at: parse_datetime("created_at", edge)?,
            
            // Source/Target IDs: Convert SerializableUuid back to Uuid
            source_id: Uuid::parse_str(&edge.outbound_id.to_string()).ok()?, 
            target_id: Uuid::parse_str(&edge.inbound_id.to_string()).ok()?,

            // Optional fields do NOT use '?' when calling the helper, 
            // as the helper returns Option and we want to preserve 'None' if the property is missing.
            actual_start_time: parse_datetime("actual_start_time", edge),
            actual_end_time: parse_datetime("actual_end_time", edge),
            duration_minutes: parse_i64("duration_minutes", edge),
            cancellation_reason: edge.properties.get("cancellation_reason").and_then(|v| v.as_str()).map(|s| s.to_string()),

            additional_properties,
        })
    }
}
